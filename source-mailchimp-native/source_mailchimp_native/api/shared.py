from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from ..models import (
    ApiKey,
    Automation,
    Campaign,
    ChildSpec,
    IdOnly,
    Interest,
    InterestCategory,
    MailchimpChildEntity,
    MailchimpIncrementalChildEntity,
    MailchimpList,
    OAuth2Credentials,
    OAuthMetadata,
    ParentContext,
    ParentId,
    ParentIdValidationContext,
    Segment,
    SegmentMember,
)

# Mailchimp's base URL is data-center-dependent. The `{dc}` placeholder (e.g. "us6")
# must be resolved from the configured credentials at runtime via `resolve_base_url`
# before any request is made.
API = "https://{dc}.api.mailchimp.com/3.0"
OAUTH_METADATA_URL = "https://login.mailchimp.com/oauth2/metadata"


async def resolve_base_url(
    log: Logger, http: HTTPSession, credentials: OAuth2Credentials | ApiKey
) -> str:
    """Resolve the account's data-center-specific base URL.

    API keys carry their data center as a suffix (`<key>-us6`); OAuth tokens
    require asking Mailchimp's metadata endpoint, which reports the account's
    `api_endpoint` (e.g. "https://us6.api.mailchimp.com").
    """
    if isinstance(credentials, ApiKey):
        _, sep, dc = credentials.password.rpartition("-")
        if not sep or not dc:
            raise ValidationError(
                [
                    (
                        "Mailchimp API key is missing its data center suffix "
                        "(expected a key ending in e.g. -us21)."
                    )
                ]
            )
        return API.format(dc=dc)

    metadata = OAuthMetadata.model_validate_json(
        await http.request(log, OAUTH_METADATA_URL)
    )
    return f"{metadata.api_endpoint}/3.0"


# Documented maximum `count` across Mailchimp collection endpoints.
MAX_PAGE_SIZE = 1000


async def fetch_collection_page[T: BaseModel](
    http: HTTPSession,
    base_url: str,
    path: str,
    items_key: str,
    model: type[T],
    params: dict[str, str | int],
    log: Logger,
    validation_context: object | None = None,
) -> AsyncGenerator[T, None]:
    """Stream one page of the collection endpoint at `path`, yielding each
    item. Iterating to completion drains the whole response — the envelope
    tail past the items array is read and discarded — so callers detect the
    last page by its item count, not by anything in the envelope."""
    _, body = await http.request_stream(log, f"{base_url}/{path}", params=params)
    processor = IncrementalJsonProcessor(
        body(),
        f"{items_key}.item",
        model,
        validation_context=validation_context,
    )

    async for item in processor:
        yield item


async def snapshot_collection[T: BaseModel](
    http: HTTPSession,
    base_url: str,
    path: str,
    items_key: str,
    model: type[T],
    log: Logger,
    params: dict[str, str | int] | None = None,
    validation_context: object | None = None,
) -> AsyncGenerator[T, None]:
    """Page a collection to exhaustion (snapshot streams).

    Completion is the short-page signal: a page smaller than the requested
    `count` (including an empty one) is the last page. This holds regardless
    of whether the endpoint caps `count` below our request or reports a
    `total_items` that disagrees with the rows actually returned, so it's the
    one rule that works across every Mailchimp collection."""
    offset = 0

    while True:
        count = 0
        page = fetch_collection_page(
            http,
            base_url,
            path,
            items_key,
            model,
            {"count": MAX_PAGE_SIZE, "offset": offset, **(params or {})},
            log,
            validation_context,
        )

        async for item in page:
            yield item
            count += 1

        offset += count
        if count < MAX_PAGE_SIZE:
            return


async def snapshot_lists(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[MailchimpList, None]:
    async for doc in snapshot_collection(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, MailchimpList, log
    ):
        yield doc


async def snapshot_automations(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[Automation, None]:
    async for doc in snapshot_collection(
        http, base_url, Automation.PATH, Automation.ITEMS_KEY, Automation, log
    ):
        yield doc


async def snapshot_interests(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[Interest, None]:
    """Snapshot interests, walking lists -> interest-categories -> interests.
    Each interest carries its own list_id/category_id, so nothing is injected."""
    for list_id in await fetch_parent_ids(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, log
    ):
        categories_path = InterestCategory.PATH_TEMPLATE.format(list_id=list_id)
        for category_id in await fetch_parent_ids(
            http, base_url, categories_path, InterestCategory.ITEMS_KEY, log
        ):
            async for doc in snapshot_collection(
                http,
                base_url,
                Interest.PATH_TEMPLATE.format(list_id=list_id, category_id=category_id),
                Interest.ITEMS_KEY,
                Interest,
                log,
            ):
                yield doc


# Cleaned/transactional/unsubscribed members are excluded by default; capture
# full membership and let `status` drive downstream filtering.
_SEGMENT_MEMBER_PARAMS: dict[str, str | int] = {
    "include_cleaned": "true",
    "include_transactional": "true",
    "include_unsubscribed": "true",
}


async def snapshot_segment_members(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[SegmentMember, None]:
    """Snapshot segment members, walking lists -> segments -> members. The
    member response omits segment_id, so it is stamped into _meta."""
    for list_id in await fetch_parent_ids(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, log
    ):
        segments_path = Segment.PATH_TEMPLATE.format(list_id=list_id)
        for segment_id in await fetch_parent_ids(
            http, base_url, segments_path, Segment.ITEMS_KEY, log
        ):
            async for doc in snapshot_collection(
                http,
                base_url,
                SegmentMember.PATH_TEMPLATE.format(
                    list_id=list_id, segment_id=segment_id
                ),
                SegmentMember.ITEMS_KEY,
                SegmentMember,
                log,
                params=_SEGMENT_MEMBER_PARAMS,
                validation_context=ParentIdValidationContext(
                    {"segment_id": segment_id}
                ),
            ):
                yield doc


async def fetch_parent_ids(
    http: HTTPSession,
    base_url: str,
    path: str,
    items_key: str,
    log: Logger,
) -> list[ParentId]:
    """Drain a parent collection into bare IDs before any child request is
    made, projecting the response down to `fields=<items_key>.id` — the
    projection is honored on top-level and nested paths alike. `IdOnly`
    rejects an empty ID, so a malformed child path can never be templated.

    IDs are sorted so the child fan-out order is deterministic: snapshot
    change-suppression digests the emitted byte stream, so an unstable
    parent order would re-emit every child document each poll. Sorting is
    by string form only because `ParentId` spans str and int; the order
    itself carries no meaning."""
    return sorted(
        [
            item.id
            async for item in snapshot_collection(
                http,
                base_url,
                path,
                items_key,
                IdOnly,
                log,
                params={"fields": f"{items_key}.id"},
            )
        ],
        key=str,
    )


async def _resolve_leaf_contexts(
    spec: ChildSpec,
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> list[ParentContext]:
    """Drain the parent collection into one binding per parent ID — each a
    context that fills the `{placeholder}` in the child's `PATH_TEMPLATE`.

    `fetch_parent_ids` fully materializes the parent list before any child is
    fetched, so no response is held open across the child requests."""

    parent = spec.parent
    parent_ids = await fetch_parent_ids(
        http, base_url, parent.path_template, parent.items_key, log
    )
    return [{parent.id_field: parent_id} for parent_id in parent_ids]


async def snapshot_children(
    spec: ChildSpec,
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[MailchimpChildEntity, None]:
    """Snapshot one child stream: resolve the parent-ID fan-out into
    fully-bound contexts, then page the leaf collection under each."""
    for ctx_to_snapshot in await _resolve_leaf_contexts(spec, http, base_url, log):
        # The context is exactly `{parent.id_field: parent_id}`, so injecting the
        # parent ID means stamping that single binding into `_meta`.
        validation_context = (
            ParentIdValidationContext(ctx_to_snapshot)
            if spec.inject_parent_id
            else None
        )

        async for doc in snapshot_collection(
            http,
            base_url,
            spec.model.PATH_TEMPLATE.format(**ctx_to_snapshot),
            spec.model.ITEMS_KEY,
            spec.model,
            log,
            validation_context=validation_context,
        ):
            yield doc


def _campaign_params(
    offset: int, since: datetime, before: datetime
) -> dict[str, str | int]:
    return {
        "count": MAX_PAGE_SIZE,
        "offset": offset,
        "since_create_time": since.isoformat(),
        "before_create_time": before.isoformat(),
        "sort_field": "create_time",
        "sort_dir": "ASC",
    }


async def fetch_campaigns(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Campaign | LogCursor, None]:
    """Incrementally fetch campaigns created at `(cursor, horizon]`.

    `create_time` is the only time filter `/campaigns` exposes, so a campaign is
    seen once, at creation; later mutations (status transitions, report stats)
    are recovered by the daily scheduled backfill.

                      cursor  cursor + 1s  horizon = last elapsed second
    ─────────────────────┼─────────┼──────────┼────▶ time (1s ticks)
                         │         │          │
    since_create_time ───(═════════╪══════════╪═══▶
    before_create_time ══╪═════════╪══════════]
    emitted ─────────────┼─────────[══════════]
                         │         │          └─ the present second is still
                         │         │             in progress; its campaigns
                         │         │             wait for the next poll
                         │         └─ first emitted second
                         └─ excluded server-side, and create_time never
                            changes, so no campaign can appear at or
                            before this second later
    """
    assert isinstance(log_cursor, datetime)

    # Uses the last fully-elapsed second.
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return

    last_seen = log_cursor
    offset = 0
    emitted = False

    while True:
        count = 0
        params = _campaign_params(offset, log_cursor, horizon)
        page = fetch_collection_page(
            http,
            base_url,
            Campaign.PATH,
            Campaign.ITEMS_KEY,
            Campaign,
            params,
            log,
        )

        async for item in page:
            count += 1
            yield item
            emitted = True
            if item.create_time > last_seen:
                last_seen = item.create_time

        offset += count
        if count < MAX_PAGE_SIZE:
            break

    if emitted:
        yield last_seen


async def backfill_campaigns(
    http: HTTPSession,
    base_url: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Campaign | PageCursor, None]:
    """Backfill campaigns over the `(start_date, cutoff − 1s]` window.

                        start_date            cutoff − 1s cutoff
    ────────────────────────┼─────────────────────┼───────┼──▶ time (1s ticks)
                            │                     │       │
    since_create_time ──────(═════════════════════╪═══════╪══▶
    before_create_time ═════╪═════════════════════]       │
    window ─────────────────(═════════════════════]       │
                            │                     │       └─ covered by the
                            │                     │          first incremental
                            │                     │          poll
                            │                     └─ last backfilled second
                            └─ start-of-window precision is not load-bearing;
                               since_create_time is exclusive, so the
                               boundary instant just falls out

    Row order is not guaranteed to be stable, so in-progress checkpoints are
    not viable.
    """
    assert page is None
    assert isinstance(cutoff, datetime)

    offset = 0
    window_end = cutoff - timedelta(seconds=1)
    params = _campaign_params(offset, start_date, window_end)

    while True:
        params["offset"] = offset
        count = 0

        page_gen = fetch_collection_page(
            http, base_url, Campaign.PATH, Campaign.ITEMS_KEY, Campaign, params, log
        )

        async for item in page_gen:
            yield item
            count += 1

        # A full page means there may be more; a short page is the last one.
        if count < MAX_PAGE_SIZE:
            return

        offset += count


async def fetch_list_children[T: MailchimpIncrementalChildEntity](
    http: HTTPSession,
    base_url: str,
    model: type[T],
    list_id: ParentId,
    extra_request_params: dict[str, str | int],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | LogCursor, None]:
    """Incrementally fetch one (list, sweep) subtask of a list-child stream
    (list_members, segments); the cursor filter and value come from the model
    (`SINCE_PARAM`, `get_cursor()`).

                  cursor   cursor + 1s  horizon = last elapsed second
    ─────────────────┼──────────┼──────────────┼─────▶ time (1s ticks)
                     │          │              │
    SINCE_PARAM ─────(══════════╪══════════════╪═════▶
    BEFORE_PARAM ════╪══════════╪══════════════]
    emitted ─────────┼──────────[══════════════]
                     │          │              └─ the present second is still
                     │          │                 in progress; its docs wait
                     │          │                 for the next poll's window
                     │          └─ first emitted second
                     └─ nothing new can appear here or earlier: this second
                        had fully elapsed when it was walked, and updates
                        always stamp "now"
    """
    assert isinstance(log_cursor, datetime)

    # The last fully-elapsed second.
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return

    last_seen = log_cursor
    offset = 0
    emitted = False
    path = model.PATH_TEMPLATE.format(list_id=list_id)

    while True:
        count = 0
        page = fetch_collection_page(
            http,
            base_url,
            path,
            model.ITEMS_KEY,
            model,
            {
                "count": MAX_PAGE_SIZE,
                "offset": offset,
                model.SINCE_PARAM: log_cursor.isoformat(),
                model.BEFORE_PARAM: horizon.isoformat(),
                **extra_request_params,
            },
            log,
        )

        async for item in page:
            count += 1
            yield item
            emitted = True
            if item.get_cursor() > last_seen:
                last_seen = item.get_cursor()

        offset += count
        if count < MAX_PAGE_SIZE:
            break

    if emitted:
        yield last_seen


async def backfill_list_children[T: MailchimpIncrementalChildEntity](
    http: HTTPSession,
    base_url: str,
    model: type[T],
    list_id: ParentId,
    extra_request_params: dict[str, str | int],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[T | PageCursor, None]:
    """Backfill one (list, sweep) subtask of a list-child stream over the
    frozen `(start_date, cutoff − 1s]`.

              start_date            cutoff − 1s cutoff
    ──────────────┼─────────────────────┼───────┼──▶ time (1s ticks)
                  │                     │       │
    SINCE_PARAM ──(═════════════════════╪═══════╪══▶
    BEFORE_PARAM ═╪═════════════════════]       │
    window ───────(═════════════════════]       │
                  │                     │       └─ covered by the first
                  │                     │          incremental poll
                  │                     └─ last backfilled second
                  └─ docs stamped exactly at start_date are skipped
                     (since_* is exclusive)

    Row order is not guaranteed to be stable, so in-progress checkpoints are
    not viable.
    """
    assert page is None
    assert isinstance(cutoff, datetime)

    path = model.PATH_TEMPLATE.format(list_id=list_id)
    offset = 0

    while True:
        count = 0
        page_gen = fetch_collection_page(
            http,
            base_url,
            path,
            model.ITEMS_KEY,
            model,
            {
                "count": MAX_PAGE_SIZE,
                "offset": offset,
                model.SINCE_PARAM: start_date.isoformat(),
                model.BEFORE_PARAM: (cutoff - timedelta(seconds=1)).isoformat(),
                **extra_request_params,
            },
            log,
        )

        async for item in page_gen:
            yield item
            count += 1

        offset += count
        # A full page means there may be more; a short page is the last one.
        if count < MAX_PAGE_SIZE:
            return
