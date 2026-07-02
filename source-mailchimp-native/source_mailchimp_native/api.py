from collections.abc import AsyncGenerator
from datetime import datetime
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import (
    ApiKey,
    Automation,
    Campaign,
    ChildSpec,
    IdOnly,
    Interest,
    InterestCategory,
    MailchimpChildEntity,
    MailchimpList,
    OAuthMetadata,
    ParentContext,
    ParentId,
    ParentIdValidationContext,
    SegmentMember,
)

# Mailchimp's base URL is data-center-dependent. The `{dc}` placeholder (e.g. "us6")
# must be resolved from the configured credentials at runtime via `resolve_base_url`
# before any request is made.
API = "https://{dc}.api.mailchimp.com/3.0"
OAUTH_METADATA_URL = "https://login.mailchimp.com/oauth2/metadata"


async def resolve_base_url(log: Logger, http: HTTPSession, credentials: object) -> str:
    """Resolve the account's data-center-specific base URL.

    API keys carry their data center as a suffix (`<key>-us6`); OAuth tokens
    require asking Mailchimp's metadata endpoint, which reports the account's
    `api_endpoint` (e.g. "https://us6.api.mailchimp.com").
    """
    if isinstance(credentials, ApiKey):
        _, _, dc = credentials.password.rpartition("-")
        if not dc:
            raise ValueError(
                (
                    "Mailchimp API key is missing its data center suffix "
                    "(expected a key ending in e.g. -us21)."
                )
            )
        return API.format(dc=dc)

    metadata = OAuthMetadata.model_validate_json(
        await http.request(log, OAUTH_METADATA_URL)
    )
    return f"{metadata.api_endpoint}/3.0"


# Documented maximum `count` across Mailchimp collection endpoints
# (live-verified accepted on /lists and /campaigns).
MAX_PAGE_SIZE = 1000


async def _fetch_collection_page[T: BaseModel](
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


async def _snapshot_collection[T: BaseModel](
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
        page = _fetch_collection_page(
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
    async for doc in _snapshot_collection(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, MailchimpList, log
    ):
        yield doc


async def snapshot_automations(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[Automation, None]:
    async for doc in _snapshot_collection(
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
    for list_id in await _fetch_parent_ids(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, log
    ):
        categories_path = InterestCategory.PATH_TEMPLATE.format(list_id=list_id)
        for category_id in await _fetch_parent_ids(
            http, base_url, categories_path, InterestCategory.ITEMS_KEY, log
        ):
            async for doc in _snapshot_collection(
                http,
                base_url,
                Interest.PATH_TEMPLATE.format(
                    list_id=list_id, category_id=category_id
                ),
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
    for list_id in await _fetch_parent_ids(
        http, base_url, MailchimpList.PATH, MailchimpList.ITEMS_KEY, log
    ):
        segments_path = f"lists/{list_id}/segments"
        for segment_id in await _fetch_parent_ids(
            http, base_url, segments_path, "segments", log
        ):
            async for doc in _snapshot_collection(
                http,
                base_url,
                SegmentMember.PATH_TEMPLATE.format(
                    list_id=list_id, segment_id=segment_id
                ),
                SegmentMember.ITEMS_KEY,
                SegmentMember,
                log,
                params=_SEGMENT_MEMBER_PARAMS,
                validation_context=ParentIdValidationContext({"segment_id": segment_id}),
            ):
                yield doc


async def _fetch_parent_ids(
    http: HTTPSession,
    base_url: str,
    path: str,
    items_key: str,
    log: Logger,
) -> list[ParentId]:
    """Drain a parent collection into bare IDs before any child request is
    made, projecting the response down to `fields=<items_key>.id`
    (live-verified honored on top-level and nested paths alike). `IdOnly`
    rejects an empty ID, so a malformed child path can never be templated."""
    return [
        item.id
        async for item in _snapshot_collection(
            http,
            base_url,
            path,
            items_key,
            IdOnly,
            log,
            params={"fields": f"{items_key}.id"},
        )
    ]


async def _resolve_leaf_contexts(
    spec: ChildSpec,
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> list[ParentContext]:
    """Drain the parent collection into one binding per parent ID — each a
    context that fills the `{placeholder}` in the child's `PATH_TEMPLATE`.

    `_fetch_parent_ids` fully materializes the parent list before any child is
    fetched, so no response is held open across the child requests."""

    parent = spec.parent
    parent_ids = await _fetch_parent_ids(
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

        async for doc in _snapshot_collection(
            http,
            base_url,
            spec.model.PATH_TEMPLATE.format(**ctx_to_snapshot),
            spec.model.ITEMS_KEY,
            spec.model,
            log,
            validation_context=validation_context,
        ):
            yield doc


def _campaign_params(offset: int, since: datetime) -> dict[str, str | int]:
    return {
        "count": MAX_PAGE_SIZE,
        "offset": offset,
        "since_create_time": since.isoformat(),
        "sort_field": "create_time",
        "sort_dir": "ASC",
    }


async def fetch_campaigns(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Campaign | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_seen = log_cursor
    offset = 0
    emitted = False

    while True:
        count = 0
        page = _fetch_collection_page(
            http,
            base_url,
            Campaign.PATH,
            Campaign.ITEMS_KEY,
            Campaign,
            _campaign_params(offset, log_cursor),
            log,
        )

        async for item in page:
            count += 1
            # Suppressing docs at-or-before the cursor makes `since_*`
            # boundary inclusivity irrelevant.
            if item.create_time <= log_cursor:
                continue

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
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    offset = page or 0
    # ASC sort under a frozen [start_date, cutoff) query keeps offsets stable
    # across restarts: new campaigns fall outside before_create_time.
    params = _campaign_params(offset, start_date)
    params["before_create_time"] = cutoff.isoformat()

    count = 0
    page_gen = _fetch_collection_page(
        http, base_url, Campaign.PATH, Campaign.ITEMS_KEY, Campaign, params, log
    )

    async for item in page_gen:
        yield item
        count += 1

    # A full page means there may be more; a short page is the last one.
    if count == MAX_PAGE_SIZE:
        yield offset + count
