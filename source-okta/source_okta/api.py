import asyncio
import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import (
    AfterToken,
    ApplicationUser,
    DeleteEvent,
    Group,
    IdEntityDocument,
    OktaApplication,
    OktaSearchableEntity,
    ParentIdContext,
    ParentRef,
    SystemLogEvent,
    User,
)
from .utils import to_rfc3339_ms

# Eventual consistency grace period.
INDEX_SETTLE = timedelta(minutes=5)
# The shortest `interval` a binding may set. The CDK re-polls right after a
# checkpoint, and neither cursor is near the present then: users and groups
# trail it by INDEX_SETTLE, so their updates skip a window shorter than this;
# the system_log token trails it by Okta's ~45-65 s polling holdback, so only
# an interval past that holdback makes `fetch_system_log` skip the re-poll.
MIN_INTERVAL = timedelta(minutes=2)
ONE_MS = timedelta(milliseconds=1)
ONE_SECOND = timedelta(seconds=1)
# How far before the latest event's `published` a synthetic `after` token is
# placed. Used in deletion subtasks.
MINT_MARGIN = timedelta(seconds=10)

ERROR_RESOURCE_NOT_FOUND_CODE = "E0000007"

_NEXT_LINK = re.compile(r'<([^>]+)>;\s*rel="next"')


def base_url(domain: str) -> str:
    """The Management API root of an Okta org.

    Okta has no single API host: every org is served from its own domain
    (`dev-123456.okta.com`, `acme.okta-emea.com`, `acme.oktapreview.com`, or a
    custom domain). `domain` is the bare host the config validator guarantees.
    """
    return f"https://{domain}/api/v1"


def next_link(headers: Headers) -> str | None:
    """Return the URL of the `rel="next"` Link, or None when the page is the last.

    Okta sends one `Link` header per relation, so every header is inspected.
    """
    for raw in headers.getall("Link", []):
        match = _NEXT_LINK.search(raw)
        if match:
            return match.group(1)

    return None


async def paginate[T: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    params: dict[str, str | int],
    model: type[T],
    validation_context: object | None = None,
) -> AsyncGenerator[T, None]:
    """Stream every item of a Link-paginated list, following next links
    verbatim. The list ends when a page carries no next link, never when a page
    is short: a full last page without a next link has been observed."""

    next_url: str | None = url
    request_params: dict[str, str | int] | None = params

    while next_url:
        headers, body = await http.request_stream(log, next_url, params=request_params)
        async for item in IncrementalJsonProcessor(
            body(), "item", model, validation_context=validation_context
        ):
            yield item
        next_url = next_link(headers)
        request_params = None


class SystemLogAccess:
    """Whether the API token can read `/logs`, probed once per connector run.

    The probe runs at discovery and validation as well as from the fetch path.
    A token without System Log access captures updates only, while their
    deletes subtasks hold their position. Once a later connector run can read
    the log, the deletes still within Okta's retention are recovered.
    """

    def __init__(self, domain: str):
        self._url: str = f"{base_url(domain)}/{SystemLogEvent.PATH}"
        self._allowed: bool | None = None
        self._lock: asyncio.Lock = asyncio.Lock()

    async def allowed(self, log: Logger, http: HTTPSession) -> bool:
        async with self._lock:
            if self._allowed is None:
                self._allowed = await self._probe(log, http)

            return self._allowed

    async def _probe(self, log: Logger, http: HTTPSession) -> bool:
        try:
            _ = await http.request(log, self._url, params={"limit": 1})
        except HTTPError as err:
            if err.code != 403:
                raise
            log.warning(
                (
                    "The API token cannot read the System Log, so user and group "
                    "deletions will not be captured and the system_log binding is "
                    "not discovered. "
                    "Create the token from an account whose admin role can view "
                    "the System Log to capture deletions and the System Log."
                ),
                {"error": err.message},
            )
            log.info("probed System Log access", {"allowed": False})
            return False

        log.info("probed System Log access", {"allowed": True})
        return True


class CommittedHorizon:
    """The newest updates horizon of one users or groups binding whose window
    is already in the journal, shared by the binding's two subtasks.

    It is held in memory only. A binding seeds it from its persisted updates
    cursor on open, which is a committed horizon by definition.
    """

    def __init__(self, at: datetime):
        self.at: datetime = at


async def _fetch_entity_updates[T: OktaSearchableEntity](
    http: HTTPSession,
    domain: str,
    model: type[T],
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | LogCursor, None]:
    """Emit every `model` object changed in `(cursor, horizon]`, then the
    horizon, and publish the horizon to `committed_horizon` once it is
    committed.

    The window is bounded server-side by `model.window_search`.

                cursor  cursor + 1s   horizon
    ───────────────┼─────────┼───────────┼─────▶ time (1s ticks)
                   │         │           │
    search gt ─────(═════════╪═══════════╪═════▶
    search le ═════╪═════════╪═══════════]
    emitted ───────┼─────────[═══════════]
                   │         │           └─ now − INDEX_SETTLE, floored to
                   │         │              the second, so late-indexed rows
                   │         │              have landed
                   │         └─ first emitted second
                   └─ excluded server-side; updates stamp "now", so nothing
                      new can appear at or before this second

    `search` bounds are compared on whole-second stored values. Groups apply
    the same rails to each of `lastUpdated` and `lastMembershipUpdated`,
    OR-ed.

    The cursor is the horizon rather than the newest timestamp seen: a group
    can enter the window through its membership leg while its `lastUpdated`
    lies past the horizon, and a max-seen cursor would skip that span.
    """
    assert isinstance(log_cursor, datetime)

    horizon = datetime.now(tz=UTC).replace(microsecond=0) - INDEX_SETTLE
    if horizon - log_cursor < MIN_INTERVAL:
        return

    params: dict[str, str | int] = {
        "limit": model.PAGE_SIZE,
        **model.SORT_PARAMS,
        model.SEARCH_PARAM: model.window_search(log_cursor, horizon),
    }
    updated = 0
    url = f"{base_url(domain)}/{model.PATH}"
    async for doc in paginate(http, log, url, params, model):
        yield doc
        updated += 1

    yield horizon
    committed_horizon.at = horizon

    log.debug(
        "fetched entity window",
        {
            "stream": model.NAME,
            "window_start": to_rfc3339_ms(log_cursor),
            "window_end": to_rfc3339_ms(horizon),
            "updated": updated,
        },
    )


async def _fetch_entity_deletes[T: OktaSearchableEntity](
    http: HTTPSession,
    domain: str,
    model: type[T],
    logs_access: SystemLogAccess,
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IdEntityDocument | LogCursor, None]:
    """Emit a delete document for every `model` object the System Log records
    as hard-deleted no later than the committed updates horizon, then the
    position to resume from. Does nothing when the token cannot read logs.

    The cursor is a one-element tuple holding an `after` token. The tail runs
    in Okta's polling mode, ordered by persistence, and stops at the first
    event E published after `committed_horizon.at`.

             t   published(E) − MINT_MARGIN  E            release
    ─────────┼────────────────┼──────────────┼───────────────┼──▶ persistence
             │                │              │               │     (1 ms)
    after ───(════════════════╪══════════════╪═══════════════]
    emitted ─(════════════════╪══════════════)               │
    next ────┼────────────────[══════════════╪═══════════════╪══▶
             │                │              │               └─ Okta holds
             │                │              │                  newer events
             │                │              │                  back
             │                │              └─ first event published after
             │                │                 the committed horizon
             │                └─ minted, so never after E; the span up to E
             │                   is read again next poll
             └─ the event that issued t is excluded

    A delete document therefore never commits ahead of an update read before
    the delete.

    A page with no such E advances on Okta's next-link token. Otherwise the
    position is minted MINT_MARGIN before `published(E)`, since no event body
    carries its persistence time.

    The `filter` only narrows the read. Its effect in polling mode is
    unverified, so `model.deleted_ids` checks each event's type and outcome
    again.
    """
    token = AfterToken.from_cursor(log_cursor)

    if not await logs_access.allowed(log, http):
        return

    horizon = committed_horizon.at
    url: str | None = f"{base_url(domain)}/{SystemLogEvent.PATH}"
    params: dict[str, str | int] | None = {
        SystemLogEvent.AFTER_PARAM: str(token),
        SystemLogEvent.FILTER_PARAM: model.delete_filter(),
        "sortOrder": "ASCENDING",
        "limit": SystemLogEvent.PAGE_SIZE,
    }

    checkpointed_token = token
    pages_count = 0
    deleted_count = 0
    has_reached_horizon = False

    while True:
        events, url = await _read_log_page(http, url, params, log, DeleteEvent)
        params = None
        if url is None:
            raise RuntimeError(
                'Okta polling response carried no rel="next" Link header'
            )
        if not events:
            break

        stop_idx = next(
            (i for i, event in enumerate(events) if event.published > horizon), None
        )
        if stop_idx is None:
            events_behind_horizon, resume_token = events, AfterToken.from_url(url)
        else:
            has_reached_horizon = True
            events_behind_horizon = events[:stop_idx]
            minted_token = AfterToken.from_timestamp(
                events[stop_idx].published - MINT_MARGIN
            )
            resume_token = max(checkpointed_token, minted_token)

        if resume_token == checkpointed_token:
            break

        for event in events_behind_horizon:
            for target_id in model.deleted_ids(event):
                yield IdEntityDocument.deleted(target_id)
                deleted_count += 1
        yield resume_token.to_cursor()

        checkpointed_token = resume_token
        pages_count += 1
        if has_reached_horizon or len(events) < SystemLogEvent.PAGE_SIZE:
            break

    log.debug(
        "polled entity deletes",
        {
            "stream": model.NAME,
            "committed_horizon": to_rfc3339_ms(horizon),
            "pages": pages_count,
            "deleted": deleted_count,
            "has_reached_horizon": has_reached_horizon,
        },
    )


async def _backfill_entities[T: OktaSearchableEntity](
    http: HTTPSession,
    domain: str,
    model: type[T],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[T | PageCursor, None]:
    """Backfill `model` objects changed in `(start_date, cutoff − 1s]`, one
    id-ordered page per invocation, checkpointing the page's last id.

             start_date  start_date + 1s      cutoff − 1s  cutoff
    ─────────────┼──────────────┼───────────────────┼────────┼──▶ time (1s)
                 │              │                   │        │
    search gt ───(══════════════╪═══════════════════╪════════╪══▶
    search le ═══╪══════════════╪═══════════════════]        │
    emitted ─────┼──────────────[═══════════════════]        │
                 │              │                   │        └─ first
                 │              │                   │           incremental
                 │              │                   │           second
                 │              │                   └─ last backfilled second
                 │              └─ first backfilled second
                 └─ as configured; not load-bearing

    The window is fixed for the whole backfill; within it, objects are walked
    in id order, and `page` is the last id emitted (None on the first
    invocation). Ids are immutable, so a resume after any gap picks up exactly
    after it. An object updated mid-backfill leaves the window and is captured
    incrementally; one created mid-backfill is never in it.
    """
    assert isinstance(cutoff, datetime)
    last = cutoff - ONE_SECOND
    if start_date >= last:
        return

    if page is not None:
        assert isinstance(page, str)

    params: dict[str, str | int] = {
        "limit": model.PAGE_SIZE,
        **model.SORT_PARAMS,
        model.SEARCH_PARAM: model.page_search(start_date, last, page),
    }
    headers, body = await http.request_stream(
        log, f"{base_url(domain)}/{model.PATH}", params=params
    )

    ids: list[str] = []
    async for doc in IncrementalJsonProcessor(body(), "item", model):
        yield doc
        ids.append(doc.id)

    log.debug(
        "backfilled entity page",
        {"stream": model.NAME, "after_id": page, "emitted": len(ids)},
    )

    resumed_ids = ids if page is None else [page, *ids]
    misordered = next(
        ((a, b) for a, b in pairwise(resumed_ids) if a.lower() >= b.lower()), None
    )
    if misordered is not None:
        earlier, later = misordered
        raise RuntimeError(
            (
                f"Okta returned {model.NAME} id {later!r} after {earlier!r}, out of "
                "case-insensitive order, so resuming on `id gt` could skip rows"
            )
        )

    # The list ends where a page carries no next link, never on a short page.
    if next_link(headers) is None:
        return
    if not ids:
        raise RuntimeError(
            f"Okta returned an empty {model.NAME} page with a next link after id {page}"
        )

    yield ids[-1]


async def fetch_user_updates(
    http: HTTPSession,
    domain: str,
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[User | LogCursor, None]:
    """Users changed in the window."""
    async for item in _fetch_entity_updates(
        http, domain, User, committed_horizon, log, log_cursor
    ):
        yield item


async def fetch_user_deletes(
    http: HTTPSession,
    domain: str,
    logs_access: SystemLogAccess,
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IdEntityDocument | LogCursor, None]:
    """Users hard-deleted up to the committed updates horizon."""
    async for item in _fetch_entity_deletes(
        http, domain, User, logs_access, committed_horizon, log, log_cursor
    ):
        yield item


async def backfill_users(
    http: HTTPSession,
    domain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[User | PageCursor, None]:
    """One id-ordered page of the users whose `lastUpdated` falls in the
    backfill window."""
    async for item in _backfill_entities(
        http, domain, User, start_date, log, page, cutoff
    ):
        yield item


async def fetch_group_updates(
    http: HTTPSession,
    domain: str,
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Group | LogCursor, None]:
    """Groups whose profile or membership changed in the window."""
    async for item in _fetch_entity_updates(
        http, domain, Group, committed_horizon, log, log_cursor
    ):
        yield item


async def fetch_group_deletes(
    http: HTTPSession,
    domain: str,
    logs_access: SystemLogAccess,
    committed_horizon: CommittedHorizon,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IdEntityDocument | LogCursor, None]:
    """Groups deleted up to the committed updates horizon."""
    async for item in _fetch_entity_deletes(
        http, domain, Group, logs_access, committed_horizon, log, log_cursor
    ):
        yield item


async def backfill_groups(
    http: HTTPSession,
    domain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Group | PageCursor, None]:
    """One id-ordered page of the groups whose `lastUpdated` or
    `lastMembershipUpdated` falls in the backfill window."""
    async for item in _backfill_entities(
        http, domain, Group, start_date, log, page, cutoff
    ):
        yield item


async def snapshot_applications(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[OktaApplication, None]:
    """List every app integration the org has, inactive ones included.

    `includeNonDeleted` is sent because the spec documents the default as
    excluding non-active apps, even though the observed default lists them.
    """
    url = f"{base_url(domain)}/{OktaApplication.PATH}"
    async for app in paginate(
        http, log, url, OktaApplication.LIST_PARAMS, OktaApplication
    ):
        yield app


async def snapshot_application_users(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[ApplicationUser, None]:
    """Every user assignment of every app, ACTIVE or INACTIVE, each stamped
    with its app and granting group.

    Apps are listed in full before any assignment request. An app deleted
    between the two steps answers 404 ERROR_RESOURCE_NOT_FOUND_CODE and is
    skipped."""

    root = base_url(domain)
    app_ids = [
        app.id
        async for app in paginate(
            http,
            log,
            f"{root}/{OktaApplication.PATH}",
            OktaApplication.LIST_PARAMS,
            ParentRef,
        )
    ]

    emitted = 0
    for app_id in app_ids:
        url = f"{root}/{ApplicationUser.PATH_TEMPLATE.format(app_id=app_id)}"
        params: dict[str, str | int] = {"limit": ApplicationUser.PAGE_SIZE}
        try:
            async for doc in paginate(
                http, log, url, params, ApplicationUser, ParentIdContext(app_id)
            ):
                yield doc
                emitted += 1
        except HTTPError as err:
            if err.code != 404 or ERROR_RESOURCE_NOT_FOUND_CODE not in err.message:
                raise
            log.info("app vanished during snapshot", {"app_id": app_id})

    log.debug(
        "snapshotted application users", {"apps": len(app_ids), "emitted": emitted}
    )


async def _read_log_page[T: BaseModel](
    http: HTTPSession,
    url: str,
    params: dict[str, str | int] | None,
    log: Logger,
    model: type[T],
) -> tuple[list[T], str | None]:
    headers, body = await http.request_stream(log, url, params=params)
    events = [event async for event in IncrementalJsonProcessor(body(), "item", model)]

    return events, next_link(headers)


async def fetch_system_log(
    http: HTTPSession,
    domain: str,
    interval: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SystemLogEvent | LogCursor, None]:
    """Tail the System Log in Okta's polling mode.

    The cursor is a one-element tuple holding an `after` token. Okta orders
    polling pages by persistence time and withholds an event until everything
    persisted before it is visible, so a checkpointed token never passes an
    unreleased event: the complete-ticks guarantee is the server's, and no
    client-side horizon is applied.

                 token t              Okta release horizon (~45-65 s back)
    ─────────────────┼──────────────────────────────┼──────▶ persistence (1 ms)
                     │                              │
    after ───────────(══════════════════════════════╪══════▶
    emitted ─────────┼─[════════════════════════════]
                     │                              └─ newer events are held
                     │                                 back by Okta; a later
                     │                                 poll returns them
                     └─ t = persistence ms of the last emitted event

    `after` excludes the event that issued it; the seed is inclusive at the
    cutoff ms, like `since`. Pages are 1000 events; a short page ends the
    poll, and an empty page means the tail is caught up.
    """
    token = AfterToken.from_cursor(log_cursor)
    if datetime.now(tz=UTC) - token.timestamp < interval:
        return

    url: str | None = f"{base_url(domain)}/{SystemLogEvent.PATH}"
    params: dict[str, str | int] | None = {
        SystemLogEvent.AFTER_PARAM: str(token),
        "sortOrder": "ASCENDING",
        "limit": SystemLogEvent.PAGE_SIZE,
    }

    pages = 0
    emitted = 0
    while True:
        events, url = await _read_log_page(http, url, params, log, SystemLogEvent)
        params = None
        if url is None:
            raise RuntimeError(
                'Okta polling response carried no rel="next" Link header'
            )
        if not events:
            break

        for event in events:
            yield event
        yield AfterToken.from_url(url).to_cursor()

        pages += 1
        emitted += len(events)
        if len(events) < SystemLogEvent.PAGE_SIZE:
            break

    log.debug(
        "polled system log",
        {"pages": pages, "events": emitted, "caught_up": pages == 0},
    )


async def backfill_system_log(
    http: HTTPSession,
    domain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[SystemLogEvent | PageCursor, None]:
    """Walk historical events in Okta's bounded mode, oldest first.

    The page cursor is the `published` instant of the last emitted event, a
    value watermark over an immutable key; resuming re-reads the events that
    share that millisecond and the `uuid` key collapses them. Okta's next links
    are followed only inside one invocation.

              start_date                      cutoff − 1 ms  cutoff
    ─────────────┼────────────────────────────────────┼─────────┼───▶ published
                 │                                    │         │     (1 ms)
    since ═══════[════════════════════════════════════╪═════════╪═══▶
    until ═══════╪════════════════════════════════════]         │
    emitted ─────[════════════════════════════════════]         │
                 │                                    │         └─ incremental
                 │                                    │            takes over
                 └─ boundary instant included         └─ inclusive

    Both bounds are inclusive. Events older than Okta's 90-day retention are
    simply absent, so `start_date` needs no clamp.
    """
    assert isinstance(cutoff, datetime)
    if page is None:
        since = to_rfc3339_ms(start_date)
    else:
        assert isinstance(page, str)
        since = page

    url: str | None = f"{base_url(domain)}/{SystemLogEvent.PATH}"
    params: dict[str, str | int] | None = {
        SystemLogEvent.SINCE_PARAM: since,
        SystemLogEvent.UNTIL_PARAM: to_rfc3339_ms(cutoff - ONE_MS),
        "sortOrder": "ASCENDING",
        "limit": SystemLogEvent.PAGE_SIZE,
    }

    pages = 0
    emitted = 0
    while url:
        events, url = await _read_log_page(http, url, params, log, SystemLogEvent)
        params = None

        for event in events:
            yield event
        pages += 1
        emitted += len(events)

        # The last page carries no next link; returning without a cursor is
        # what tells the CDK the backfill is complete.
        if url is not None and events:
            yield to_rfc3339_ms(events[-1].published)

    log.debug(
        "backfilled system log window",
        {"since": since, "pages": pages, "events": emitted},
    )
