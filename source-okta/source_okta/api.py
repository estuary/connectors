import asyncio
import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from urllib.parse import parse_qs, urlparse

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import (
    DeleteEvent,
    Group,
    IdEntityDocument,
    OktaApplication,
    OktaFilterableEntity,
    SystemLogEvent,
    User,
)
from .utils import RFC3339_MS, to_rfc3339_ms

# Okta's System Log index trails ingestion by seconds (observed 7-35 s). Every
# bounded read of it is held this far behind "now": the system_log backfill's
# upper bound, and the Users and Groups windows, whose delete events come from
# the same index and whose windows are never revisited, so a late event would
# otherwise be lost for good.
INDEX_SETTLE = timedelta(minutes=10)
ONE_MS = timedelta(milliseconds=1)
ONE_SECOND = timedelta(seconds=1)

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


def after_token(url: str) -> str:
    """Return the `after` query value of a next link.

    The token is Okta's position in the log and is never interpreted; it is
    only carried from one poll to the next.
    """
    values = parse_qs(urlparse(url).query).get(SystemLogEvent.AFTER_PARAM)
    if not values:
        raise RuntimeError(f"Okta next link carries no `after` parameter: {url}")

    return values[0]


async def paginate[T: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    params: dict[str, str | int],
    model: type[T],
) -> AsyncGenerator[T, None]:
    """Stream every item of a Link-paginated list, following next links
    verbatim. The list ends when a page carries no next link, never when a
    page is short: a full last page without a next link has been observed."""
    next_url: str | None = url
    request_params: dict[str, str | int] | None = params
    while next_url:
        headers, body = await http.request_stream(log, next_url, params=request_params)
        async for item in IncrementalJsonProcessor(body(), "item", model):
            yield item
        next_url = next_link(headers)
        request_params = None


class SystemLogAccess:
    """Whether the API token can read `/logs`, probed once per connector run.

    The probe runs eagerly at discovery and validation as well as from the
    fetch path, so a user learns from the first interaction that deletions will
    not be captured. A token without System Log access is still valid: Users
    and Groups then capture updates only, and deletes that happen before access
    is granted are never recovered, since their windows have already passed.
    Every other failure propagates.
    """

    def __init__(self, domain: str):
        self._url: str = f"{base_url(domain)}/{SystemLogEvent.PATH}"
        self._allowed: bool | None = None
        self._lock: asyncio.Lock = asyncio.Lock()

    async def allowed(self, log: Logger, http: HTTPSession) -> bool:
        if self._allowed is not None:
            return self._allowed

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
                    "discovered disabled; enabling it fails with this same 403. "
                    "Create the token from an account whose admin role can view "
                    "the System Log to capture deletions and the System Log."
                ),
                {"error": err.message},
            )
            log.info("probed System Log access", {"allowed": False})
            return False

        log.info("probed System Log access", {"allowed": True})
        return True


async def _fetch_entities[T: OktaFilterableEntity](
    http: HTTPSession,
    domain: str,
    model: type[T],
    logs_access: SystemLogAccess,
    interval: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | IdEntityDocument | LogCursor, None]:
    """Emit every `model` object changed in `(cursor, horizon]`, then a delete
    document for each one hard-deleted in the same span, then the horizon.

    The window is bounded server-side by `model.window_filter`. Hard deletes
    only surface in the System Log, so the same span is read from `/logs` for
    `model.delete_filter`; that step is skipped when the token cannot read
    logs. Updates run before deletes, so a delete is never followed by a stale
    update for the same object, and Okta never reuses ids.

                cursor  cursor + 1s   horizon  horizon + 999ms
    ───────────────┼─────────┼───────────┼─────┼───▶ time (1s ticks)
                   │         │           │     │
    filter gt ─────(═════════╪═══════════╪═════╪═══▶
    filter le ═════╪═════════╪═══════════]     │
    logs since ────┼─────────[═══════════╪═════╪═══▶
    logs until ════╪═════════╪═══════════╪═════]
    emitted ───────┼─────────[═══════════]     │
                   │         │           │     └─ closes the horizon's second
                   │         │           │        on the 1 ms `/logs` clock
                   │         │           └─ now − INDEX_SETTLE, floored to the
                   │         │              second, so late-indexed delete
                   │         │              events have landed
                   │         └─ first emitted second
                   └─ excluded server-side; updates stamp "now", so nothing
                      new can appear at or before this second

    `filter` bounds are compared on whole-second stored values; `/logs` bounds
    are inclusive at 1 ms ticks, so `since`/`until` span the same seconds.
    Groups apply the same rails to each of `lastUpdated` and
    `lastMembershipUpdated`, OR-ed.

    The cursor is the horizon rather than the newest timestamp seen: a group
    can enter the window through its membership leg while its `lastUpdated`
    lies past the horizon, and a max-seen cursor would skip that span.

    A window is fetched at most once per binding `interval` and is never
    shorter than it: the cursor always trails "now" by INDEX_SETTLE, which
    can exceed the interval, and the CDK re-invokes without sleeping whenever
    it does. Returning without a checkpoint is what makes it sleep.
    """
    assert isinstance(log_cursor, datetime)

    horizon = datetime.now(tz=UTC).replace(microsecond=0) - INDEX_SETTLE
    if horizon - log_cursor < interval:
        return

    root = base_url(domain)
    params: dict[str, str | int] = {
        "limit": model.PAGE_SIZE,
        model.FILTER_PARAM: model.window_filter(log_cursor, horizon),
    }
    updated = 0
    async for doc in paginate(http, log, f"{root}/{model.PATH}", params, model):
        yield doc
        updated += 1

    deleted = 0
    if await logs_access.allowed(log, http):
        log_params: dict[str, str | int] = {
            SystemLogEvent.SINCE_PARAM: to_rfc3339_ms(log_cursor + ONE_SECOND),
            SystemLogEvent.UNTIL_PARAM: to_rfc3339_ms(horizon + ONE_SECOND - ONE_MS),
            SystemLogEvent.FILTER_PARAM: model.delete_filter(),
            "sortOrder": "ASCENDING",
            "limit": SystemLogEvent.PAGE_SIZE,
        }
        async for event in paginate(
            http, log, f"{root}/{SystemLogEvent.PATH}", log_params, DeleteEvent
        ):
            for target in event.target:
                if target.type == model.TARGET_TYPE:
                    yield IdEntityDocument.deleted(target.id)
                    deleted += 1

    yield horizon

    log.debug(
        "fetched entity window",
        {
            "stream": model.NAME,
            "window_start": to_rfc3339_ms(log_cursor),
            "window_end": to_rfc3339_ms(horizon),
            "updated": updated,
            "deleted": deleted,
        },
    )


async def _backfill_entities[T: OktaFilterableEntity](
    http: HTTPSession,
    domain: str,
    model: type[T],
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[T | PageCursor, None]:
    """Backfill `model` objects over `(start_date, cutoff − 1s]`, one fixed
    slice `(R, min(R + window_size, cutoff − 1s)]` per invocation, where `R` is
    the page cursor (`start_date` on the first invocation).

                   R = page     R + window_size   cutoff − 1s  cutoff
    ───────────────┼──────────────────┼────────────────┼────────┼──▶ time (1s)
                   │                  │                │        │
    filter gt ─────(══════════════════╪════════════════╪════════╪══▶
    filter le ═════╪══════════════════]                │        │
    slice ─────────(══════════════════]                │        │
                   │                  │                │        └─ first
                   │                  │                │           incremental
                   │                  │                │           window
                   │                  │                └─ caps the slice end;
                   │                  │                   a slice ending here
                   │                  │                   is the last one
                   │                  └─ yielded as the next page cursor
                   └─ shared with the previous slice's `le`, so slices are
                      gapless

    `filter` cannot sort, so there is no per-page watermark; the slice end is
    an immutable instant that means the same thing after any gap, and a restart
    redoes at most one slice. The last slice is not checkpointed: reaching
    `cutoff − 1s` ends the backfill. An object updated mid-backfill leaves its
    slice and is picked up incrementally.
    """
    assert isinstance(cutoff, datetime)
    last = cutoff - ONE_SECOND

    if page is None:
        start = start_date
    else:
        assert isinstance(page, str)
        start = datetime.fromisoformat(page)

    if start >= last:
        return

    window_end = min((start + window_size).replace(microsecond=0), last)
    params: dict[str, str | int] = {
        "limit": model.PAGE_SIZE,
        model.FILTER_PARAM: model.window_filter(start, window_end),
    }
    emitted = 0
    async for doc in paginate(
        http, log, f"{base_url(domain)}/{model.PATH}", params, model
    ):
        yield doc
        emitted += 1

    log.debug(
        "backfilled entity window",
        {
            "stream": model.NAME,
            "window_start": to_rfc3339_ms(start),
            "window_end": to_rfc3339_ms(window_end),
            "emitted": emitted,
        },
    )

    if window_end >= last:
        return

    yield to_rfc3339_ms(window_end)


async def fetch_users(
    http: HTTPSession,
    domain: str,
    logs_access: SystemLogAccess,
    interval: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[User | IdEntityDocument | LogCursor, None]:
    """Users changed in the window, then the users hard-deleted in it."""
    async for item in _fetch_entities(
        http, domain, User, logs_access, interval, log, log_cursor
    ):
        yield item


async def backfill_users(
    http: HTTPSession,
    domain: str,
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[User | PageCursor, None]:
    """Users whose `lastUpdated` falls in one backfill slice."""
    async for item in _backfill_entities(
        http, domain, User, start_date, window_size, log, page, cutoff
    ):
        yield item


async def fetch_groups(
    http: HTTPSession,
    domain: str,
    logs_access: SystemLogAccess,
    interval: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Group | IdEntityDocument | LogCursor, None]:
    """Groups whose profile or membership changed in the window, then the
    groups deleted in it."""
    async for item in _fetch_entities(
        http, domain, Group, logs_access, interval, log, log_cursor
    ):
        yield item


async def backfill_groups(
    http: HTTPSession,
    domain: str,
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Group | PageCursor, None]:
    """Groups whose `lastUpdated` or `lastMembershipUpdated` falls in one
    backfill slice."""
    async for item in _backfill_entities(
        http, domain, Group, start_date, window_size, log, page, cutoff
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
    params: dict[str, str | int] = {
        "limit": OktaApplication.PAGE_SIZE,
        "includeNonDeleted": "true",
    }
    async for app in paginate(http, log, url, params, OktaApplication):
        yield app


async def _read_log_page(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int] | None,
    log: Logger,
) -> tuple[list[SystemLogEvent], str | None]:
    headers, body = await http.request_stream(log, url, params=params)
    events = [
        event
        async for event in IncrementalJsonProcessor(body(), "item", SystemLogEvent)
    ]
    return events, next_link(headers)


async def fetch_system_log(
    http: HTTPSession,
    domain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SystemLogEvent | LogCursor, None]:
    """Tail the System Log in Okta's polling mode.

    The cursor is a one-element tuple holding either an RFC3339 instant (the
    seed, sent as `since`) or the `after` token from the last page's next link.
    Okta orders polling pages by persistence time and withholds an event until
    everything persisted before it is visible, so a checkpointed token never
    passes an unreleased event: the complete-ticks guarantee is the server's,
    and no client-side horizon is applied.

                 token t              Okta release horizon (~45-65 s back)
    ─────────────────┼──────────────────────────────┼──────▶ persistence (1 ms)
                     │                              │
    after ───────────(══════════════════════════════╪══════▶
    emitted ─────────┼─[════════════════════════════]
                     │                              └─ newer events are held
                     │                                 back by Okta; a later
                     │                                 poll returns them
                     └─ t = persistence ms of the last emitted event

    `after` excludes the event that issued it; the seed form sends `since`,
    which is inclusive on the same clock. Pages are 1000 events; a short page
    ends the poll, and an empty page means the tail is caught up.
    """
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    url: str | None = f"{base_url(domain)}/{SystemLogEvent.PATH}"
    params: dict[str, str | int] | None = {
        "sortOrder": "ASCENDING",
        "limit": SystemLogEvent.PAGE_SIZE,
    }
    if SystemLogEvent.AFTER_TOKEN.fullmatch(cursor):
        params[SystemLogEvent.AFTER_PARAM] = cursor
    elif RFC3339_MS.fullmatch(cursor):
        params[SystemLogEvent.SINCE_PARAM] = cursor
    else:
        raise RuntimeError(f"unrecognized System Log cursor: {cursor!r}")

    pages = 0
    emitted = 0
    while url:
        events, url = await _read_log_page(http, url, params, log)
        params = None
        if url is None:
            raise RuntimeError(
                'Okta polling response carried no rel="next" Link header'
            )
        if not events:
            break

        for event in events:
            yield event
        yield (after_token(url),)

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
        events, url = await _read_log_page(http, url, params, log)
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
