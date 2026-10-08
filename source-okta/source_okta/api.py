import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.http import Headers, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import AfterToken, OktaApplication, SystemLogEvent
from .utils import to_rfc3339_ms

# Okta's System Log index trails ingestion by seconds (observed 7-35 s); the
# backfill's upper bound is held this far behind "now" so its last window is
# complete by the time it is read. Mirrors source-sentry's _EXPLORE_INGESTION_LAG.
BACKFILL_SETTLE = timedelta(minutes=5)
ONE_MS = timedelta(milliseconds=1)

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


async def paginate[T: BaseDocument](
    http: HTTPSession,
    log: Logger,
    url: str,
    params: dict[str, str | int],
    model: type[T],
) -> AsyncGenerator[T, None]:
    """Stream every item of a Link-paginated list, following next links
    verbatim."""
    next_url: str | None = url
    request_params: dict[str, str | int] | None = params
    while next_url:
        headers, body = await http.request_stream(log, next_url, params=request_params)
        async for item in IncrementalJsonProcessor(body(), "item", model):
            yield item
        next_url = next_link(headers)
        request_params = None


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

              start_date                       cutoff − 1 ms  cutoff
    ─────────────┼─────────────────────────────────────┼─────────┼───▶ published
                 │                                     │         │     (1 ms)
    since ═══════[═════════════════════════════════════╪═════════╪═══▶
    until ═══════╪═════════════════════════════════════]         │
    emitted ─────[═════════════════════════════════════]         │
                 │                                     │         └─ incremental
                 │                                     │            takes over
                 └─ boundary instant included          └─ inclusive

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
