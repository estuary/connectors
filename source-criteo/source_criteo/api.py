from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

# `alru_cache` keeps the portfolio lookup off the wire for repeat callers and
# collapses concurrent misses onto a single shared future, so simultaneous
# binding tasks don't stampede the endpoint.
from async_lru import alru_cache
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .shared import call_with_cache_logging

from .models import (
    ApiProblem,
    CriteoEntity,
    EntityResponse,
    ReportConfig,
    ReportEnvelope,
    ReportRow,
)

API = "https://api.criteo.com"

# The current stable Criteo release, live since 2026-07-21. Criteo sunsets a
# version roughly 12 months after it stabilizes, so this needs revisiting around
# mid-2027. Pinning keeps request and response shapes fixed; bumping it is a
# deliberate change, not something that should drift underneath a running capture.
#
# Each version's OpenAPI spec is public and unauthenticated, at
# https://api.criteo.com/<version>/marketingsolutions/open-api-specifications.json
# — diff it against this version before bumping. Note that Criteo serves the
# next, unreleased version's spec too; pin only to a version with release notes.
API_VERSION = "2026-07"

# The audience search endpoint's documented maximum page size. The ad set and
# campaign search endpoints take no pagination parameters at all.
AUDIENCES_PAGE_SIZE = 100

# One day, the resolution of a statistics report's date dimensions.
TICK = timedelta(days=1)

# Criteo's documented cap on the rows a single statistics report may return.
# A response that reaches it may have been truncated, and the connector cannot
# tell truncation from a window that genuinely held exactly this many rows, so
# it treats the cap as truncation and narrows the window.
REPORT_ROW_LIMIT = 100_000

# How far back the statistics report serves data.
REPORT_RETENTION = timedelta(days=2 * 365)


def _raise_for_problems(problems: list[ApiProblem], context: str) -> None:
    if not problems:
        return

    details = "; ".join(
        f"{problem.type or 'error'}/{problem.code or 'unknown'}: {problem.title or ''} {problem.detail or ''}".strip()
        for problem in problems
    )
    raise RuntimeError(f"Criteo API returned errors for {context}: {details}")


def _log_problems(log: Logger, problems: list[ApiProblem], context: str) -> None:
    for problem in problems:
        log.warning(
            "criteo api warning",
            {
                "context": context,
                "code": problem.code,
                "type": problem.type,
                "title": problem.title,
                "detail": problem.detail,
            },
        )


def _validate_entities(body: bytes, context: str, log: Logger) -> EntityResponse:
    response = EntityResponse.model_validate_json(body)
    _raise_for_problems(response.errors, context)
    _log_problems(log, response.warnings, context)
    return response


async def _fetch_portfolio(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    """Yield every advertiser the API client can see."""
    url = f"{API}/{API_VERSION}/advertisers/me"
    response = _validate_entities(await http.request(log, url), "advertisers/me", log)

    for entity in response.data:
        yield entity


async def snapshot_advertisers(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    async for advertiser in _fetch_portfolio(http, log):
        yield advertiser


@alru_cache(maxsize=16, ttl=15 * 60)
async def _fetch_advertiser_ids(http: HTTPSession, log: Logger) -> tuple[str, ...]:
    # Returns a tuple. The result is retained in the cache, so it must not be
    # something a caller can mutate.
    return tuple([advertiser.id async for advertiser in _fetch_portfolio(http, log)])


async def resolve_advertiser_ids(
    http: HTTPSession,
    configured_ids: list[str],
    log: Logger,
) -> list[str]:
    """Return the advertiser IDs a request should be scoped to."""
    if configured_ids:
        return configured_ids

    return list(await call_with_cache_logging(_fetch_advertiser_ids, log, http, log))


async def snapshot_audiences(
    http: HTTPSession,
    configured_advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    """Yield every audience belonging to the in-scope advertisers."""
    advertiser_ids = await resolve_advertiser_ids(http, configured_advertiser_ids, log)
    url = f"{API}/{API_VERSION}/marketing-solutions/audiences/search"
    body = {
        "data": {"type": "AudienceSearch", "attributes": {"advertiserIds": advertiser_ids}}
    }
    offset = 0

    while True:
        response = _validate_entities(
            await http.request(
                log,
                url,
                method="POST",
                params={"limit": AUDIENCES_PAGE_SIZE, "offset": offset},
                json=body,
            ),
            "audiences/search",
            log,
        )

        for entity in response.data:
            yield entity

        if response.meta is None:
            raise RuntimeError(
                "Criteo audiences/search response did not include the pagination `meta` object."
            )

        # Advance by what was actually served rather than the requested `limit`,
        # so the offset stays correct if the endpoint ever clamps the page size.
        offset += len(response.data)
        if len(response.data) == 0 or offset >= response.meta.totalItems:
            return


async def _snapshot_search_entities(
    http: HTTPSession,
    path: str,
    configured_advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    """Drain a search endpoint that returns its full result set in one response."""
    advertiser_ids = await resolve_advertiser_ids(http, configured_advertiser_ids, log)
    url = f"{API}/{API_VERSION}/marketing-solutions/{path}/search"
    response = _validate_entities(
        await http.request(
            log,
            url,
            method="POST",
            json={"filters": {"advertiserIds": advertiser_ids}},
        ),
        f"{path}/search",
        log,
    )

    # These endpoints expose no pagination parameters, so a `meta` reporting more
    # items than were served would mean the result set is being capped somewhere
    # this code has no way to page through.
    if response.meta is not None and response.meta.totalItems > len(response.data):
        raise RuntimeError(
            f"Criteo {path}/search returned {len(response.data)} of "
            f"{response.meta.totalItems} items, but the endpoint takes no pagination "
            "parameters. The connector cannot reach the remaining items."
        )

    for entity in response.data:
        yield entity


async def snapshot_ad_sets(
    http: HTTPSession,
    configured_advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    """Yield every ad set belonging to the in-scope advertisers."""
    async for entity in _snapshot_search_entities(
        http, "ad-sets", configured_advertiser_ids, log
    ):
        yield entity


async def snapshot_campaigns(
    http: HTTPSession,
    configured_advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[CriteoEntity, None]:
    """Yield every campaign belonging to the in-scope advertisers."""
    async for entity in _snapshot_search_entities(
        http, "campaigns", configured_advertiser_ids, log
    ):
        yield entity


def floor_to_day(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def clamp_to_retention(day: datetime, report: ReportConfig, log: Logger) -> datetime:
    """Move `day` up to the earliest day Criteo still serves, if it's older."""
    earliest = floor_to_day(datetime.now(tz=UTC)) - REPORT_RETENTION
    if day >= earliest:
        return day

    log.warning(
        "report window starts before the two years of history Criteo serves, starting from the earliest day it does",
        {
            "report": report.name,
            "requested_first_day": day.date().isoformat(),
            "first_day": earliest.date().isoformat(),
        },
    )
    return earliest


async def _request_report_window(
    http: HTTPSession,
    report: ReportConfig,
    document_model: type[ReportRow],
    advertiser_ids: list[str],
    first_day: datetime,
    last_day: datetime,
    log: Logger,
) -> AsyncGenerator[ReportRow, None]:
    """Yield the rows Criteo returns for one inclusive [first_day, last_day] slice."""
    if not advertiser_ids:
        # Unlike the search endpoints, the statistics report requires an explicit
        # advertiser list, so an empty portfolio can't be reported on at all.
        raise RuntimeError(
            f"Cannot capture report {report.name}: no advertisers are configured and the "
            "API client's portfolio is empty. Grant the client access to an advertiser, "
            "or set advertiser_ids explicitly."
        )

    url = f"{API}/{API_VERSION}/statistics/report"
    body: dict[str, Any] = {
        "advertiserIds": ",".join(advertiser_ids),
        "startDate": first_day.isoformat(),
        "endDate": last_day.isoformat(),
        "format": "json",
        "dimensions": report.all_dimensions,
        "metrics": report.metrics,
        "currency": report.currency,
        "timezone": report.timezone,
    }

    _, stream = await http.request_stream(log, url, method="POST", json=body)
    processor = IncrementalJsonProcessor(
        stream(),
        "Rows.item",
        document_model,
        ReportEnvelope,
    )

    rows = 0
    async for row in processor:
        rows += 1
        yield row

    envelope = processor.get_remainder()
    _raise_for_problems(envelope.errors, f"report {report.name}")
    _log_problems(log, envelope.warnings, f"report {report.name}")

    if envelope.rows is None:
        raise RuntimeError(
            f"Criteo statistics report for {report.name} did not return a `Rows` array; "
            "the response envelope is not the documented shape."
        )

    log.debug(
        "fetched statistics report window",
        {
            "report": report.name,
            "start_date": body["startDate"],
            "end_date": body["endDate"],
            "rows": rows,
        },
    )


async def _fetch_report_window(
    http: HTTPSession,
    report: ReportConfig,
    document_model: type[ReportRow],
    advertiser_ids: list[str],
    first_day: datetime,
    last_day: datetime,
    log: Logger,
) -> AsyncGenerator[ReportRow, None]:
    """Yield every row of the inclusive [first_day, last_day] slice.

    A response at Criteo's row cap may have been truncated, and there is no way
    to page the rest of it, so the slice is halved and re-requested until each
    half comes back under the cap. Accepting a capped response instead would
    checkpoint past the dropped rows, which no later sweep revisits.

    Rows already streamed from the capped attempt have been yielded by the time
    the cap is detected. They are re-yielded by the halves and collapse on the
    collection key, so the duplication costs bandwidth, not correctness.
    """
    rows = 0
    async for row in _request_report_window(
        http, report, document_model, advertiser_ids, first_day, last_day, log
    ):
        rows += 1
        yield row

    if rows < REPORT_ROW_LIMIT:
        return

    if first_day >= last_day:
        # A single day is the narrowest slice Criteo's date parameters express,
        # so there is nothing left to split.
        raise RuntimeError(
            f"Criteo statistics report {report.name} returned {rows} rows for the single "
            f"day {first_day.date().isoformat()}, at or above Criteo's {REPORT_ROW_LIMIT}-row "
            "response cap, so the response may be truncated and the connector cannot narrow "
            "the request any further. Request fewer dimensions, or split the report into "
            "several reports scoped to different advertiser_ids."
        )

    midpoint = floor_to_day(first_day + (last_day - first_day) // 2)
    log.warning(
        "statistics report window hit Criteo's row cap, splitting it in half",
        {
            "report": report.name,
            "start_date": first_day.date().isoformat(),
            "end_date": last_day.date().isoformat(),
            "rows": rows,
            "row_limit": REPORT_ROW_LIMIT,
        },
    )

    for half_first, half_last in (
        (first_day, midpoint),
        (midpoint + TICK, last_day),
    ):
        async for row in _fetch_report_window(
            http, report, document_model, advertiser_ids, half_first, half_last, log
        ):
            yield row


async def probe_report(
    http: HTTPSession,
    report: ReportConfig,
    document_model: type[ReportRow],
    advertiser_ids: list[str],
    log: Logger,
) -> None:
    """Ask Criteo to serve one day of a report, to prove its definition is valid.

    Criteo is the authority on which dimensions, metrics, currencies and
    timezones it accepts, and the combinations it will serve together. Asking it
    directly is worth more than any list this connector could hard-code and keep
    up to date, and it reports the problem in Criteo's own words.

    Raises HTTPError if the request is rejected, RuntimeError if the response
    carries `errors`, and a pydantic ValidationError if the returned columns
    don't match what the report's documents are keyed on.
    """
    # Yesterday: a whole elapsed day, always a valid thing to ask for.
    #
    # The response is drained rather than stopped at its first row. Criteo
    # carries `errors` in the response envelope, outside the streamed `Rows`
    # array, so the envelope is only complete once the body is exhausted;
    # abandoning the generator early would discard a rejection Criteo reported
    # alongside rows.
    probe_day = floor_to_day(datetime.now(tz=UTC)) - TICK

    async for _ in _request_report_window(
        http, report, document_model, advertiser_ids, probe_day, probe_day, log
    ):
        pass


async def backfill_report(
    http: HTTPSession,
    report: ReportConfig,
    document_model: type[ReportRow],
    configured_advertiser_ids: list[str],
    start_date: datetime,
    window_size: int,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ReportRow | PageCursor, None]:
    """Walk historical report days from `start_date` up to (not including) `cutoff`.

              start_date   page_cursor   cutoff - 1d    cutoff
    ───────────────┼────────────┼────────────┼───────────┼──▶ time (1d ticks)
                   │            │            │           │
    startDate ─────[════════════╪════════════╪══▶        │
    endDate ═══════╪════════════╪════════════]           │
    window ────────[════════════╪════════════]           │
                   │            │            │           └─ owned by fetch_report
                   │            │            │              from here on
                   │            │            └─ last day this task collects
                   │            └─ resume point; on a resumed invocation the
                   │               window starts here rather than at start_date
                   └─ floored to a whole UTC day

    Both report boundaries are inclusive whole days, so a window ends at
    `cutoff - 1 day` and the next one starts the day after it.

    The first day is clamped to Criteo's two-year history, which is what makes a
    `start_date` older than that walk only the days Criteo can actually serve.
    """
    assert isinstance(cutoff, datetime)

    if page_cursor is None:
        window_start = floor_to_day(start_date)
    else:
        assert isinstance(page_cursor, str)
        window_start = datetime.fromisoformat(page_cursor)

    window_start = clamp_to_retention(window_start, report, log)
    final_day = floor_to_day(cutoff) - TICK
    if window_start > final_day:
        return

    advertiser_ids = await resolve_advertiser_ids(http, configured_advertiser_ids, log)
    window_end = min(window_start + (window_size - 1) * TICK, final_day)

    async for row in _fetch_report_window(
        http, report, document_model, advertiser_ids, window_start, window_end, log
    ):
        yield row

    next_window_start = window_end + TICK
    if next_window_start <= final_day:
        yield next_window_start.isoformat()


async def fetch_report(
    http: HTTPSession,
    report: ReportConfig,
    document_model: type[ReportRow],
    configured_advertiser_ids: list[str],
    start_date: datetime,
    window_size: int,
    lookback_days: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ReportRow | LogCursor, None]:
    """Re-read recent report days, ending at the present instant.

           cursor - lookback      cursor          today        now
    ─────────────┼──────────────────┼───────────────┼───────────┼──▶ 1d ticks
                 │                  │               │           │
    startDate ───[══════════════════╪═══════════════╪══▶        │
    endDate ═════╪══════════════════╪═══════════════]           │
    emitted ─────[══════════════════╪═══════════════]           │
                 │                  │               └─ the current day is still
                 │                  │                  accruing, and is re-read
                 │                  │                  by every later sweep
                 │                  └─ already captured, re-read anyway
                 └─ clamped to start_date, and to Criteo's two-year history

    Both report boundaries are inclusive whole days. The emitted cursor is the
    wall-clock instant the sweep began, not a day boundary: a report day is never
    final, so the cursor records how far time has advanced rather than asserting
    that any day is closed.
    """
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)
    if now <= log_cursor:
        return

    advertiser_ids = await resolve_advertiser_ids(http, configured_advertiser_ids, log)

    first_day = clamp_to_retention(
        max(
            floor_to_day(start_date),
            floor_to_day(log_cursor) - lookback_days * TICK,
        ),
        report,
        log,
    )
    final_day = floor_to_day(now)

    window_start = first_day
    cursor = log_cursor
    while window_start <= final_day:
        window_end = min(window_start + (window_size - 1) * TICK, final_day)

        async for row in _fetch_report_window(
            http, report, document_model, advertiser_ids, window_start, window_end, log
        ):
            yield row

        window_start = window_end + TICK

        # Checkpoint between windows so a long sweep doesn't restart from the
        # beginning. Cursors stay below `now` so the final one is always greater.
        if cursor < window_start < now:
            cursor = window_start
            yield cursor

    yield now
