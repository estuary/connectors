from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from urllib.parse import urlencode

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession

from .models import (
    CostRow,
    Granularity,
    ReportPage,
    ReportRow,
    UsageRow,
    utc_now,
)

API = "https://api.anthropic.com"

# Every Usage and Cost Analytics API request carries these. The beta header is
# required by the analytics endpoints; the version pins response shapes.
ANTHROPIC_HEADERS = {
    "anthropic-version": "2026-10-21",
    "anthropic-beta": "analytics-usage-cost-2026-08-24",
}

PAGE_LIMIT = 1000
# Anthropic refreshes its data every few hours, so a window whose page cursor
# keeps expiring is stuck; raising lets the CDK restart the task instead.
MAX_WINDOW_RESTARTS = 3
# Anthropic serves nothing earlier than this, nor older than RETENTION.
DATA_FLOOR = datetime(2026, 1, 1, tzinfo=UTC)
RETENTION = timedelta(days=365)
# Anthropic may revise a month's numbers until about seven days after it ends
# (values for October 1 can change until about November 7), and revisions can
# take up to 24 hours to export. By 00:00 UTC on the 9th the previous month is
# final.
# https://platform.claude.com/docs/en/manage-claude/usage-cost-analytics-api#data-freshness
MONTH_SETTLE_DAY = 9


def floor_to(dt: datetime, tick: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return epoch + ((dt - epoch) // tick) * tick


def ceil_to(dt: datetime, tick: timedelta) -> datetime:
    floored = floor_to(dt, tick)
    return floored if floored == dt else floored + tick


def earliest_start(start_date: datetime, granularity: Granularity, now: datetime) -> datetime:
    return max(
        floor_to(start_date, granularity.tick),
        DATA_FLOOR,
        ceil_to(now - RETENTION, granularity.tick),
    )


def previous_month_start(now: datetime) -> datetime:
    first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return (first_of_month - timedelta(days=1)).replace(day=1)


def previous_month_settled_at(now: datetime) -> datetime:
    """When the month before `now`'s month stops being revised."""
    first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_of_month + timedelta(days=MONTH_SETTLE_DAY - 1)


def _windows(
    start: datetime, end: datetime, span: timedelta
) -> list[tuple[datetime, datetime]]:
    windows = []
    while start < end:
        windows.append((start, min(start + span, end)))
        start += span
    return windows


def _rfc3339(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _fetch_window[RowT: ReportRow](
    log: Logger,
    http: HTTPSession,
    model: type[RowT],
    granularity: Granularity,
    group_by: list[str],
    start: datetime,
    end: datetime,
) -> AsyncGenerator[RowT, None]:
    """Yield every row with a bucket in [start, end), both bucket-aligned,
    one row per (starting_at, group_key): rows sharing a key are merged by
    summing their metrics.

    Rows arrive ordered by bucket, so a bucket's rows are held until the next
    bucket starts (they may span pages) and then yielded.

    A page cursor that expires mid-window (HTTP 410, when Anthropic refreshes
    the data) restarts the window from its first page, up to
    MAX_WINDOW_RESTARTS times; after that the 410 is raised. Rows re-read
    that way collapse by key.
    """
    request_params: list[tuple[str, str]] = [
        ("starting_at", _rfc3339(start)),
        ("ending_at", _rfc3339(end)),
        ("time_bucket_width", granularity.bucket_width),
        ("order_by", "time"),
        ("order", "asc"),
        ("limit", str(PAGE_LIMIT)),
    ] + [("group_by[]", dim) for dim in group_by + model.EXTRA_GROUP_BY]

    url = f"{API}/v1/organizations/analytics/{model.PATH}"
    page: str | None = None
    restarts = 0
    # Rows of the bucket currently being read, by group_key.
    bucket: dict[str, RowT] = {}
    bucket_start: datetime | None = None

    while True:
        params = request_params + ([("page", page)] if page else [])
        try:
            body = await http.request(
                log, f"{url}?{urlencode(params)}", headers=dict(ANTHROPIC_HEADERS)
            )
        except HTTPError as err:
            if err.code == 410 and page is not None and restarts < MAX_WINDOW_RESTARTS:
                restarts += 1
                log.info(
                    "page cursor expired, restarting window",
                    {"start": start, "end": end, "path": model.PATH, "restarts": restarts},
                )
                page = None
                bucket, bucket_start = {}, None
                continue
            raise

        result = ReportPage[model].model_validate_json(body)
        if page is None:
            # Anthropic's completeness watermark: rows after it are partial.
            # Logged so "why is the last few hours missing?" is answerable
            # from the task logs.
            log.info(
                "fetched report window",
                {
                    "path": model.PATH,
                    "bucket_width": granularity.bucket_width,
                    "start": start,
                    "end": end,
                    "data_refreshed_at": result.data_refreshed_at,
                },
            )
        for row in result.data:
            if bucket_start is not None and row.starting_at != bucket_start:
                if row.starting_at < bucket_start:
                    raise RuntimeError(
                        f"rows out of time order: {row.starting_at} after {bucket_start}"
                    )
                for merged in bucket.values():
                    yield merged
                bucket = {}
            bucket_start = row.starting_at

            if (existing := bucket.get(row.group_key)) is not None:
                existing.merge(row)
            else:
                bucket[row.group_key] = row

        if not result.has_more or not result.next_page:
            for merged in bucket.values():
                yield merged
            return
        page = result.next_page


async def _fetch_report[RowT: ReportRow](
    http: HTTPSession,
    model: type[RowT],
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    lookback: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[RowT | LogCursor, None]:
    """Re-fetch the lookback before the last sweep through the current
    bucket, then advance the cursor to the time of this sweep.

    The cursor is when the last sweep ran. Anthropic exports data in batches
    up to 24 hours late and revises it afterwards, so every sweep re-reads
    the lookback; re-read rows overwrite earlier ones by key.

          cursor - lookback     cursor            now
    ──────────────┼───────────────┼─────────────────┼───┼───▶ time
                  │               │                 │   │
    starting_at ──[═══════════════╪═════════════════╪═══╪═══▶
    ending_at ════╪═══════════════╪═════════════════╪═══)
    emitted ──────[═══════════════╪═════════════════]
                  │               │                 │   └─ now, ceiled
                  │               │                 └─ partial; re-read
                  │               │                    next sweep
                  │               └─ read last sweep; re-read for
                  │                  late and revised data
                  └─ floored to a bucket; minutely
                     uses now - lookback

    `starting_at` is inclusive and `ending_at` exclusive, at bucket bounds.
    Once a month, the first sweep at or after 00:00 UTC on the
    MONTH_SETTLE_DAY also reaches back to the start of the previous month, to
    pick up its final revised values. A sweep missed on that day (the task
    was down) happens on the next sweep instead, since the cursor still
    predates the settle point.
    """
    assert isinstance(log_cursor, datetime)

    now = utc_now()
    end = ceil_to(now, granularity.tick)

    if granularity.rolling_window is not None:
        # Re-read the lookback too, so late and revised minutes are picked up
        # like the hourly and daily streams' are.
        reread = max(granularity.rolling_window, lookback)
        start = floor_to(now - reread, granularity.tick)
    else:
        start = floor_to(log_cursor - lookback, granularity.tick)
        if log_cursor < previous_month_settled_at(now) <= now:
            start = min(start, previous_month_start(now))

    start = max(start, earliest_start(start_date, granularity, now))

    for window_start, window_end in _windows(start, end, granularity.max_span):
        async for row in _fetch_window(
            log, http, model, granularity, group_by, window_start, window_end
        ):
            yield row

    if now > log_cursor:
        yield now


async def _backfill_report[RowT: ReportRow](
    http: HTTPSession,
    model: type[RowT],
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[RowT | PageCursor, None]:
    """Fetch every bucket from the start date up to the cutoff's bucket.

            page or start      last bucket          cutoff bucket
    ──────────────┼─────────────────┼───┼─────────────────┼───▶ time
                  │                 │   │                 │
    starting_at ──[═════════════════╪═══╪═════════════════╪═══▶
    ending_at ════╪═════════════════╪═══)
    emitted ──────[═════════════════]
                  │                 │   │                 └─ incremental
                  │                 │   │                    owns it
                  │                 │   └─ window end; the next
                  │                 │      page starts here
                  │                 └─ last bucket of the window
                  └─ clamped to 2026-01-01 and 365 days
                     of retention

    The page cursor is the RFC 3339 start of the next window, an immutable
    time boundary.
    """
    assert isinstance(cutoff, datetime)

    end = floor_to(cutoff, granularity.tick)
    start = (
        datetime.fromisoformat(page)
        if isinstance(page, str)
        else earliest_start(start_date, granularity, utc_now())
    )

    for window_start, window_end in _windows(start, end, granularity.max_span):
        async for row in _fetch_window(
            log, http, model, granularity, group_by, window_start, window_end
        ):
            yield row

        if window_end < end:
            yield _rfc3339(window_end)


async def fetch_usage(
    http: HTTPSession,
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    lookback: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[UsageRow | LogCursor, None]:
    async for item in _fetch_report(
        http, UsageRow, granularity, group_by, start_date, lookback, log, log_cursor
    ):
        yield item


async def backfill_usage(
    http: HTTPSession,
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[UsageRow | PageCursor, None]:
    async for item in _backfill_report(
        http, UsageRow, granularity, group_by, start_date, log, page, cutoff
    ):
        yield item


async def fetch_cost(
    http: HTTPSession,
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    lookback: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CostRow | LogCursor, None]:
    async for item in _fetch_report(
        http, CostRow, granularity, group_by, start_date, lookback, log, log_cursor
    ):
        yield item


async def backfill_cost(
    http: HTTPSession,
    granularity: Granularity,
    group_by: list[str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[CostRow | PageCursor, None]:
    async for item in _backfill_report(
        http, CostRow, granularity, group_by, start_date, log, page, cutoff
    ):
        yield item
