from datetime import date, datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator, Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from pydantic import BaseModel, Field

from .api import (
    Advertiser,
    AdvertiserRegistry,
    Envelope,
    PageInfo,
    json_param,
    raise_for_code,
    warn_if_ads_truncated,
)
from .models import (
    Granularity,
    Report,
    ReportDocument,
)


REPORT_PATH = "report/integrated/get/"
REPORT_PAGE_SIZE = 1000

# TikTok caps a report's date span by the time dimension it carries.
MAX_DAILY_SPAN = timedelta(days=30)
MAX_HOURLY_SPAN = timedelta(days=1)

# Absent metric values arrive as this literal rather than as null.
EMPTY_METRIC = "-"

REPORT_DATE_FORMAT = "%Y-%m-%d"
REPORT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class ReportRow(BaseModel, extra="allow"):
    dimensions: dict[str, Any]
    metrics: dict[str, Any]


class ReportResponse(Envelope):
    class Data(BaseModel, extra="allow"):
        page_info: PageInfo | None = None
        # Aliased because TikTok names this "list", which would shadow the builtin.
        rows: list[ReportRow] = Field(default_factory=list, alias="list")

    data: Data | None = None


def advertiser_timezone(advertiser: Advertiser, log: Logger) -> ZoneInfo:
    """Resolve the zone a report's dates are expressed in.

    `display_timezone` is preferred because `timezone` may carry a POSIX-style name such as
    `Etc/GMT+8`, whose sign is inverted relative to the offset it appears to name.
    """
    for candidate in (advertiser.display_timezone, advertiser.timezone):
        if not candidate:
            continue

        try:
            return ZoneInfo(candidate)
        except (ZoneInfoNotFoundError, ValueError):
            log.warning(
                "advertiser reported an unusable timezone",
                {
                    "advertiser_id": advertiser.advertiser_id,
                    "timezone": candidate,
                },
            )

    return ZoneInfo("UTC")


def _parse_report_datetime(value: str, tz: ZoneInfo) -> datetime:
    """Attach the advertiser's zone to a report timestamp.

    Report timestamps are expressed in the ad account's own timezone and carry no offset, so
    the same wall-clock string denotes different instants for different advertisers.
    """
    return datetime.strptime(value, REPORT_DATETIME_FORMAT).replace(tzinfo=tz)


def _to_report_record(
    row: ReportRow,
    report: Report,
    advertiser_id: str,
    tz: ZoneInfo,
) -> dict[str, Any]:
    record: dict[str, Any] = dict(row.dimensions)

    for name, value in row.metrics.items():
        record[name] = None if value == EMPTY_METRIC else value

    # Single-advertiser report rows never name the account they belong to, so without this
    # the key would be ambiguous across advertisers.
    record["advertiser_id"] = advertiser_id

    time_dimension = report.time_dimension
    if time_dimension is not None and isinstance(record.get(time_dimension), str):
        record[time_dimension] = _parse_report_datetime(record[time_dimension], tz)

    return record


def _report_windows(
    start: date, end: date, granularity: Granularity
) -> Iterator[tuple[date, date]]:
    """Split an inclusive date range into spans TikTok will accept for this granularity."""
    if granularity is Granularity.LIFETIME:
        return

    span = MAX_HOURLY_SPAN if granularity is Granularity.HOURLY else MAX_DAILY_SPAN
    # Both endpoints are inclusive, so a 30-day span reaches start + 29 days.
    stride = span - timedelta(days=1)

    window_start = start
    while window_start <= end:
        window_end = min(window_start + stride, end)
        yield (window_start, window_end)
        window_start = window_end + timedelta(days=1)


async def _fetch_report_pages(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    report: Report,
    advertiser_id: str,
    tz: ZoneInfo,
    window: tuple[date, date] | None,
) -> AsyncGenerator[dict[str, Any], None]:
    """Walk every page of one report window for one advertiser."""
    url = f"{base_url}/{REPORT_PATH}"
    page = 1

    while True:
        request_params: dict[str, Any] = {
            "advertiser_id": advertiser_id,
            "report_type": report.report_type.value,
            "data_level": report.data_level.value,
            "dimensions": json_param(report.all_dimensions()),
            "metrics": json_param(report.metrics),
            "page": page,
            "page_size": REPORT_PAGE_SIZE,
        }

        if window is None:
            request_params["query_lifetime"] = "true"
        else:
            window_start, window_end = window
            request_params["start_date"] = window_start.strftime(REPORT_DATE_FORMAT)
            request_params["end_date"] = window_end.strftime(REPORT_DATE_FORMAT)

        headers, body = await http.request_stream(log, url, params=request_params)
        chunks = [chunk async for chunk in body()]

        response = ReportResponse.model_validate_json(b"".join(chunks))
        raise_for_code(response, url)
        warn_if_ads_truncated(headers, log, url, response.request_id)

        if response.data is None:
            return

        for row in response.data.rows:
            yield _to_report_record(row, report, advertiser_id, tz)

        page_info = response.data.page_info
        if page_info is None or page >= page_info.total_page:
            return

        page += 1


async def _sweep_report(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    report: Report,
    doc_model: type[ReportDocument],
    registry: AdvertiserRegistry,
    start: date,
    end: date,
) -> AsyncGenerator[ReportDocument, None]:
    """Read one report across every advertiser, each in its own account timezone."""
    for advertiser in await registry.accounts(log):
        tz = advertiser_timezone(advertiser, log)

        windows: list[tuple[date, date] | None]
        if report.granularity is Granularity.LIFETIME:
            windows = [None]
        else:
            # An advertiser east of UTC can already be on a later date than the sweep
            # assumed, and one to the west on an earlier one; clamp to its own today.
            local_end = min(end, datetime.now(tz=tz).date())
            if start > local_end:
                continue
            windows = list(_report_windows(start, local_end, report.granularity))

        for window in windows:
            async for record in _fetch_report_pages(
                http, base_url, log, report, advertiser.advertiser_id, tz, window
            ):
                yield doc_model.model_validate(record)


async def fetch_report(
    http: HTTPSession,
    base_url: str,
    report: Report,
    doc_model: type[ReportDocument],
    registry: AdvertiserRegistry,
    start_date: datetime,
    lookback_window_days: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ReportDocument | LogCursor, None]:
    """Re-read the lookback window, then catch up to the present.

        cursor - lookback      cursor              today
    ────────────┼──────────────────┼──────────────────┼───▶ time (1d ticks)
                │                  │                  │
    start_date ═[══════════════════╪══════════════════╪══▶
    emitted ────[══════════════════╪══════════════════]
                │                  │                  └─ still accruing; a later
                │                  │                     poll re-reads it
                │                  └─ prior cursor
                └─ re-read, because TikTok attributes a conversion to the day of
                   the ad interaction, so rows behind the cursor keep changing
                   until every ad group's attribution window closes

    Both report date bounds are inclusive. The cursor is sampled before the sweep starts, so
    a row that changes mid-sweep is re-read next poll rather than missed.
    """
    assert isinstance(log_cursor, datetime)

    swept_at = datetime.now(tz=UTC)
    replay_from = max(log_cursor - timedelta(days=lookback_window_days), start_date)

    async for doc in _sweep_report(
        http,
        base_url,
        log,
        report,
        doc_model,
        registry,
        replay_from.date(),
        swept_at.date(),
    ):
        yield doc

    yield swept_at


async def backfill_report(
    http: HTTPSession,
    base_url: str,
    report: Report,
    doc_model: type[ReportDocument],
    registry: AdvertiserRegistry,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ReportDocument | PageCursor, None]:
    """Sweep historical report windows up to the cutoff, one span per invocation.

    Resume is keyed on the window's start date. Dates are immutable, so a resumed window
    denotes the same range however long the gap was.
    """
    assert isinstance(cutoff, datetime)

    # A lifetime report always describes the whole history, so the incremental task alone
    # keeps it current and there is no historical range to walk.
    if report.granularity is Granularity.LIFETIME:
        return

    if page is None:
        start = start_date.date()
    else:
        assert isinstance(page, str)
        start = date.fromisoformat(page)

    # The incremental task owns the cutoff day onwards.
    last_day = cutoff.date() - timedelta(days=1)
    if start > last_day:
        return

    span = MAX_HOURLY_SPAN if report.granularity is Granularity.HOURLY else MAX_DAILY_SPAN
    end = min(start + span - timedelta(days=1), last_day)

    async for doc in _sweep_report(
        http, base_url, log, report, doc_model, registry, start, end
    ):
        yield doc

    next_start = end + timedelta(days=1)
    if next_start > last_day:
        return

    yield next_start.isoformat()
