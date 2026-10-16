import json
import re
from collections.abc import AsyncGenerator, Iterator
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import Any
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers
from pydantic import BaseModel

from .api import TikTokAPIError, TikTokClient, list_campaign_ids, paginate
from .models import (
    TIKTOK_DATETIME_FORMAT,
    DataLevel,
    Granularity,
    ReportConfig,
    ReportDocument,
)

REPORT_PATH = "report/integrated/get/"
REPORT_PAGE_SIZE = 1000

# TikTok caps a synchronous report request at 30 days of daily rows and one day
# of hourly rows.
WINDOW_DAYS: dict[Granularity, int] = {
    Granularity.DAILY: 30,
    Granularity.HOURLY: 1,
}

# Reports drop deleted campaigns, ad groups and ads by default; this filter keeps
# their history. TikTok validates report filter fields, so a wrong field fails
# the request instead of being ignored. Each status filter applies at its own
# level only, so advertiser-level reports send none.
STATUS_FILTER_FIELD: dict[DataLevel, str] = {
    DataLevel.CAMPAIGN: "campaign_status",
    DataLevel.AD_GROUP: "adgroup_status",
    DataLevel.AD: "ad_status",
}

# Above 20,000 ads TikTok processes only the newest 20,000 and still answers
# `code 0`; this header is the only sign. TikTok's remedy is to request the
# report in batches of at most 100 campaigns.
TRUNCATION_HEADER = "X-Tt-Ads-Throttle"
CAMPAIGN_BATCH_SIZE = 100

# "Query too large": TikTok asks for a smaller request, so the window is halved.
QUERY_TOO_LARGE = 40067

_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")

# TikTok's attribute metrics (names and settings) are text even when they look
# numeric, like an ad named "007", so they keep the value TikTok sends. Those
# ending in `_id` are covered by the ID rule in `_metric_value`.
ATTRIBUTE_METRICS = frozenset(
    {
        "ad_id_v2",
        "ad_name",
        "ad_profile_image",
        "ad_text",
        "ad_url",
        "adgroup_download_url",
        "adgroup_name",
        "advertiser_name",
        "aeo_type",
        "app_promotion_type",
        "bid",
        "bid_secondary_goal",
        "bid_strategy",
        "billing_event",
        "budget",
        "call_to_action",
        "campaign_automation_type",
        "campaign_budget",
        "campaign_dedicate_type",
        "campaign_name",
        "currency",
        "dpa_target_audience_type",
        "image_mode",
        "is_aco",
        "is_smart_creative",
        "minis_name",
        "objective_type",
        "optimization_event",
        "opt_status",
        "placement_type",
        "pricing_category",
        "profile_image",
        "promotion_type",
        "smart_target",
        "split_test",
        "tt_app_name",
    }
)


def _now() -> datetime:
    return datetime.now(tz=UTC)


class ReportRow(BaseModel):
    dimensions: dict[str, Any]
    metrics: dict[str, Any]


class _TruncatedReport(Exception):
    """TikTok truncated a report request to its 20,000 most recently created
    ads."""


def _report_params(
    report: ReportConfig,
    advertiser_id: str,
    start: date | None,
    end: date | None,
    campaign_ids: list[str] | None = None,
) -> dict[str, Any]:
    filters: list[dict[str, str]] = []
    if report.data_level in STATUS_FILTER_FIELD:
        filters.append(
            {
                "field_name": STATUS_FILTER_FIELD[report.data_level],
                "filter_type": "IN",
                "filter_value": json.dumps(["STATUS_ALL"]),
            }
        )
    if campaign_ids is not None:
        filters.append(
            {
                "field_name": "campaign_ids",
                "filter_type": "IN",
                "filter_value": json.dumps(campaign_ids),
            }
        )
    params: dict[str, Any] = {
        "advertiser_id": advertiser_id,
        "report_type": str(report.report_type),
        "data_level": str(report.data_level),
        "dimensions": json.dumps(report.dimensions()),
        "metrics": json.dumps(report.metric_list),
    }
    if filters:
        params["filtering"] = json.dumps(filters)
    if start is None or end is None:
        params["query_lifetime"] = "true"
    else:
        params["start_date"] = start.isoformat()
        params["end_date"] = end.isoformat()
    return params


def _metric_value(name: str, value: Any) -> Any:
    """TikTok sends every metric as a string. Attribute metrics and IDs keep
    TikTok's value; otherwise numbers become numbers and `-` (no value) null."""
    if name in ATTRIBUTE_METRICS or name.endswith("_id"):
        return value
    if value == "-":
        return None
    if isinstance(value, str) and _NUMBER.fullmatch(value):
        return float(value) if "." in value else int(value)
    return value


def _to_document[DocT: ReportDocument](
    report: ReportConfig,
    model: type[DocT],
    advertiser_id: str,
    zone: ZoneInfo,
    row: ReportRow,
) -> DocT:
    doc = {k: _metric_value(k, v) for k, v in row.metrics.items()}
    doc.update(row.dimensions)
    doc["advertiser_id"] = advertiser_id

    # Report times are wall-clock times in the advertiser's zone, without an offset.
    if report.granularity == Granularity.DAILY:
        doc["stat_time_day"] = str(row.dimensions["stat_time_day"])[:10]
    elif report.granularity == Granularity.HOURLY:
        doc["stat_time_hour"] = datetime.strptime(
            row.dimensions["stat_time_hour"], TIKTOK_DATETIME_FORMAT
        ).replace(tzinfo=zone)

    return model.model_validate(doc)


async def _request_documents[DocT: ReportDocument](
    client: TikTokClient,
    report: ReportConfig,
    model: type[DocT],
    advertiser_id: str,
    zone: ZoneInfo,
    start: date | None,
    end: date | None,
    campaign_ids: list[str] | None,
    should_keep_truncated: bool,
    log: Logger,
) -> AsyncGenerator[DocT, None]:
    """Every row of one report request. Raises _TruncatedReport when TikTok
    truncated it, unless `should_keep_truncated`."""

    def check_truncation(headers: Headers) -> None:
        if headers.get(TRUNCATION_HEADER) and not should_keep_truncated:
            raise _TruncatedReport(headers[TRUNCATION_HEADER])

    async for row in paginate(
        client,
        REPORT_PATH,
        _report_params(report, advertiser_id, start, end, campaign_ids),
        REPORT_PAGE_SIZE,
        ReportRow,
        log,
        check_truncation,
    ):
        yield _to_document(report, model, advertiser_id, zone, row)


async def _documents_split_by_campaign[DocT: ReportDocument](
    client: TikTokClient,
    report: ReportConfig,
    model: type[DocT],
    advertiser_id: str,
    zone: ZoneInfo,
    start: date | None,
    end: date | None,
    campaign_ids: list[str] | None,
    log: Logger,
) -> AsyncGenerator[DocT, None]:
    """Every row for the campaigns in `campaign_ids`, or for every campaign when
    None, including the rows TikTok leaves out of a request above 20,000 ads.
    Rows that can't be split by campaign are kept as TikTok returns them, with a
    warning, rather than stop the whole capture over one report."""
    window = f"{start} to {end}" if start else "lifetime"
    try:
        async for doc in _request_documents(
            client,
            report,
            model,
            advertiser_id,
            zone,
            start,
            end,
            campaign_ids,
            should_keep_truncated=False,
            log=log,
        ):
            yield doc
        return
    except _TruncatedReport:
        if report.data_level == DataLevel.ADVERTISER:
            unsplittable = "advertiser-level rows can't be split by campaign"
        elif campaign_ids is not None and len(campaign_ids) == 1:
            unsplittable = f"campaign {campaign_ids[0]} alone has over 20,000 ads"
        else:
            unsplittable = None

    if unsplittable:
        log.warning(
            "TikTok truncated a report that can't be split further; capturing "
            "the rows it returns, which leave out its oldest ads",
            {
                "report": report.name,
                "advertiser_id": advertiser_id,
                "window": window,
                "reason": unsplittable,
            },
        )
        async for doc in _request_documents(
            client,
            report,
            model,
            advertiser_id,
            zone,
            start,
            end,
            campaign_ids,
            should_keep_truncated=True,
            log=log,
        ):
            yield doc
        return

    # Every row belongs to exactly one campaign, so batches of campaigns split
    # the rows without overlap. Rows already emitted are re-read; the key
    # collapses them.
    if campaign_ids is None:
        ids = await list_campaign_ids(client, advertiser_id, log)
        batches = [
            ids[i : i + CAMPAIGN_BATCH_SIZE]
            for i in range(0, len(ids), CAMPAIGN_BATCH_SIZE)
        ]
    else:
        middle = len(campaign_ids) // 2
        batches = [campaign_ids[:middle], campaign_ids[middle:]]
    log.info(
        "TikTok truncated the report, reading it in batches of campaigns",
        {
            "report": report.name,
            "advertiser_id": advertiser_id,
            "window": window,
            "batches": len(batches),
        },
    )
    for batch in batches:
        async for doc in _documents_split_by_campaign(
            client, report, model, advertiser_id, zone, start, end, batch, log
        ):
            yield doc


async def _documents_split_by_date[DocT: ReportDocument](
    client: TikTokClient,
    report: ReportConfig,
    model: type[DocT],
    advertiser_id: str,
    zone: ZoneInfo,
    start: date | None,
    end: date | None,
    log: Logger,
) -> AsyncGenerator[DocT, None]:
    """Every row of one advertiser's report over `[start, end]` (inclusive, in the
    advertiser's dates), or its lifetime rows when both are None. A range TikTok
    calls too large (`40067`) is read in halves."""
    try:
        async for doc in _documents_split_by_campaign(
            client, report, model, advertiser_id, zone, start, end, None, log
        ):
            yield doc
    except TikTokAPIError as err:
        if err.code != QUERY_TOO_LARGE or start is None or end is None or start >= end:
            raise
        # Rows already emitted are re-read by the halves; the key collapses them.
        middle = start + (end - start) // 2
        log.info(
            "TikTok reported the query too large, splitting the window",
            {"report": report.name, "start": str(start), "end": str(end)},
        )
        for half_start, half_end in (
            (start, middle),
            (middle + timedelta(days=1), end),
        ):
            async for doc in _documents_split_by_date(
                client, report, model, advertiser_id, zone, half_start, half_end, log
            ):
                yield doc


def _windows(first: date, last: date, days: int) -> Iterator[tuple[date, date]]:
    """Inclusive date spans of at most `days` days covering `[first, last]`."""
    start = first
    while start <= last:
        end = min(start + timedelta(days=days - 1), last)
        yield start, end
        start = end + timedelta(days=1)


async def fetch_report[DocT: ReportDocument](
    client: TikTokClient,
    advertiser_id: str,
    zone: ZoneInfo,
    report: ReportConfig,
    model: type[DocT],
    start_date: datetime,
    lookback_days: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[DocT | LogCursor, None]:
    """Re-read one advertiser's recent report days, from the cursor's date less
    the lookback through today, in the advertiser's own dates.

                first          last sweep           today
    ──────────────┼─────────────────┼─────────────────┼───────▶ date
                  │                 │                 │
    start_date ═══[═════════════════╪═════════════════╪═════════▶
    end_date ═════╪═════════════════╪═════════════════]
    re-read ──────[═════════════════╪═════════════════]
                  │                 │                 └─ today, partial:
                  │                 │                    read again next sweep
                  │                 └─ the last sweep's date
                  └─ cursor date − lookback:
                     attribution can still
                     move these days

    Both bounds are inclusive dates in the advertiser's time zone, `zone`. The
    lookback applies on the first sweep of each local day; later sweeps that day
    start at the cursor's date. A re-read row replaces the earlier one by key.
    Lifetime reports have no dates and are read whole. The cursor is the instant
    the sweep started.
    """
    assert isinstance(log_cursor, datetime)
    swept_at = _now()

    if report.granularity == Granularity.LIFETIME:
        async for doc in _documents_split_by_date(
            client, report, model, advertiser_id, zone, None, None, log
        ):
            yield doc
    else:
        cursor_day = log_cursor.astimezone(zone).date()
        today = swept_at.astimezone(zone).date()
        # Re-reading the lookback emits every row again, so it is done on the
        # first sweep of each local day only.
        lookback = lookback_days if cursor_day < today else 0
        first = max(
            cursor_day - timedelta(days=lookback),
            start_date.astimezone(zone).date(),
        )
        for start, end in _windows(first, today, WINDOW_DAYS[report.granularity]):
            async for doc in _documents_split_by_date(
                client, report, model, advertiser_id, zone, start, end, log
            ):
                yield doc

    yield swept_at


async def backfill_report[DocT: ReportDocument](
    client: TikTokClient,
    advertiser_id: str,
    zone: ZoneInfo,
    report: ReportConfig,
    model: type[DocT],
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[DocT | PageCursor, None]:
    """Read one window of one advertiser's history per invocation, from
    `start_date` up to the day before the cutoff's date, in the advertiser's own
    dates.

              W               W+span−1      cutoff−1       cutoff
    ──────────┼───────────────────┼─────────────┼─────────────┼───────▶ date
              │                   │             │             │
    start ════[═══════════════════╪═════════════╪═════════════╪═════════▶
    end ══════╪═══════════════════]             │             │
    read ─────[═══════════════════]             │             │
              │                   │             │             └─ incremental
              │                   │             │                starts here
              │                   │             └─ the backfill's last day
              │                   └─ the next window starts
              │                      the day after
              └─ the PageCursor, a date

    Bounds are inclusive dates in the advertiser's time zone, `zone`; the last
    window ends at the day before the cutoff's date. Dates never change meaning,
    so a window re-reads identically after any resume gap.
    """
    assert isinstance(cutoff, datetime)

    last_day = cutoff.astimezone(zone).date() - timedelta(days=1)
    if page_cursor is None:
        window_start = start_date.astimezone(zone).date()
    else:
        assert isinstance(page_cursor, str)
        window_start = date.fromisoformat(page_cursor)

    if window_start > last_day:
        return

    window_end = min(
        window_start + timedelta(days=WINDOW_DAYS[report.granularity] - 1), last_day
    )
    async for doc in _documents_split_by_date(
        client, report, model, advertiser_id, zone, window_start, window_end, log
    ):
        yield doc

    if window_end < last_day:
        yield (window_end + timedelta(days=1)).isoformat()


async def check_report(
    client: TikTokClient,
    report: ReportConfig,
    advertiser_id: str,
    log: Logger,
) -> None:
    """Request a single row of `report`, so TikTok rejects an invalid metric or
    dimension combination before the capture starts. Raises TikTokAPIError."""
    if report.granularity == Granularity.LIFETIME:
        start = end = None
    else:
        start = end = _now().date() - timedelta(days=1)
    async for _ in paginate(
        client,
        REPORT_PATH,
        _report_params(report, advertiser_id, start, end),
        1,
        ReportRow,
        log,
    ):
        break
