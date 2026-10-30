import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from logging import DEBUG, Logger
from typing import Any, AsyncGenerator, Literal

from pydantic import BaseModel, create_model

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_csv_processor import (
    BaseCSVRow,
    CSVConfig,
    IncrementalCSVProcessor,
)
from estuary_cdk.unzip_stream import UnzipStream

from .api import AccountContext, reporting_url
from .errors import fault_of
from .models import MAX_LOOKBACK_WINDOW_DAYS, utc_now

# One cap on in-flight report jobs (submit through download) for the whole
# connector process. Microsoft doesn't publish the concurrent-report limit
# behind fault 207, nor whether it counts per customer, user or developer
# token, so this covers the broadest scope the connector controls.
# [UNVERIFIED] Tune from production.
MAX_CONCURRENT_REPORT_JOBS = 10
_report_jobs = asyncio.Semaphore(MAX_CONCURRENT_REPORT_JOBS)

# Microsoft suggests polling every few minutes and giving up after an hour.
# Waits stay short so a stopping task isn't held up.
# https://learn.microsoft.com/en-us/advertising/guides/request-download-report?view=bingads-13
POLL_INTERVAL = timedelta(seconds=15)
MAX_POLL_WAIT = timedelta(hours=1)

# Days requested per backfill job, so a long history checkpoints often.
BACKFILL_WINDOW_DAYS = 30
# Days requested per incremental job: enough that the longest regular sweep
# (the maximum lookback of 90 days, plus the cursor's day and today) is one
# job. Only catching up after a long outage splits a sweep. Microsoft
# documents no maximum custom date range, and Airbyte's connector requests
# its whole start-date-to-today range in a single job.
INCREMENTAL_WINDOW_DAYS = MAX_LOOKBACK_WINDOW_DAYS + 2

TICK = timedelta(days=1)

# Report days are London days, as in Airbyte's connector, which customers are
# migrating from. "Greenwich Mean Time" observes British Summer Time, so a UTC
# date is never later than the report's current day and is always a valid
# range end. Microsoft offers no UTC time zone.
# https://learn.microsoft.com/en-us/advertising/reporting-service/reporttimezone?view=bingads-13
REPORT_TIME_ZONE = "GreenwichMeanTimeDublinEdinburghLisbonLondon"

# Attribute columns that split a report into rows: entity IDs and segments.
# Only these join AccountId and the time column in a report's key. Every other
# attribute (names, numbers, statuses, quality scores, bids, CurrencyCode)
# describes the entity a row is about, and some of them change over time, so
# keying on them would leave a stale duplicate behind whenever one changed.
# A column added to a report and missing here is never keyed: add it here
# only if it splits rows, since that changes the key.
# https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnrestrictions
GRAIN_COLUMNS = frozenset(
    {
        "AccountId",
        "CampaignId",
        "AdGroupId",
        "AdId",
        "KeywordId",
        "AdDistribution",
        "BidMatchType",
        "DeliveredMatchType",
        "DeviceOS",
        "DeviceType",
        "Language",
        "Network",
        "TopVsOther",
    }
)

def months_before(day: date, months: int) -> date:
    month_index = day.year * 12 + day.month - 1 - months
    year, month = divmod(month_index, 12)
    # Clamp to the end of shorter months.
    for d in (day.day, 30, 29, 28):
        try:
            return date(year, month + 1, d)
        except ValueError:
            continue
    raise AssertionError("unreachable")


class ReportRow(BaseCSVRow):
    """A row of a downloaded report. Values are passed through as Microsoft
    writes them, as strings; only an empty value becomes null."""

    AccountId: str


@dataclass(frozen=True)
class ReportDefinition:
    name: str
    request_type: str
    columns: tuple[str, ...]
    # How far back Microsoft keeps this report at this aggregation.
    # https://learn.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#reportretention
    retention_months: int
    time_column: Literal["TimePeriod", "Date"] = "TimePeriod"
    # None for reports that are always daily and take no Aggregation element.
    aggregation: str | None = "Daily"

    def grain_columns(self) -> list[str]:
        return sorted(c for c in self.columns if c in GRAIN_COLUMNS and c != "AccountId")

    def key(self) -> list[str]:
        return [
            "/AccountId",
            f"/{self.time_column}",
            *(f"/{c}" for c in self.grain_columns()),
        ]

    def row_model(self) -> type[ReportRow]:
        """Declares only the key columns. Schema inference types the rest."""
        fields: dict[str, Any] = {self.time_column: (str, ...)}
        fields.update({c: (str | None, None) for c in self.grain_columns()})
        return create_model(f"{self.request_type}Row", __base__=ReportRow, **fields)


class SubmitResponse(BaseModel, extra="allow"):
    ReportRequestId: str


class ReportRequestStatus(BaseModel, extra="allow"):
    Status: str
    ReportDownloadUrl: str | None = None


class PollResponse(BaseModel, extra="allow"):
    ReportRequestStatus: ReportRequestStatus


def _date(day: date) -> dict[str, int]:
    return {"Day": day.day, "Month": day.month, "Year": day.year}


def _report_request(
    definition: ReportDefinition,
    account_id: str,
    first_day: date,
    last_day: date,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "Type": definition.request_type,
        "Format": "Csv",
        "FormatVersion": "2.0",
        "ReportName": definition.name,
        "ExcludeColumnHeaders": False,
        "ExcludeReportHeader": True,
        "ExcludeReportFooter": True,
        # "Complete" data still changes with late conversions and invalid
        # traffic credits; the lookback re-fetch corrects it instead.
        "ReturnOnlyCompleteData": False,
        "Columns": list(definition.columns),
        "Scope": {"AccountIds": [account_id]},
        "Time": {
            "CustomDateRangeStart": _date(first_day),
            "CustomDateRangeEnd": _date(last_day),
            "ReportTimeZone": REPORT_TIME_ZONE,
        },
    }
    if definition.aggregation is not None:
        request["Aggregation"] = definition.aggregation
    return {"ReportRequest": request}


async def _submit(
    http: HTTPSession,
    log: Logger,
    context: AccountContext,
    request: dict[str, Any],
) -> str:
    """https://learn.microsoft.com/en-us/advertising/reporting-service/submitgeneratereport?view=bingads-13"""
    response = SubmitResponse.model_validate_json(
        await http.request(
            log,
            f"{reporting_url()}/GenerateReport/Submit",
            method="POST",
            json=request,
            headers=context.headers(),
        )
    )
    return response.ReportRequestId


async def _poll(
    http: HTTPSession,
    log: Logger,
    context: AccountContext,
    request_id: str,
) -> ReportRequestStatus:
    """https://learn.microsoft.com/en-us/advertising/reporting-service/pollgeneratereport?view=bingads-13"""
    response = PollResponse.model_validate_json(
        await http.request(
            log,
            f"{reporting_url()}/GenerateReport/Poll",
            method="POST",
            json={"ReportRequestId": request_id},
            headers=context.headers(),
        )
    )
    return response.ReportRequestStatus


async def fetch_report_rows(
    http: HTTPSession,
    log: Logger,
    context: AccountContext,
    definition: ReportDefinition,
    row_model: type[ReportRow],
    first_day: date,
    last_day: date,
) -> AsyncGenerator[ReportRow, None]:
    """Every row of one account's report for the inclusive day range.

    The request ID is never persisted: Microsoft keeps it for as little as a
    day, so an interrupted job is resubmitted.
    """
    request = _report_request(definition, context.id, first_day, last_day)
    job = f"{definition.name} for account {context.id} ({first_day} to {last_day})"

    async with _report_jobs:
        try:
            request_id = await _submit(http, log, context, request)

            deadline = datetime.now(tz=UTC) + MAX_POLL_WAIT
            while True:
                status = await _poll(http, log, context, request_id)
                if status.Status != "Pending":
                    break
                if datetime.now(tz=UTC) >= deadline:
                    raise RuntimeError(f"{job} was still pending after {MAX_POLL_WAIT}.")
                await asyncio.sleep(POLL_INTERVAL.total_seconds())
        except HTTPError as err:
            fault = fault_of(err)
            detail = fault.describe() if fault else err.message
            raise HTTPError(f"{job} failed: {detail}\n\n{err.message}", err.code) from err

        if status.Status != "Success":
            raise RuntimeError(f"{job} failed with status {status.Status}.")

        # Success without a URL means the report has no rows.
        if status.ReportDownloadUrl is None:
            log.debug("report has no data", {"report": definition.name, "account_id": context.id})
            return

        # The download URL lasts about five minutes after the poll that
        # returned it, and it's signed, so it's fetched straight away and
        # without the access token.
        _, body = await http.request_stream(log, status.ReportDownloadUrl, with_token=False)

        async def unzipped() -> AsyncGenerator[bytes, None]:
            async for chunk in UnzipStream(body()):
                yield chunk

        # Only built when debugging: a set of every key in a large report
        # costs memory the capture otherwise doesn't need.
        seen_keys: set[tuple[Any, ...]] | None = set() if log.isEnabledFor(DEBUG) else None
        key_fields = [k.lstrip("/") for k in definition.key()]

        async for row in IncrementalCSVProcessor(
            unzipped(), row_model, CSVConfig(encoding="utf-8-sig")
        ):
            if seen_keys is not None:
                key = tuple(getattr(row, f) for f in key_fields)
                if key in seen_keys:
                    # Two rows sharing a key means the key is coarser than
                    # the report's real grain, and one row overwrites the other.
                    log.debug(
                        "duplicate report key",
                        {"report": definition.name, "account_id": context.id, "key": key},
                    )
                seen_keys.add(key)
            yield row


def earliest_day(definition: ReportDefinition, start_date: datetime, today: date) -> date:
    """The configured start date, clamped to Microsoft's retention for the report."""
    return max(start_date.date(), months_before(today, definition.retention_months) + TICK)


def _windows(first_day: date, last_day: date, days: int) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    start = first_day
    while start <= last_day:
        end = min(start + (days - 1) * TICK, last_day)
        windows.append((start, end))
        start = end + TICK
    return windows


def day_cursor(day: date) -> datetime:
    """Midnight UTC at the start of a report day."""
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


async def fetch_report(
    http: HTTPSession,
    context: AccountContext,
    definition: ReportDefinition,
    row_model: type[ReportRow],
    start_date: datetime,
    lookback_window_days: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ReportRow | LogCursor, None]:
    """Fetch report days through today, re-fetching the lookback on the first
    run of each day to pick up late conversions and invalid-traffic credits.

    The cursor is when the last sweep finished. Its day was only partly over
    then, so that day is always fetched again.

    First run of a day (cursor's day before today):

     cursor day - lookback   cursor day                today
    ─────────┼───────────────────┼──────────────────────┼──▶ 1d ticks
             │                   │                      │
    RangeStart [═════════════════╪══════════════════════╪══▶
    RangeEnd ══╪═════════════════╪══════════════════════]
    emitted ───[═════════════════╪══════════════════════]
             │                   │                      └─ partial; fetched
             │                   │                         again next run
             │                   └─ partial when last fetched; final now
             └─ clamped to start_date and Microsoft's retention

    Later runs the same day (cursor's day is today):

                                                   today
    ────────────────────────────────────────────────┼──▶ 1d ticks
    RangeStart ─────────────────────────────────────[══▶
    RangeEnd ═══════════════════════════════════════]
    emitted ────────────────────────────────────────[]

    Both range ends are inclusive whole days. Days are UTC dates, which are
    never later than the report's London day, so `today` is a valid range end.
    Re-fetched rows overwrite earlier ones by key, so emitting a partial day
    is safe: its final numbers arrive on a later run.
    """
    assert isinstance(log_cursor, datetime)

    now = utc_now()
    today = now.date()
    cursor_day = log_cursor.date()
    first_day = cursor_day - lookback_window_days * TICK if cursor_day < today else today
    first_day = max(first_day, earliest_day(definition, start_date, today))

    last_cursor = log_cursor
    for window_start, window_end in _windows(first_day, today, INCREMENTAL_WINDOW_DAYS):
        async for row in fetch_report_rows(
            http, log, context, definition, row_model, window_start, window_end
        ):
            yield row

        # Only a sweep long enough to split (catching up after an outage) has
        # windows before today. Checkpoint each one past the cursor, as the
        # start of the next day to fetch.
        if window_end < today and day_cursor(window_end + TICK) > last_cursor:
            last_cursor = day_cursor(window_end + TICK)
            yield last_cursor

    if now > last_cursor:
        yield now


async def backfill_report(
    http: HTTPSession,
    context: AccountContext,
    definition: ReportDefinition,
    row_model: type[ReportRow],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ReportRow | PageCursor, None]:
    """Fetch every report day from the start date through the cutoff day.

            page (or start_date)    window end         cutoff
    ───────────────────┼────────────────────┼─────────────┼──▶ 1d ticks
                       │                    │             │
    RangeStart ────────[════════════════════╪═════════════╪══▶
    RangeEnd ══════════╪════════════════════]             │
    emitted ───────────[════════════════════]             │
                       │                    │             └─ the last
                       │                    │                backfilled day;
                       │                    │                incremental owns
                       │                    │                the days after it
                       │                    └─ the next page starts the
                       │                       day after
                       └─ start_date is clamped to Microsoft's retention

    The page cursor is the ISO date of the next window's first day, so a
    resumed backfill restarts at a window boundary.
    """
    assert isinstance(cutoff, datetime)

    cutoff_day = cutoff.date()
    first_day = (
        date.fromisoformat(page)
        if isinstance(page, str)
        else earliest_day(definition, start_date, cutoff_day)
    )

    for window_start, window_end in _windows(first_day, cutoff_day, BACKFILL_WINDOW_DAYS):
        async for row in fetch_report_rows(
            http, log, context, definition, row_model, window_start, window_end
        ):
            yield row

        if window_end < cutoff_day:
            yield (window_end + TICK).isoformat()


# The connector's built-in reports: ATF's daily report streams, with the
# columns ATF 0.2.1 requests for each, so customers keep the same columns.
REPORTS: list[ReportDefinition] = [
    ReportDefinition(
        name="account_performance_report_daily",
        request_type="AccountPerformanceReportRequest",
        retention_months=36,
        columns=(
            "AccountId", "TimePeriod", "CurrencyCode", "AdDistribution",
            "DeviceType", "Network", "DeliveredMatchType", "DeviceOS",
            "TopVsOther", "BidMatchType", "AccountName", "AccountNumber",
            "PhoneImpressions", "PhoneCalls", "Clicks", "Ctr", "Spend",
            "Impressions", "CostPerConversion", "Ptr", "Assists",
            "ReturnOnAdSpend", "CostPerAssist", "AverageCpc", "AveragePosition",
            "AverageCpm", "Conversions", "ConversionRate", "ConversionsQualified",
            "LowQualityClicks", "LowQualityClicksPercent", "LowQualityImpressions",
            "LowQualitySophisticatedClicks", "LowQualityConversions",
            "LowQualityConversionRate", "Revenue", "RevenuePerConversion",
            "RevenuePerAssist",
        ),
    ),
    ReportDefinition(
        name="campaign_performance_report_daily",
        request_type="CampaignPerformanceReportRequest",
        retention_months=36,
        columns=(
            "AccountId", "CampaignId", "TimePeriod", "CurrencyCode",
            "AdDistribution", "DeviceType", "Network", "DeliveredMatchType",
            "DeviceOS", "TopVsOther", "BidMatchType", "AccountName",
            "CampaignName", "CampaignType", "CampaignStatus", "CampaignLabels",
            "Impressions", "Clicks", "Ctr", "Spend", "CostPerConversion",
            "QualityScore", "AdRelevance", "LandingPageExperience",
            "PhoneImpressions", "PhoneCalls", "Ptr", "Assists", "ReturnOnAdSpend",
            "CostPerAssist", "CustomParameters", "ViewThroughConversions",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllConversions",
            "AllConversionRate", "AllRevenue", "AllRevenuePerConversion",
            "AverageCpc", "AveragePosition", "AverageCpm", "Conversions",
            "ConversionRate", "ConversionsQualified", "LowQualityClicks",
            "LowQualityClicksPercent", "LowQualityImpressions",
            "LowQualitySophisticatedClicks", "LowQualityConversions",
            "LowQualityConversionRate", "Revenue", "RevenuePerConversion",
            "RevenuePerAssist", "BudgetName", "BudgetStatus",
            "BudgetAssociationStatus", "HistoricalQualityScore",
            "HistoricalExpectedCtr", "HistoricalAdRelevance",
            "HistoricalLandingPageExperience",
        ),
    ),
    ReportDefinition(
        name="ad_group_performance_report_daily",
        request_type="AdGroupPerformanceReportRequest",
        retention_months=36,
        columns=(
            "AccountId", "CampaignId", "AdGroupId", "TimePeriod", "CurrencyCode",
            "AdDistribution", "DeviceType", "Network", "DeliveredMatchType",
            "DeviceOS", "TopVsOther", "BidMatchType", "Language", "AccountName",
            "CampaignName", "CampaignType", "AdGroupName", "AdGroupType",
            "Impressions", "Clicks", "Ctr", "Spend", "CostPerConversion",
            "QualityScore", "ExpectedCtr", "AdRelevance", "LandingPageExperience",
            "PhoneImpressions", "PhoneCalls", "Ptr", "Assists", "CostPerAssist",
            "CustomParameters", "FinalUrlSuffix", "ViewThroughConversions",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllConversions",
            "AllConversionRate", "AllRevenue", "AllRevenuePerConversion",
            "AverageCpc", "AveragePosition", "AverageCpm", "Conversions",
            "ConversionRate", "ConversionsQualified", "Revenue",
            "RevenuePerConversion", "RevenuePerAssist", "HistoricalQualityScore",
            "HistoricalExpectedCtr", "HistoricalAdRelevance",
            "HistoricalLandingPageExperience",
        ),
    ),
    ReportDefinition(
        name="ad_performance_report_daily",
        request_type="AdPerformanceReportRequest",
        retention_months=36,
        columns=(
            "AccountId", "CampaignId", "AdGroupId", "AdId", "TimePeriod",
            "CurrencyCode", "AdDistribution", "DeviceType", "Language", "Network",
            "DeviceOS", "TopVsOther", "BidMatchType", "DeliveredMatchType",
            "AccountName", "CampaignName", "CampaignType", "AdGroupName",
            "Impressions", "Clicks", "Ctr", "Spend", "CostPerConversion",
            "DestinationUrl", "Assists", "ReturnOnAdSpend", "CostPerAssist",
            "CustomParameters", "FinalAppUrl", "AdDescription", "AdDescription2",
            "ViewThroughConversions", "ViewThroughConversionsQualified",
            "AllCostPerConversion", "AllReturnOnAdSpend", "Conversions",
            "ConversionRate", "ConversionsQualified", "AverageCpc",
            "AveragePosition", "AverageCpm", "AllConversions", "AllConversionRate",
            "AllRevenue", "AllRevenuePerConversion", "Revenue",
            "RevenuePerConversion", "RevenuePerAssist",
        ),
    ),
    ReportDefinition(
        name="keyword_performance_report_daily",
        request_type="KeywordPerformanceReportRequest",
        retention_months=36,
        columns=(
            "AccountId", "CampaignId", "AdGroupId", "KeywordId", "AdId",
            "TimePeriod", "CurrencyCode", "DeliveredMatchType", "AdDistribution",
            "DeviceType", "Language", "Network", "DeviceOS", "TopVsOther",
            "BidMatchType", "AccountName", "CampaignName", "AdGroupName",
            "Keyword", "KeywordStatus", "Impressions", "Clicks", "Ctr",
            "CurrentMaxCpc", "Spend", "CostPerConversion", "QualityScore",
            "ExpectedCtr", "AdRelevance", "LandingPageExperience", "QualityImpact",
            "Assists", "ReturnOnAdSpend", "CostPerAssist", "CustomParameters",
            "FinalAppUrl", "Mainline1Bid", "MainlineBid", "FirstPageBid",
            "FinalUrlSuffix", "ViewThroughConversions",
            "ViewThroughConversionsQualified", "AllCostPerConversion",
            "AllReturnOnAdSpend", "Conversions", "ConversionRate",
            "ConversionsQualified", "AverageCpc", "AveragePosition", "AverageCpm",
            "AllConversions", "AllConversionRate", "AllRevenue",
            "AllRevenuePerConversion", "Revenue", "RevenuePerConversion",
            "RevenuePerAssist", "HistoricalQualityScore", "HistoricalExpectedCtr",
            "HistoricalAdRelevance", "HistoricalLandingPageExperience",
        ),
    ),
    ReportDefinition(
        name="budget_summary_report",
        request_type="BudgetSummaryReportRequest",
        retention_months=24,
        time_column="Date",
        aggregation=None,
        # ATF's columns plus CurrencyCode, which Microsoft lists as required.
        columns=(
            "AccountName", "AccountNumber", "AccountId", "CampaignName",
            "CampaignId", "Date", "CurrencyCode", "MonthlyBudget", "DailySpend",
            "MonthToDateSpend",
        ),
    ),
]
