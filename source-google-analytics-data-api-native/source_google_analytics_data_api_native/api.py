from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator, Generator
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    DimensionHeader,
    MetricHeader,
    Report,
    ReportDocument,
    Row,
    RunReportResponse,
    MetadataResponse,
)
from .utils import (
    dt_to_date_str,
    dt_to_str,
    str_to_dt,
)


API = "https://analyticsdata.googleapis.com/v1beta/properties"
MAX_REPORT_RESULTS_LIMIT = 250_000


def _transform_into_record(
    row: Row,
    dimension_headers: list[DimensionHeader],
    metric_headers: list[MetricHeader],
    property_id: str,
    report_date: datetime
) -> dict[str, str | float | int]:
    record: dict[str, str | float | int] = {}

    for index, dimension_value in enumerate(row.dimensionValues):
        dimension_name = dimension_headers[index].name
        record[dimension_name] = dimension_value.value

    for index, metric_value in enumerate(row.metricValues):
        metric_name = metric_headers[index].name

        record[metric_name] = metric_value.value

    # Add report identifying fields.
    record["property_id"] = property_id
    record["report_date"] = dt_to_date_str(report_date)

    return record


def _build_report_body(
    dt: datetime,
    report: Report,
    offset: int = 0,
    limit: int = MAX_REPORT_RESULTS_LIMIT,
) -> dict[str, Any]:
    body = {
        "dateRanges": [
            {
                "startDate": dt_to_date_str(dt),
                "endDate": dt_to_date_str(dt),
            }
        ],
        "dimensions": [{"name": d} for d in report.dimensions],
        "metrics": [{"name": m} for m in report.metrics],
        "limit": limit,
        "offset": offset,
    }

    if report.dimensionFilter:
        body["dimensionFilter"] = report.dimensionFilter

    if report.metricFilter:
        body["metricFilter"] = report.metricFilter

    if report.metricAggregations:
        body["metricAggregations"] = report.metricAggregations

    return body


def _are_same_day(dt1: datetime, dt2: datetime) -> bool:
    return dt1.date() == dt2.date()


async def fetch_property_timezone(
    http: HTTPSession,
    property_id: str,
    log: Logger,
) -> str:
    url = f"{API}/{property_id}:runReport"
    basic_report = Report.model_validate({"name": "test_report", "dimensions": ["date"], "metrics": ["active1DayUsers"]})
    body = _build_report_body(datetime.now(tz=UTC), basic_report)
    response = RunReportResponse.model_validate_json(
        await http.request(log, url, method="POST", json=body)
    )

    return response.metadata.timeZone


async def _fetch_headers(
    http: HTTPSession,
    url: str,
    date: datetime,
    report: Report,
    log: Logger,
) -> tuple[list[DimensionHeader], list[MetricHeader]]:
    body = _build_report_body(date, report, offset=0, limit=1)
    response = RunReportResponse.model_validate_json(
        await http.request(log, url, method="POST", json=body)
    )

    return (response.dimensionHeaders, response.metricHeaders)


async def _paginate_through_report_results(
    http: HTTPSession,
    property_id: str,
    report_doc_model: type[ReportDocument],
    date: datetime,
    report: Report,
    log: Logger,
) -> AsyncGenerator[ReportDocument, None]:
    url = f"{API}/{property_id}:runReport"
    offset = 0
    dimension_headers, metric_headers = await _fetch_headers(http, url, date, report, log)
    while True:
        body = _build_report_body(date, report, offset)

        _, response_body = await http.request_stream(log, url, method="POST", json=body)

        processor = IncrementalJsonProcessor(
            response_body(),
            f"rows.item",
            Row,
            RunReportResponse,
        )

        async for row in processor:
            record = _transform_into_record(row, dimension_headers, metric_headers, property_id, date)
            yield report_doc_model.model_validate(record)

        remainder = processor.get_remainder()

        offset += MAX_REPORT_RESULTS_LIMIT

        # Pagination is complete if there are no results or the next offset is beyond the total number of rows.
        if remainder.rowCount is None or offset >= remainder.rowCount:
            # If there are metric aggregates for this report, emit them after processing all non-aggregate rows.
            for aggregate in [remainder.totals, remainder.minimums, remainder.maximums]:
                if aggregate is not None:
                    record = _transform_into_record(aggregate[0], dimension_headers, metric_headers, property_id, date)
                    yield report_doc_model.model_validate(record)

            return


async def fetch_report(
    http: HTTPSession,
    property_id: str,
    timezone: ZoneInfo,
    report_doc_model: type[ReportDocument],
    report: Report,
    start_date: datetime,
    lookback_window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ReportDocument | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    lookback_start = max(log_cursor - timedelta(days=lookback_window_size), start_date)
    start = log_cursor
    end = datetime.now(tz=timezone)

    # Recapture reports from the past lookback_window_size days. The underlying data for reports in this window could have changed
    # for various reasons, so we recapture reports within the lookback window.
    # https://support.google.com/analytics/answer/11198161
    while not _are_same_day(lookback_start, start):
        async for record in _paginate_through_report_results(http, property_id, report_doc_model, lookback_start, report, log):
            yield record

        lookback_start = min(lookback_start + timedelta(days=1), start)

    # Catch up to the present day.
    while not _are_same_day(start, end):
        async for record in _paginate_through_report_results(http, property_id, report_doc_model, start, report, log):
            yield record

        start = min(start + timedelta(days=1), end)
        if start < end:
            yield start

    # Fetch the current day's results.
    async for record in _paginate_through_report_results(http, property_id, report_doc_model, start, report, log):
        yield record

    yield end


async def backfill_report(
    http: HTTPSession,
    property_id: str,
    report_doc_model: type[ReportDocument],
    report: Report,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ReportDocument | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)
    start = str_to_dt(page)

    # The incremental task will capture records for the cutoff day.
    if start >= cutoff or _are_same_day(start, cutoff):
        return

    async for record in _paginate_through_report_results(http, property_id, report_doc_model, start, report, log):
        yield record

    next_start = min(start + timedelta(days=1), cutoff)
    yield dt_to_str(next_start)


async def fetch_metadata(
    http: HTTPSession,
    property_id: str,
    log: Logger,
) -> tuple[list[str], list[str]]:
    url = f"{API}/{property_id}/metadata"

    response = MetadataResponse.model_validate_json(
        await http.request(log, url)
    )

    dimensions = [dimension.apiName for dimension in response.dimensions]
    metrics = [metric.apiName for metric in response.metrics]

    return (dimensions, metrics)
