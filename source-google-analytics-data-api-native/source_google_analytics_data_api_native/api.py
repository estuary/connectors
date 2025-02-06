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

    # TODO(bair): add support for dimension filters & metric filters.
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

        processor = IncrementalJsonProcessor(
            await http.request_stream(log, url, method="POST", json=body),
            f"rows.item",
            Row,
            RunReportResponse,
        )

        async for row in processor:
            record = _transform_into_record(row, dimension_headers, metric_headers, property_id, date)
            yield report_doc_model.model_validate(record)

        remainder = processor.get_remainder()

        if remainder.rows is None or remainder.rowCount is None:
            raise RuntimeError(f"Missing rows or rowCount in API response. Verify property's timezone is a valid IANA timezone.")

        offset += MAX_REPORT_RESULTS_LIMIT
        if offset >= remainder.rowCount:
            return


async def fetch_report(
    http: HTTPSession,
    property_id: str,
    timezone: ZoneInfo,
    report_doc_model: type[ReportDocument],
    report: Report,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ReportDocument | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = log_cursor
    end = datetime.now(tz=timezone)

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
