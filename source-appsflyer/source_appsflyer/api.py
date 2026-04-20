import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from urllib.parse import urljoin

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor

from .models import (
    AdRevenueOrganicReport,
    AdRevenueReport,
    AdRevenueRetargetingReport,
    AggDailyReport,
    AggGeoDailyReport,
    AggGeoReport,
    AggPartnersDailyReport,
    AggPartnersReport,
    AggregateReportDocument,
    PullApiDocument,
    BlockedClicksReport,
    BlockedInAppEventsPostAttributionReport,
    BlockedInAppEventsReport,
    BlockedInstallPostbacksReport,
    BlockedInstallsPostAttributionReport,
    BlockedInstallsReport,
    EventTypesResponse,
    InAppEventPostbacksReport,
    InAppEventsReport,
    InstallPostbacksReport,
    InstallsReport,
    OrganicInAppEventsReport,
    OrganicInstallsReport,
    OrganicReinstallsReport,
    OrganicUninstallsReport,
    RawReportDocument,
    ReinstallsReport,
    RetargetingConversionPostbacksReport,
    RetargetingConversionsReport,
    RetargetingInAppEventPostbacksReport,
    RetargetingInAppEventsReport,
    UninstallsReport,
)

ATTRIBUTING_ENTITIES = ["appsflyer", "skadnetwork"]

MAX_ROWS = 1_000_000
RATE_LIMIT_RETRIES = 3
RATE_LIMIT_SLEEP_SECONDS = 60

RAW_REPORT_TYPES: list[type[RawReportDocument]] = [
    # TODO: We're limiting scope of the connector to focus on only what's required
    # in the initial version. Raw reports are not required right now, so we're 
    # commentting them out until we decide to add support for them later.
    #
    # # Non-organic
    # InstallsReport,
    # InAppEventsReport,
    # UninstallsReport,
    # ReinstallsReport,
    # # Organic
    # OrganicInstallsReport,
    # OrganicInAppEventsReport,
    # OrganicUninstallsReport,
    # OrganicReinstallsReport,
    # # Retargeting
    # RetargetingConversionsReport,
    # RetargetingInAppEventsReport,
    # # Ad Revenue
    # AdRevenueReport,
    # AdRevenueOrganicReport,
    # AdRevenueRetargetingReport,
    # # Protect360
    # BlockedInstallsReport,
    # BlockedInstallsPostAttributionReport,
    # BlockedInAppEventsReport,
    # BlockedInAppEventsPostAttributionReport,
    # BlockedClicksReport,
    # BlockedInstallPostbacksReport,
    # # Postbacks
    # InstallPostbacksReport,
    # InAppEventPostbacksReport,
    # RetargetingInAppEventPostbacksReport,
    # RetargetingConversionPostbacksReport,
]

AGG_REPORT_TYPES: list[type[AggregateReportDocument]] = [
    # TODO: Only the AggGeoDailyReport is required for the initial version of the connector.
    # So we're commenting all others out for now until we add support for them later.
    # 
    # AggPartnersReport,
    # AggPartnersDailyReport,
    # AggDailyReport,
    # AggGeoReport,
    AggGeoDailyReport,
]


async def fetch_event_types(log: Logger, http: HTTPSession) -> set[str]:
    event_types: set[str] = set()

    for entity in ATTRIBUTING_ENTITIES:
        url = urljoin("https://hq1.appsflyer.com/api/pushapi/v1.0/event-types/", entity)
        response = EventTypesResponse.model_validate_json(await http.request(log, url))
        event_types |= set(response.event_types)

    return event_types


async def _fetch_window(
    doc_type: type[PullApiDocument],
    app_id: str,
    min_date: datetime,
    max_date: datetime,
    http: HTTPSession,
    log: Logger,
    max_rows: int = MAX_ROWS,
) -> AsyncGenerator[PullApiDocument, None]:
    url = doc_type.get_api_url(app_id)
    params = {
        "from": min_date.strftime(doc_type.date_format),
        "to": max_date.strftime(doc_type.date_format),
        # Aggregate report endpoints do not explicitly support this parameter
        # Worst case scenario, it gets ignored
        "maximum_rows": max_rows,
    }

    for attempt in range(1, RATE_LIMIT_RETRIES + 1):
        try:
            _, body = await http.request_stream(log, url, params=params)

            async for row in IncrementalCSVProcessor(
                body(),
                doc_type,
                validation_context={"app_id": app_id},
            ):
                yield row

            return

        except HTTPError as e:
            if e.code != 403 or "Limit reached" not in e.message:
                raise

            if attempt < RATE_LIMIT_RETRIES:
                log.warning(
                    "Rate limit hit, retrying after sleep",
                    {
                        "endpoint": doc_type.resource_name,
                        "attempt": attempt,
                        "sleep_seconds": RATE_LIMIT_SLEEP_SECONDS,
                    },
                )
                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
            else:
                raise


async def is_endpoint_supported(
    doc_type: type[PullApiDocument],
    app_id: str,
    log: Logger,
    http: HTTPSession,
) -> bool:
    if not doc_type.requires_permission_check:
        return True

    now = datetime.now(tz=UTC)

    try:
        _ = await anext(
            _fetch_window(
                doc_type,
                app_id,
                now - timedelta(days=1),
                now,
                http,
                log,
                max_rows=100,
            ),
            None,
        )
        return True

    except HTTPError as e:
        if "isn't supported for this report type" in e.message:
            log.warning(
                "Endpoint skipped: platform not supported",
                {"endpoint": doc_type.resource_name},
            )
            return False

        if "doesn't include raw data reports" in e.message:
            log.warning(
                (
                    "Endpoint skipped: current subscription package "
                    "does not include reports for data type"
                ),
                {"endpoint": doc_type.resource_name},
            )
            return False

        if (
            "Protect360 detection raw data reports are available "
            "for the earlier of 90 days or the date you subscribed to Protect360"
        ) in e.message:
            log.warning(
                "Endpoint skipped: Protect360 subscription is not available",
                {"endpoint": doc_type.resource_name},
            )
            return False

        if e.code == 404:
            log.warning(
                "Endpoint skipped: not supported",
                {"endpoint": doc_type.resource_name},
            )
            return False

        raise


async def _fetch_adaptive_window(
    doc_type: type[PullApiDocument],
    app_id: str,
    start_time: datetime,
    end_time: datetime,
    window_size: timedelta,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[PullApiDocument | datetime, None]:
    """Fetch rows, automatically halving the window when MAX_ROWS is hit.

    Yields validated documents, then the actual window_end datetime as the final item.
    """
    window_size = min(window_size, timedelta(days=1))
    while True:
        window_end = min(start_time + window_size, end_time)

        row_count = 0
        async for row in _fetch_window(
            doc_type, app_id, start_time, window_end, http, log
        ):
            row_count += 1
            yield row

        if row_count >= MAX_ROWS:
            window_size = window_size / 2
            log.warning(
                "Fetch returned maximum rows, halving window to avoid truncation",
                {
                    "endpoint": doc_type.resource_name,
                    "window_start": start_time,
                    "new_window": window_size,
                },
            )
            continue

        break

    yield window_end


async def backfill_report(
    doc_type: type[PullApiDocument],
    app_id: str,
    start_time: datetime,
    window_size: timedelta,
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[PullApiDocument | PageCursor, None]:
    assert page is None or isinstance(page, str)
    assert isinstance(cutoff, datetime)

    if page is None:
        page = start_time.isoformat()

    page_ts = datetime.fromisoformat(page)

    if page_ts >= cutoff:
        return

    async for item in _fetch_adaptive_window(
        doc_type, app_id, page_ts, cutoff, window_size, http, log
    ):
        if isinstance(item, datetime):
            page_ts = item
        else:
            yield item

    if page_ts < cutoff:
        yield page_ts.isoformat(timespec="seconds")


async def fetch_report(
    doc_type: type[PullApiDocument],
    app_id: str,
    window_size: timedelta,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[PullApiDocument | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)
    window_start = log_cursor

    while window_start < now:
        async for item in _fetch_adaptive_window(
            doc_type, app_id, window_start, now, window_size, http, log
        ):
            if isinstance(item, datetime):
                window_start = item
                break

            yield item

        if window_start < now:
            yield window_start

    yield now
