import asyncio
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError

from source_apple_app_store.utils import (
    str_to_date,
    date_to_str,
)

from .client import AppleAppStoreClient
from .models import (
    AnalyticsReportAccessType,
    AnalyticsReportGranularity,
    AnalyticsReportInstance,
    AppleAnalyticsRow,
    AppReview,
)


async def _get_or_create_analytics_report_request(
    log: Logger,
    client: AppleAppStoreClient,
    app_id: str,
    access_type: AnalyticsReportAccessType,
) -> str:
    report_request_id = None

    try:
        report_request_id = await client.get_or_create_report_request(
            app_id,
            access_type,
        )
    except HTTPError as e:
        if e.code == 409:
            log.warning(
                "Report request was created after first run. This is likely due to concurrent requests. "
                "Waiting for 60 seconds before trying to fetch the newly created report request.",
                e,
            )

            await asyncio.sleep(60)

            report_requests = await client.list_analytics_report_requests(
                app_id,
                access_type,
            )

            for report_req in report_requests:
                if report_req.attributes.stoppedDueToInactivity:
                    log.warning(
                        f"Found report request with id {report_req.id}, but the report is inactive due to inactivity."
                    )
                    continue
                else:
                    report_request_id = report_req.id
                    break

            if not report_request_id:
                raise RuntimeError(
                    f"Failed to discover or create report request for {app_id}"
                ) from e
    finally:
        if not report_request_id:
            # This should never happen, but if it does, we'll raise a RuntimeError.
            # The try/except block above would raise an exception before this point.
            raise RuntimeError("Report request ID is missing")

    return report_request_id


async def _select_valid_report_instances(
    log: Logger,
    client: AppleAppStoreClient,
    model: type[AppleAnalyticsRow],
    report_request_id: str,
    granularity: AnalyticsReportGranularity,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[AnalyticsReportInstance]:
    analytics_reports = await client.list_analytics_reports(report_request_id)

    target_report = None
    for report in analytics_reports:
        report_details = await client.get_analytics_report_details(report.id)
        if report_details.attributes.name == model.report_name:
            target_report = report
            break

    if not target_report:
        return []

    report_instances = await client.list_analytic_report_instances(
        target_report.id,
        granularity,
    )

    valid_instances = []
    for instance in report_instances:
        if instance.attributes.granularity != granularity:
            continue

        processing_date = instance.attributes.processingDate
        if start_date and processing_date < start_date:
            continue
        if end_date and processing_date > end_date:
            continue

        valid_instances.append(instance)

    return valid_instances


async def _stream_analytics_report_data(
    log: Logger,
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    instance: AnalyticsReportInstance,
) -> AsyncGenerator[AppleAnalyticsRow, None]:
    """
    Streams the appropriate analytics report TSV rows for a given report request instance.
    """

    segments = await client.list_analytics_report_segments(instance.id)
    log.debug(
        f"Found {len(segments)} analytics report segment(s) for app {app_id}",
        {
            "report_name": model.report_name,
            "instance_id": instance.id,
        },
    )

    for segment in segments:
        log.debug(
            f"Processing analytics report segment {segment.id} for app {app_id}",
            {
                "report_name": model.report_name,
                "instance_id": instance.id,
                "segment_id": segment.id,
                "segment_filename": segment.filename,
            },
        )

        async for row in client.stream_tsv_data(
            segment.filename, segment.attributes.url, model
        ):
            yield row


async def _ongoing_analytics_report_request_exists(
    client: AppleAppStoreClient,
    app_id: str,
) -> bool:
    report_requests = await client.list_analytics_report_requests(
        app_id,
        AnalyticsReportAccessType.ONGOING,
    )

    for report_request in report_requests:
        if report_request.attributes.accessType == AnalyticsReportAccessType.ONGOING:
            if not report_request.attributes.stoppedDueToInactivity:
                return True

    return False


async def fetch_incremental_analytics(
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AppleAnalyticsRow | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_processed_date = log_cursor

    report_request_id = await _get_or_create_analytics_report_request(
        log,
        client,
        app_id,
        AnalyticsReportAccessType.ONGOING,
    )

    now = datetime.now(tz=UTC)
    current_lag = now.date() - last_processed_date
    start_date = last_processed_date + timedelta(days=1)

    report_instances = await _select_valid_report_instances(
        log,
        client,
        model,
        report_request_id,
        AnalyticsReportGranularity.DAILY,
        start_date,
    )

    report_instance = min(
        report_instances,
        key=lambda x: x.attributes.processingDate,
        default=None,
    )

    if not report_instance:
        log.warning(
            f"The {model.report_name} report is not yet available after waiting for {current_lag.days} days. "
            f"Apple's documentation states this report has a completeness lag of {model.completeness_lag.days} days. "
            "If the issue persists, the ONGOING report request may need to be re-created.",
            {
                "report_name": model.report_name,
                "completeness_lag": model.completeness_lag,
                "current_lag": current_lag,
            },
        )

        return

    async for row in _stream_analytics_report_data(
        log,
        client,
        app_id,
        model,
        report_instance,
    ):
        yield row

    new_cursor = datetime.combine(
        report_instance.attributes.processingDate, datetime.min.time()
    ).replace(tzinfo=UTC)

    yield new_cursor


async def fetch_backfill_analytics(
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[AppleAnalyticsRow | PageCursor, None]:
    """
    Fetches historical analytics data for backfill operations using ONE_TIME_SNAPSHOT report requests.

    This function processes analytics data chronologically by waiting for report instances with specific
    processed dates. It only operates when an ONGOING report request exists, ensuring proper data
    continuity between backfill and incremental processing.

    Workflow:
        1. Parse page cursor to determine target report instance processed date and last check time
        2. Verify an ONGOING analytics report request exists for the app
        3. Get or create a ONE_TIME_SNAPSHOT analytics report request
        4. Look for report instance with the target processed date > page (last_processed_date)

    Notes:
        - Report instances are expected to have processed dates in chronological order. This has been observed in testing.
        - Only processes data if an ONGOING report request exists (prerequisite for proper setup); Does not guarantee data completeness since the ONE_TIME_SNAPSHOT
        could have still been created before the ONGOING report request was created. There is no way to know the creation time of these requests according to documentation.
    """

    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    if not page:
        last_processed_date = date.fromisoformat("1970-01-01")
        start_date = last_processed_date
    else:
        last_processed_date = str_to_date(page)
        start_date = last_processed_date + timedelta(days=1)

    ongoing_report_exists = await _ongoing_analytics_report_request_exists(
        client,
        app_id,
    )

    if not ongoing_report_exists:
        log.warning(
            f"Delaying backfill for {model.report_name} since an ONGOING report request does not exist for {app_id} yet. "
            "Backfill requires an existing ONGOING report request to ensure proper data continuity.",
            {
                "app_id": app_id,
                "report_name": model.report_name,
            },
        )
        await asyncio.sleep(30)
        yield date_to_str(last_processed_date)
        return

    report_request_id = await _get_or_create_analytics_report_request(
        log,
        client,
        app_id,
        AnalyticsReportAccessType.ONE_TIME_SNAPSHOT,
    )

    report_instances = await _select_valid_report_instances(
        log,
        client,
        model,
        report_request_id,
        AnalyticsReportGranularity.DAILY,
        start_date,
    )

    report_instance = min(
        report_instances,
        key=lambda x: x.attributes.processingDate,
        default=None,
    )

    if not report_instance:
        log.warning(
            f"Report instance not found for app {app_id} and report request {report_request_id}",
            {
                "app_id": app_id,
                "report_name": model.report_name,
                "report_request_id": report_request_id,
            },
        )

        yield date_to_str(last_processed_date)
    else:
        async for row in _stream_analytics_report_data(
            log,
            client,
            app_id,
            model,
            report_instance,
        ):
            if row.record_date <= cutoff.date():
                yield row

        if report_instance.attributes.processingDate <= cutoff.date():
            yield date_to_str(report_instance.attributes.processingDate)


async def fetch_incremental_reviews(
    client: AppleAppStoreClient,
    app_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AppReview | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    cursor = None
    max_created_date = log_cursor
    found_new_reviews = True

    while found_new_reviews:
        found_new_reviews = False

        reviews, cursor = await client.list_app_reviews(app_id, cursor)

        for review in reviews:
            if review.attributes.createdDate >= log_cursor:
                review.app_id = app_id
                yield review
                max_created_date = max(max_created_date, review.attributes.createdDate)
                found_new_reviews = True
            else:
                # Reviews are ordered by newest first, so we can stop iterating
                # once we find a review that is older than the log cursor
                found_new_reviews = False
                break

        if not cursor:
            break

    if max_created_date > log_cursor:
        yield max_created_date
    else:
        yield log_cursor + timedelta(milliseconds=1)


async def fetch_backfill_reviews(
    client: AppleAppStoreClient,
    app_id: str,
    log: Logger,
    page_cursor: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[AppReview | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert page_cursor is None or isinstance(page_cursor, str)

    reviews, cursor = await client.list_app_reviews(app_id, page_cursor)

    for review in reviews:
        if review.attributes.createdDate < cutoff:
            review.app_id = app_id
            yield review

    if not cursor:
        return

    yield cursor
