import asyncio
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Optional

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError

from source_apple_app_store.utils import (
    dt_to_str,
    str_to_date,
    str_to_dt,
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
                if report_req.attributes.stoppingDueToInactivity:
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


async def _select_valid_report_instance(
    log: Logger,
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    report_request_id: str,
    granularity: AnalyticsReportGranularity,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[AnalyticsReportInstance]:
    analytics_reports = await client.list_analytics_reports(report_request_id)
    log.debug(
        f"Found {len(analytics_reports)} analytics report(s) for app {app_id}",
        {
            "report_name": model.report_name,
            "report_request_id": report_request_id,
        },
    )

    for report in analytics_reports:
        report_details = await client.get_analytics_report_details(report.id)
        if report_details.attributes.name != model.report_name:
            log.debug(
                f"Skipping report {report_details.attributes.name}",
                {
                    "report_request_id": report_request_id,
                    "report_id": report.id,
                    "report_name": model.report_name,
                    "response_report_name": report_details.attributes.name,
                },
            )
            continue

        log.debug(f"Found report {report_details.attributes.name}")
        report_instances = await client.list_analytic_report_instances(
            report.id,
            granularity,
        )
        log.debug(f"Found {len(report_instances)} instances")

        for instance in report_instances:
            if start_date:
                if instance.attributes.processingDate < start_date:
                    log.debug(
                        f"Skipping analytics report instance {instance.id} for app {app_id} because the processing date is before {start_date}",
                        {
                            "report_name": model.report_name,
                            "report_request_id": report_request_id,
                            "report_id": report.id,
                            "instance_id": instance.id,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                    continue
            elif end_date:
                if instance.attributes.processingDate > end_date:
                    log.debug(
                        f"Skipping analytics report instance {instance.id} for app {app_id} because the processing date after {end_date}",
                        {
                            "report_name": model.report_name,
                            "report_request_id": report_request_id,
                            "report_id": report.id,
                            "instance_id": instance.id,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                    continue
            elif instance.attributes.granularity != granularity:
                log.debug(
                    f"Skipping analytics report instance {instance.id} for app {app_id} because the granularity is not {granularity}",
                    {
                        "report_name": model.report_name,
                        "report_request_id": report_request_id,
                        "report_id": report.id,
                        "instance_id": instance.id,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                continue

            return instance

    return None


async def _stream_analytics_report_data(
    log: Logger,
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    instance: AnalyticsReportInstance,
) -> AsyncGenerator[AppleAnalyticsRow, None]:
    """
    Streams the appropriate analytics report TSV rows for a given report request for a target date.

    If there is a report instance with a processing date equal to the target date, we'll stream the data.
    Apple documents
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
            },
        )

        async for row in client.stream_tsv_data(app_id, segment.attributes.url, model):
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
            if not report_request.attributes.stoppingDueToInactivity:
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

    # The code below this return is a best-effort implementation to fetch the analytics report data
    # based on Apple App Store Connect API documentation. It boils down to the following steps:
    # 1. Get or create the ONGOING analytics report request for the app.
    # 2. Get the report request ID for the ONGOING report request.
    # 3. Select a valid report instance for the app and model based on the log_cursor.
    # 4. Stream the analytics report data for the report instance.
    #
    # Once we have valid credentials, development can continue and we can iterate on the code.
    return

    report_request_id = await _get_or_create_analytics_report_request(
        log,
        client,
        app_id,
        AnalyticsReportAccessType.ONGOING,
    )

    now = datetime.now(tz=UTC)
    current_lag = now - log_cursor

    report_instance = await _select_valid_report_instance(
        log,
        client,
        app_id,
        model,
        report_request_id,
        AnalyticsReportGranularity.DAILY,
        log_cursor.date(),
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
        if current_lag > model.completeness_lag:
            new_cursor = (log_cursor + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            log.warning(
                "Yielding a new cursor to continue incremental processing in case the report does not have data for the current date.",
                {
                    "report_name": model.report_name,
                    "previous_log_cursor": log_cursor,
                    "new_log_cursor": new_cursor,
                    "completeness_lag": model.completeness_lag,
                    "current_lag": current_lag,
                },
            )
            yield new_cursor

        return

    processed_report_for_date = False
    doc_count = 0

    async for row in _stream_analytics_report_data(
        log,
        client,
        app_id,
        model,
        report_instance,
    ):
        processed_report_for_date = True
        if row.record_date > log_cursor:
            doc_count += 1
            yield row

    if processed_report_for_date:
        new_cursor = (log_cursor + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        log.info(
            "Incremental processing completed",
            {
                "report_name": model.report_name,
                "previous_log_cursor": log_cursor,
                "new_log_cursor": new_cursor,
                "doc_count": doc_count,
            },
        )
        yield new_cursor
    else:
        # This may indicate that the report did not have any data segments even though there
        # was a report instance with a processed date matching our log_cursor. Return without
        # yielding a new cursor in case the report is not yet available, yet shown in the API.
        log.warning(
            "No data segments found for report",
            {
                "report_name": model.report_name,
                "log_cursor": log_cursor,
                "report_instance_id": report_instance.id,
            },
        )
        return


async def fetch_backfill_analytics(
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    log: Logger,
    page: Optional[PageCursor],
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
        4. Look for report instance with the target processed date (within cutoff + completeness_lag range)
        5. If found: stream data and advance to next processed date
        6. If not found: check if we've waited long enough (> completeness_lag) to give up or continue waiting

    Page Cursor Format:
        "YYYY-MM-DD:ISO_DATETIME" where:
        - YYYY-MM-DD: Target processed date for the report instance we're waiting for
        - ISO_DATETIME: When we last checked for this report instance (to calculate wait time since reports can take time to generate)

    Completion Logic:
        - Backfill completes when (next_processed_date - completeness_lag) > cutoff - i.e., the reports oldest data is past the cutoff date
        - This ensures the next report instance would only contain data already handled by incremental
        - If no report instance found after waiting > completeness_lag, backfill terminates

    Wait Logic:
        - If target report instance not found and wait time > completeness_lag: terminate backfill
        - If target report instance not found and wait time <= completeness_lag: yield cursor to wait longer
        
    Notes:
        - Report instances are expected to have processed dates in chronological order; However, it is uknown if there
        are gaps in the processed dates. If that is the case, the wait and completion logic will change to handle this.
        - Only processes data if an ONGOING report request exists (prerequisite for proper setup); Does not guarantee data completeness since the ONE_TIME_SNAPSHOT
        could have still been created before the ONGOING report request was created. There is no way to know the creation time of these requests according to documentation.
        - Report instances contain data from processed_date back to (processed_date - completeness_lag)
        - Completeness lag varies by report type (48 hours to 5+ days per Apple documentation)
        - Initial run starts from 1970-01-01 to find the oldest available report instance
    """

    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    # The code below this return is a best-effort implementation to fetch the analytics report data
    # based on Apple App Store Connect API documentation. It boils down to the following steps:
    # 1. Get or create the ONE_TIME_SNAPSHOT analytics report request for the app if there is an ONGOING report request.
    # 2. Get the report request ID for the ONE_TIME_SNAPSHOT report request.
    # 3. Select a valid report instance for the app and model based on the page cursor.
    # 4. Stream the analytics report data for the report instance.
    # 5. Stop when the next processed date is greater than the cutoff date or there are no more report instances to process.
    #
    # Once we have valid credentials, development can continue and we can iterate on the code.
    return

    if not page:
        next_processed_date = date.fromisoformat("1970-01-01")
        last_checked_datetime = None
    else:
        parts = page.split(":")
        assert len(parts) == 2, f"Invalid page format: {page}"

        next_processed_date = str_to_date(parts[0])
        last_checked_datetime = str_to_dt(parts[1])

    ongoing_report_exists = await _ongoing_analytics_report_request_exists(
        client,
        app_id,
    )

    if ongoing_report_exists:
        report_request_id = await _get_or_create_analytics_report_request(
            log,
            client,
            app_id,
            AnalyticsReportAccessType.ONE_TIME_SNAPSHOT,
        )

        log.debug(
            f"Found or created report request {report_request_id} for app {app_id}"
        )

        report_instance_check_datetime = datetime.now(tz=UTC)
        end_date = cutoff + model.completeness_lag
        current_lag = (
            (report_instance_check_datetime - last_checked_datetime)
            if last_checked_datetime
            else None
        )

        report_instance = await _select_valid_report_instance(
            log,
            client,
            app_id,
            model,
            report_request_id,
            AnalyticsReportGranularity.DAILY,
            next_processed_date,
            end_date,
        )

        if not report_instance and current_lag and current_lag > model.completeness_lag:
            # Only complete backfill if the current lag is greater than the completeness lag
            # otherwise, we may still be waiting for the report to be generated by Apple
            log.warning(
                f"Report instance not found after {model.completeness_lag} days. Report request may be outdated.",
                {
                    "app_id": app_id,
                    "report_name": model.report_name,
                    "report_request_id": report_request_id,
                },
            )
            return

        if not report_instance:
            log.warning(
                f"Report instance not found for app {app_id} and report request {report_request_id}",
                {
                    "app_id": app_id,
                    "report_name": model.report_name,
                    "report_request_id": report_request_id,
                },
            )

            cursor = (
                f"{next_processed_date}:{dt_to_str(report_instance_check_datetime)}"
            )
            yield cursor
        else:
            async for row in _stream_analytics_report_data(
                log,
                client,
                app_id,
                model,
                report_instance,
            ):
                if row.record_date <= cutoff:
                    yield row

            new_processed_date = next_processed_date + timedelta(days=1)
            new_processed_start_date = new_processed_date - model.completeness_lag

            if new_processed_start_date > cutoff:
                # Data completeness lag tells us that the next report date we would try to process would only contain data that is for new data
                # that the incremental task is already responsible for processing.
                log.debug(
                    f"Completed backfilling {model.report_name} for {app_id}. The next date to process will only contain data from {new_processed_start_date} to {new_processed_date}",
                    {
                        "last_processed_date": next_processed_date,
                        "last_checked_datetime": last_checked_datetime,
                        "next_processed_start_date": new_processed_start_date,
                        "next_processed_date": new_processed_date,
                        "end_date": end_date,
                    },
                )
                return

            # Emit a checkpoint for the backfill to process the next processed date with an updated last report instance check datetime
            cursor = f"{new_processed_date}:{dt_to_str(report_instance_check_datetime)}"
            yield cursor


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
    page_cursor: Optional[PageCursor],
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
