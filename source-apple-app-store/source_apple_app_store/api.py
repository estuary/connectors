import asyncio
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError

from .client import AppleAppStoreClient
from .models import (
    AnalyticsReportAccessType,
    AnalyticsReportGranularity,
    AnalyticsReportInstance,
    AnalyticsReportRequest,
    AppleAnalyticsRow,
    AppReview,
)


async def _list_active_report_requests(
    client: AppleAppStoreClient,
    app_id: str,
    access_type: AnalyticsReportAccessType,
) -> list[AnalyticsReportRequest]:
    report_requests = await client.list_analytics_report_requests(
        app_id,
        access_type,
    )

    return [
        report_request
        for report_request in report_requests
        if report_request.attributes.accessType == access_type
        and not report_request.attributes.stoppedDueToInactivity
    ]


async def _try_create_report_request(
    log: Logger,
    client: AppleAppStoreClient,
    app_id: str,
    access_type: AnalyticsReportAccessType,
) -> bool:
    """
    Asks Apple to start producing reports of `access_type` for the app.
    Returns False when Apple refuses with a 409.
    """

    try:
        report_request = await client.create_analytics_report_request(
            app_id,
            access_type,
        )
    except HTTPError as e:
        # Apple returns 409 when it won't create another request of this type,
        # for example because the app already has one or, for snapshots,
        # because one was made in the past month.
        if e.code != 409:
            raise
        return False

    log.info(
        f"Created {access_type} report request {report_request.id} for app {app_id}",
        {
            "app_id": app_id,
            "report_request_id": report_request.id,
        },
    )
    return True


async def _select_valid_report_instances(
    log: Logger,
    client: AppleAppStoreClient,
    model: type[AppleAnalyticsRow],
    report_request_id: str,
    granularity: AnalyticsReportGranularity,
    start_date: date | None = None,
) -> list[AnalyticsReportInstance]:
    analytics_reports = await client.list_analytics_reports(report_request_id)

    target_report = None
    for report in analytics_reports:
        if report.attributes.name == model.report_name:
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


async def fetch_incremental_analytics(
    client: AppleAppStoreClient,
    app_id: str,
    model: type[AppleAnalyticsRow],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AppleAnalyticsRow | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_processed_date = log_cursor.date()

    report_requests = await _list_active_report_requests(
        client,
        app_id,
        AnalyticsReportAccessType.ONGOING,
    )

    if not report_requests:
        created = await _try_create_report_request(
            log,
            client,
            app_id,
            AnalyticsReportAccessType.ONGOING,
        )
        if not created:
            log.warning(
                f"Apple refused to create an ONGOING report request for app {app_id}, and none is listed as active. "
                "This is expected right after another binding creates one. "
                "If it persists, the app's existing ONGOING requests may have been stopped.",
                {
                    "app_id": app_id,
                    "report_name": model.report_name,
                },
            )

        # Apple publishes a new request's first instance a day or more after
        # it's created, so there is nothing to read yet.
        return

    now = datetime.now(tz=UTC)
    current_lag = now.date() - last_processed_date
    start_date = last_processed_date + timedelta(days=1)

    # An app can have several ONGOING requests, and only some of them may
    # have instances, so collect instances from all of them.
    report_instances: list[AnalyticsReportInstance] = []
    for report_request in report_requests:
        report_instances.extend(
            await _select_valid_report_instances(
                log,
                client,
                model,
                report_request.id,
                AnalyticsReportGranularity.DAILY,
                start_date,
            )
        )

    # min keeps the first of equal dates, so a tie goes to the request Apple
    # lists first.
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
                "report_request_ids": [
                    report_request.id for report_request in report_requests
                ],
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

    Emits the rows dated on or before the cutoff from one snapshot instance processed on or after
    the cutoff day, then finishes. It only operates when an ONGOING report request exists, ensuring
    proper data continuity between backfill and incremental processing.

    Notes:
        - A snapshot holds data up to the day Apple made it. An older one would leave a gap between
        its last day and the cutoff that the incremental never reads, so it isn't used.
        - An app can have several snapshot requests, and any of them may hold the instance.
        - Apple allows one new snapshot request per app per month. Until a snapshot qualifies, the
        backfill requests one and checks again every few minutes.
        - A backfill that already read an older snapshot emits its dates again from the newer one.
        The new rows have different keys because keys include the filename, so the destination
        keeps both. Apple says consumers should use the newest file for each date.
    """

    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    # The page cursor no longer tracks progress, since one snapshot covers the
    # whole backfill. Yielding it unchanged keeps the backfill running while
    # it waits.
    unchanged_page = page or "1970-01-01"

    ongoing_requests = await _list_active_report_requests(
        client,
        app_id,
        AnalyticsReportAccessType.ONGOING,
    )

    if not ongoing_requests:
        log.warning(
            f"Delaying backfill for {model.report_name} since an ONGOING report request does not exist for {app_id} yet. "
            "Backfill requires an existing ONGOING report request to ensure proper data continuity.",
            {
                "app_id": app_id,
                "report_name": model.report_name,
            },
        )
        await asyncio.sleep(30)
        yield unchanged_page
        return

    snapshot_requests = await _list_active_report_requests(
        client,
        app_id,
        AnalyticsReportAccessType.ONE_TIME_SNAPSHOT,
    )

    report_instances: list[AnalyticsReportInstance] = []
    for report_request in snapshot_requests:
        report_instances.extend(
            await _select_valid_report_instances(
                log,
                client,
                model,
                report_request.id,
                AnalyticsReportGranularity.DAILY,
                cutoff.date(),
            )
        )

    # min keeps the first of equal dates, so a tie goes to the request Apple
    # lists first.
    report_instance = min(
        report_instances,
        key=lambda x: x.attributes.processingDate,
        default=None,
    )

    if not report_instance:
        await _try_create_report_request(
            log,
            client,
            app_id,
            AnalyticsReportAccessType.ONE_TIME_SNAPSHOT,
        )
        log.info(
            f"Waiting for a snapshot of the {model.report_name} report made on or after {cutoff.date()} for app {app_id}. "
            "Apple allows one new snapshot request per app per month.",
            {
                "app_id": app_id,
                "report_name": model.report_name,
                "report_request_ids": [
                    report_request.id for report_request in snapshot_requests
                ],
            },
        )

        # Apple allows one new snapshot request a month, so checking more often
        # gains nothing. The CDK calls the backfill again as soon as it yields.
        await asyncio.sleep(5 * 60)
        yield unchanged_page
        return

    async for row in _stream_analytics_report_data(
        log,
        client,
        app_id,
        model,
        report_instance,
    ):
        if row.record_date <= cutoff.date():
            yield row


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
