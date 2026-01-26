import asyncio
import itertools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

import aiohttp

from estuary_cdk import emitted_changes_cache as cache
from estuary_cdk.buffer_ordered import buffer_ordered
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .export_job_manager import ExportJobManager, TruncatedExportJobError
from .models import (
    CampaignFinalStates,
    CampaignInProgressStates,
    CampaignMetrics,
    CampaignPreLaunchStates,
    Campaigns,
    CampaignsResponse,
    EventValidationContext,
    Events,
    ListUsers,
    Lists,
    MessageMediums,
    Templates,
    TemplateTypes,
    TExportResource,
    TIterableResource,
    SMALLEST_EXPORT_DATE_WINDOW_SIZE,
)
from .shared import BASE_URL, EPOCH, dt_to_ms, dt_to_str, ms_to_dt, str_to_dt


MAX_CAMPAIGNS_PAGE_SIZE = 1_000
# If too many campaigns are included in the query params for a metrics request, the URL gets
# too long and Iterable returns an error indicating as such. Empirically, 400 campaigns per
# metrics request seems to be just below the upper limit.
MAX_CAMPAIGNS_PER_METRICS_REQUEST = 400
# Fetch metrics for campaigns in a final state within the past 15 days
# so we capture any late arriving attributions.
CAMPAIGN_METRICS_LOOKBACK_WINDOW = timedelta(days=15)

MAX_LIST_USERS_CONCURRENCY = 5

async def snapshot_resources(
    http: HTTPSession,
    model: type[TIterableResource],
    log: Logger,
) -> AsyncGenerator[TIterableResource, None]:
    url = f"{BASE_URL}/{model.path}"

    _, body = await http.request_stream(log, url)

    processor = IncrementalJsonProcessor(
        body(),
        f"{model.path}.item",
        model,
    )

    async for doc in processor:
        yield doc


async def snapshot_templates(
    http: HTTPSession,
    model: type[Templates],
    log: Logger,
) -> AsyncGenerator[Templates, None]:
    url = f"{BASE_URL}/{model.path}"

    for template_type in TemplateTypes:
        for message_medium in MessageMediums:
            params = {
                "templateType": template_type,
                "messageMedium": message_medium,
            }

            _, body = await http.request_stream(log, url, params=params)

            processor = IncrementalJsonProcessor(
                body(),
                f"{model.path}.item",
                model,
            )

            async for doc in processor:
                yield doc


async def snapshot_list_users(
    http: HTTPSession,
    model: type[ListUsers],
    total_request_timeout: timedelta,
    log: Logger,
) -> AsyncGenerator[ListUsers, None]:
    list_ids: list[int] = []

    async for l in snapshot_resources(http, Lists, log):
        list_ids.append(l.id)

    url = f"{BASE_URL}/{model.path}"

    # We use an extended timeout for the /lists/getUsers endpoint since it can take
    # Iterable more than 5 minutes (aiohttp's default total timeout) to respond with
    # all users in a list. We also preserve the default sock_connect timeout.
    request_timeout = aiohttp.ClientTimeout(
        total=total_request_timeout.total_seconds(),
        sock_connect=30
    )

    # The /lists/getUsers endpoint has a rate limit of 5 requests per minute.
    rate_limit_interval = 60 / 5  # 12 seconds between requests

    # The API can take > 12 seconds to respond when a list has many thousands
    # of users. To improve concurrency, we use buffer_ordered to process
    # multiple lists concurrently while maintaining result order.
    async def _fetch_users_in_list(list_id: int) -> list[tuple[int, str]]:
        params = {"listId": list_id}
        response = await http.request(log, url, params=params, timeout=request_timeout)

        return [
            (list_id, user_id.decode())
            for user_id in response.splitlines()
            if user_id
        ]

    async def _generate_requests():
        for i, list_id in enumerate(list_ids):
            if i > 0:
                # If this isn't the first list, wait the rate_limit_interval before
                # allowing another list's users to be fetched.
                await asyncio.sleep(rate_limit_interval)
            yield _fetch_users_in_list(list_id)

    async for users in buffer_ordered(_generate_requests(), MAX_LIST_USERS_CONCURRENCY):
        for list_id, user_id in users:
            yield ListUsers(list_id=list_id, user_id=user_id)


async def _paginate_through_campaigns(
    http: HTTPSession,
    log: Logger,
    sort_param: str = "createdAt"
) -> AsyncGenerator[Campaigns, None]:
    url = f"{BASE_URL}/{Campaigns.path}"

    page = 1

    params = {
        "pageSize": MAX_CAMPAIGNS_PAGE_SIZE,
        "page": page,
        "sort": sort_param,
    }

    while True:
        _, body = await http.request_stream(log, url, params=params)

        processor = IncrementalJsonProcessor(
            body(),
            f"{Campaigns.path}.item",
            Campaigns,
            CampaignsResponse,
        )

        async for campaign in processor:
            yield campaign

        remainder = processor.get_remainder()

        if not remainder.nextPageUrl:
            break

        page += 1
        params["page"] = page


async def fetch_campaigns(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Campaigns | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_checkpointed_ts = dt_to_ms(log_cursor)
    most_recent_ts = last_checkpointed_ts

    async for campaign in _paginate_through_campaigns(http, log, sort_param="-updatedAt"):
        if campaign.updatedAt <= last_checkpointed_ts:
            break

        most_recent_ts = max(most_recent_ts, campaign.updatedAt)
        yield campaign

    if most_recent_ts > last_checkpointed_ts:
        yield ms_to_dt(most_recent_ts)


async def backfill_campaigns(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Campaigns | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert page is None

    cutoff_ms = dt_to_ms(cutoff)

    async for campaign in _paginate_through_campaigns(http, log):
        if campaign.updatedAt <= cutoff_ms:
            yield campaign


async def _fetch_metrics_for_campaigns(
    http: HTTPSession,
    log: Logger,
    campaign_ids: list[int],
) -> AsyncGenerator[CampaignMetrics, None]:
    url = f"{BASE_URL}/{CampaignMetrics.path}"

    params = {
        "startDateTime": EPOCH.date().isoformat(),
        "campaignId": campaign_ids,
    }

    _, body = await http.request_stream(log, url, params=params)

    processor = IncrementalCSVProcessor(
        body(),
        CampaignMetrics,
    )

    async for row in processor:
        yield row


def _should_fetch_metrics(
    campaign: Campaigns,
    start: int,
    end: int,
) -> bool:
    if campaign.campaignState in CampaignPreLaunchStates:
        return False

    if campaign.campaignState in CampaignInProgressStates:
        return True

    if campaign.campaignState in CampaignFinalStates:
        return campaign.most_recent_ts >= start and campaign.most_recent_ts <= end

    raise ValueError(f"Unknown campaign state {campaign.campaignState} for campaign {campaign.id}")


async def fetch_campaign_metrics(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CampaignMetrics | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)

    start = log_cursor - CAMPAIGN_METRICS_LOOKBACK_WINDOW

    start_ms = dt_to_ms(start)
    end_ms = dt_to_ms(now)

    campaign_ids: list[int] = []

    async for campaign in _paginate_through_campaigns(http, log):
        if _should_fetch_metrics(campaign, start_ms, end_ms):
            campaign_ids.append(campaign.id)

    for batch in itertools.batched(campaign_ids, MAX_CAMPAIGNS_PER_METRICS_REQUEST):
        async for metric in _fetch_metrics_for_campaigns(http, log, list(batch)):
            yield metric

        # The /campaigns/metrics endpoint has a rate limit of 10 requests
        # per minute. Managing different rate limits per endpoint is currently not
        # supported by the CDK, so we do the simplest solution here and sleep long
        # enough to not get rate limited.
        await asyncio.sleep(60 / 10) 

    yield now


async def fetch_export_resources(
    http: HTTPSession,
    model: type[TExportResource],
    export_job_manager: ExportJobManager,
    data_type: str,
    horizon: timedelta | None,
    window_size: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TExportResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)

    start = log_cursor
    max_end = now - horizon if horizon else now

    end = min(
        max_end,
        start + window_size,
    )

    if start >= end:
        return

    validation_context = EventValidationContext(event_type=data_type) if model is Events else None

    while True:
        try:
            async for doc in export_job_manager.execute(model, data_type, start, end, validation_context):
                if cache.should_yield(data_type, doc.identifier, doc.cursor_value):
                    yield doc

            break
        except TruncatedExportJobError:
            log.info("There are too many results in the requested date window and the export job was truncated. Attempting to retry export job with a smaller date window.")
            new_date_window = (end - start) / 2

            # It's extremely unlikely we'd ever reduce the date window size below SMALLEST_DATE_WINDOW_SIZE,
            # but if it ever does happen, I'd like to know to aid troubleshooting efforts.
            if new_date_window < SMALLEST_EXPORT_DATE_WINDOW_SIZE:
                raise RuntimeError(f"Date window is too small.\nstart: {start}\nend: {end}\ndate_window: {new_date_window}")

            end = start + new_date_window

    yield end


async def backfill_export_resources(
    http: HTTPSession,
    model: type[TExportResource],
    export_job_manager: ExportJobManager,
    data_type: str,
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TExportResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is not None:
        assert isinstance(page, str)
        start = str_to_dt(page)
    else:
        start = start_date

    if start >= cutoff:
        return

    end = min(
        cutoff,
        start + window_size,
    )

    validation_context = EventValidationContext(event_type=data_type) if model is Events else None

    while True:
        try:
            async for doc in export_job_manager.execute(model, data_type, start, end, validation_context):
                yield doc

            break
        except TruncatedExportJobError:
            log.info("There are too many results in the requested date window and the export job was truncated. Attempting to retry export job with a smaller date window.")
            new_date_window = (end - start) / 2

            # It's extremely unlikely we'd ever reduce the date window size below SMALLEST_DATE_WINDOW_SIZE,
            # but if it ever does happen, I'd like to know to aid troubleshooting efforts.
            if new_date_window < SMALLEST_EXPORT_DATE_WINDOW_SIZE:
                raise RuntimeError(f"Date window is too small.\nstart: {start}\nend: {end}\ndate_window: {new_date_window}")

            end = start + new_date_window

    yield dt_to_str(end)
