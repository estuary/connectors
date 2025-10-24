from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
from estuary_cdk.buffer_ordered import buffer_ordered

from .client import FacebookAPIClient, FacebookRequestParams, AfterCursor
from .job_manager import FacebookInsightsJobManager
from .models import FacebookResource, FacebookInsightsResource


async def snapshot_resource(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    accounts: list[str],
    log: Logger,
) -> AsyncGenerator[FacebookResource, None]:
    # TODO: use buffered order
    params = FacebookRequestParams(fields=resource_model.fields)

    async def _generator():
        for account_id in accounts:

            async def task():
                results = []
                async for item in client.fetch_all(
                    resource_model, params, account_id=account_id
                ):
                    results.append(item)
                return results

            yield task()

    async for results in buffer_ordered(_generator(), concurrency=len(accounts)):
        for item in results:
            yield item


async def snapshot_ad_creatives(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    accounts: list[str],
    log: Logger,
) -> AsyncGenerator[FacebookResource, None]:
    params = FacebookRequestParams(fields=resource_model.fields)

    async def fetch_ad_creatives_with_thumbnails(
        account_id: str,
    ) -> AsyncGenerator[FacebookResource, None]:
        async for creative in client.fetch_all(
            resource_model,
            params,
            account_id=account_id,
        ):
            thumbnail_url = getattr(creative, "thumbnail_url", None)

            if thumbnail_url:
                thumbnail_data_url = await client.fetch_thumbnail_data_url(
                    thumbnail_url
                )
                creative = creative.model_copy(
                    update={
                        "thumbnail_data_url": thumbnail_data_url,
                    }
                )
            else:
                creative = creative.model_copy(
                    update={
                        "thumbnail_data_url": None,
                    }
                )

            yield creative

    async def _generator():
        for account_id in accounts:

            async def task():
                results = []
                async for item in fetch_ad_creatives_with_thumbnails(account_id):
                    results.append(item)
                return results

            yield task()

    async for results in buffer_ordered(_generator(), concurrency=len(accounts)):
        for item in results:
            yield item


async def fetch_page(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    """
    Fetch page function for standard incremental Facebook resources for a single account.

    This follows the Stripe pattern of handling one account per fetch function,
    allowing for individual sub-task progress tracking.

    Standard incremental resources:
    - Data ordered by cursor field in ascending order (oldest first)
    - Supports API filtering with GREATER_THAN operator (when available)
    - Examples: Campaigns, Activities

    Args:
        client: Facebook API client
        resource_model: The resource model class
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        page_cursor: PageCursor for pagination (None to start from beginning)
        cutoff: LogCursor representing the cutoff timestamp
    """
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert isinstance(page_cursor, (str, type(None))), (
        f"Expected str or None for page_cursor, got {type(page_cursor)}"
    )

    params = FacebookRequestParams(fields=resource_model.fields)

    if resource_model.entity_prefix:
        params.filtering = [
            {
                "field": f"{resource_model.entity_prefix}.{resource_model.cursor_field}",
                "operator": "GREATER_THAN",
                "value": int(start_date.timestamp()),
            },
            {
                "field": f"{resource_model.entity_prefix}.{resource_model.cursor_field}",
                "operator": "LESS_THAN",
                "value": int(cutoff.timestamp()),
            },
        ]

    # Handle page cursor for resuming from a previous state
    if page_cursor:
        params.after_cursor = page_cursor

    async for item in client.fetch_page(resource_model, params, account_id=account_id):
        if isinstance(item, AfterCursor):
            # Yield PageCursor for checkpointing
            if item.cursor:
                yield item.cursor
        else:
            # For standard incremental, yield items that are older than or equal to cutoff
            try:
                cursor_value_str = getattr(item, resource_model.cursor_field, "")
                if cursor_value_str:
                    item_time = datetime.fromisoformat(
                        cursor_value_str.replace("Z", "+00:00")
                    )
                    if item_time <= cutoff:
                        yield item
                else:
                    # If no cursor field value, yield the item
                    yield item
            except (ValueError, AttributeError) as e:
                log.warning(
                    f"Could not parse {resource_model.cursor_field} for item {getattr(item, 'id', 'unknown')}: {e}"
                )
                # Yield the item anyway since we can't determine the time
                yield item


async def fetch_changes(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    """
    Fetch changes for standard incremental Facebook resources for a single account.

    Standard incremental resources:
    - Data ordered by cursor field in ascending order (oldest first)
    - Supports API filtering with GREATER_THAN operator
    - Examples: Campaigns, Ads, AdSets

    Args:
        client: Facebook API client
        resource_model: The resource model class
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        log_cursor: LogCursor representing the last updated timestamp
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    # Use API filtering with GREATER_THAN operator for efficiency if entity_prefix is available
    params = FacebookRequestParams(fields=resource_model.fields)

    if resource_model.entity_prefix:
        params.filtering = [
            {
                "field": f"{resource_model.entity_prefix}.{resource_model.cursor_field}",
                "operator": "GREATER_THAN",
                "value": int(log_cursor.timestamp()),
            }
        ]
    max_updated_at = log_cursor
    has_results = False

    async for item in client.fetch_all(resource_model, params, account_id=account_id):
        try:
            cursor_value_str = getattr(item, resource_model.cursor_field, "")
            if cursor_value_str:
                item_time = datetime.fromisoformat(
                    cursor_value_str.replace("Z", "+00:00")
                )

                # Only yield items that are newer than the cursor
                if item_time > log_cursor:
                    has_results = True
                    max_updated_at = max(max_updated_at, item_time)
                    yield item

        except (ValueError, AttributeError) as e:
            log.warning(
                f"Could not parse {resource_model.cursor_field} for item {getattr(item, 'id', 'unknown')}: {e}"
            )
            # Skip items we can't parse the timestamp for
            continue

    # Yield new cursor if we had results
    if has_results:
        yield max_updated_at


async def fetch_page_reverse_order(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    """
    Fetch page function for reverse order incremental Facebook resources for a single account.

    Reverse order incremental resources:
    - Data ordered by cursor field in descending order (newest first)
    - No API filtering support - filters in memory
    - Examples: Videos, Images

    Args:
        client: Facebook API client
        resource_model: The resource model class
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        page_cursor: PageCursor for pagination (None to start from beginning)
        cutoff: LogCursor representing the cutoff timestamp
    """
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert isinstance(page_cursor, (str, type(None))), (
        f"Expected str or None for page_cursor, got {type(page_cursor)}"
    )

    params = FacebookRequestParams(fields=resource_model.fields)

    # Handle page cursor for resuming from a previous state
    if page_cursor:
        params.after_cursor = page_cursor

    async for item in client.fetch_page(resource_model, params, account_id=account_id):
        if isinstance(item, AfterCursor):
            # Yield PageCursor for checkpointing
            if item.cursor:
                yield item.cursor
        else:
            # For reverse order incremental, yield items that are newer than or equal to cutoff
            # (since they come in DESC order, we want to stop when we reach cutoff)
            try:
                cursor_value_str = getattr(item, resource_model.cursor_field, "")
                if cursor_value_str:
                    item_time = datetime.fromisoformat(
                        cursor_value_str.replace("Z", "+00:00")
                    )
                    if item_time >= cutoff:
                        yield item
                else:
                    # If no cursor field value, yield the item
                    yield item
            except (ValueError, AttributeError) as e:
                log.warning(
                    f"Could not parse {resource_model.cursor_field} for item {getattr(item, 'id', 'unknown')}: {e}"
                )
                # Yield the item anyway since we can't determine the time
                yield item


async def fetch_changes_reverse_order(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    """
    Fetch changes for reverse order incremental Facebook resources for a single account.

    Reverse order incremental resources:
    - Data ordered by cursor field in descending order (newest first)
    - No API filtering support - filters in memory
    - Examples: Videos, Images

    Args:
        client: Facebook API client
        resource_model: The resource model class
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        log_cursor: LogCursor representing the last updated timestamp
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    params = FacebookRequestParams(fields=resource_model.fields)
    max_updated_at = log_cursor
    has_results = False

    # For reverse order streams, we get newest items first and stop when we reach the cursor
    async for item in client.fetch_all(resource_model, params, account_id=account_id):
        # Filter out deleted images (Images stream only)
        if resource_model.name == "images":
            status = getattr(item, "status", None)
            if status == "deleted":
                continue

        try:
            cursor_value_str = getattr(item, resource_model.cursor_field, "")
            if cursor_value_str:
                item_time = datetime.fromisoformat(
                    cursor_value_str.replace("Z", "+00:00")
                )

                # Since data is in DESC order, stop when we reach items older than cursor
                if item_time < log_cursor:
                    break

                # Yield items that are newer than the cursor
                has_results = True
                max_updated_at = max(max_updated_at, item_time)
                yield item

        except (ValueError, AttributeError) as e:
            log.warning(
                f"Could not parse {resource_model.cursor_field} for item {getattr(item, 'id', 'unknown')}: {e}"
            )
            # Skip items we can't parse the timestamp for
            continue

    # Yield new cursor if we had results
    if has_results:
        yield max_updated_at


async def fetch_page_activities(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    """
    Fetch page function for Activities stream with special 'since' parameter.

    Activities stream uses the 'since' parameter instead of filtering.
    Data is ordered by event_time in ascending order (oldest first).

    Args:
        client: Facebook API client
        resource_model: The resource model class (should be Activities)
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        page_cursor: PageCursor for pagination (None to start from beginning)
        cutoff: LogCursor representing the cutoff timestamp
    """
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert isinstance(page_cursor, (str, type(None))), (
        f"Expected str or None for page_cursor, got {type(page_cursor)}"
    )

    params = FacebookRequestParams(fields=resource_model.fields)

    # Handle page cursor for resuming from a previous state
    if page_cursor:
        params.after_cursor = page_cursor

    async for item in client.fetch_page(resource_model, params, account_id=account_id):
        if isinstance(item, AfterCursor):
            # Yield PageCursor for checkpointing
            if item.cursor:
                yield item.cursor
        else:
            # For Activities, yield items that are older than or equal to cutoff
            try:
                cursor_value_str = getattr(item, resource_model.cursor_field, "")
                if cursor_value_str:
                    item_time = datetime.fromisoformat(
                        cursor_value_str.replace("Z", "+00:00")
                    )
                    if item_time <= cutoff:
                        yield item
                else:
                    # If no cursor field value, yield the item
                    yield item
            except (ValueError, AttributeError) as e:
                log.warning(
                    f"Could not parse {resource_model.cursor_field} for item: {e}"
                )
                # Yield the item anyway since we can't determine the time
                yield item


async def fetch_changes_activities(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    """
    Fetch changes for Activities stream with special 'since' parameter.

    Activities stream uses the 'since' parameter to filter results on the API side.
    Data is ordered by event_time in ascending order (oldest first).

    Args:
        client: Facebook API client
        resource_model: The resource model class (should be Activities)
        account_id: Single Facebook account ID to fetch from
        log: Logger instance
        log_cursor: LogCursor representing the last event_time timestamp
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    # Use 'since' parameter to filter on the API side
    params = FacebookRequestParams(
        fields=resource_model.fields, since=int(log_cursor.timestamp())
    )
    max_updated_at = log_cursor
    has_results = False

    async for item in client.fetch_all(resource_model, params, account_id=account_id):
        try:
            cursor_value_str = getattr(item, resource_model.cursor_field, "")
            if cursor_value_str:
                item_time = datetime.fromisoformat(
                    cursor_value_str.replace("Z", "+00:00")
                )

                # Only yield items that are newer than the cursor
                if item_time > log_cursor:
                    has_results = True
                    max_updated_at = max(max_updated_at, item_time)
                    yield item

        except (ValueError, AttributeError) as e:
            log.warning(f"Could not parse {resource_model.cursor_field} for item: {e}")
            # Skip items we can't parse the timestamp for
            continue

    # Yield new cursor if we had results
    if has_results:
        yield max_updated_at


async def fetch_page_insights(
    job_manager: FacebookInsightsJobManager,
    model: type[FacebookInsightsResource],
    account_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    """
    Fetch insights page (backfill) for a single account using job manager.

    This is used during initial backfill to fetch historical insights data.
    Processes ONE time slice per call to allow proper checkpointing.
    Each call submits a single job, waits for completion, emits data and cursor.

    Args:
        job_manager: FacebookInsightsJobManager instance
        resource_model: The insights resource model
        account_id: Single account ID to fetch insights for
        start_date: Start date for insights data
        log: Logger instance
        page_cursor: ISO date string to resume from, or None to start from start_date
        cutoff: LogCursor representing the cutoff date
        level: Insights level (account, campaign, adset, ad)
        breakdowns: Optional list of breakdown dimensions
        action_breakdowns: Optional list of action breakdown dimensions
        time_increment: Time increment in days (default: 1)

    Yields:
        FacebookResource instances with insights data, then PageCursor for checkpointing
    """
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert page_cursor is None or isinstance(page_cursor, str), (
        f"Expected None or str for page_cursor, got {type(page_cursor)}"
    )

    if page_cursor:
        current_date = datetime.fromisoformat(page_cursor).replace(tzinfo=UTC)
    else:
        current_date = start_date

    if current_date >= cutoff:
        return

    # Calculate end date for this single time slice
    job_end = min(current_date + timedelta(days=model.time_increment - 1), cutoff)

    date_range = {
        "since": current_date.strftime("%Y-%m-%d"),
        "until": job_end.strftime("%Y-%m-%d"),
    }

    try:
        # Submit ONE job, wait for completion, and yield all results
        # Facebook API respects the date_range we specified, so no need to filter
        async for result in job_manager.fetch_insights(
            log=log,
            model=model,
            account_id=account_id,
            date_range=date_range,
        ):
            insight = model.model_validate(result)
            yield insight

        # Calculate next time slice start date by moving the start date forward 1 day
        # This ensures that if a user provides a time_increment > 1, we still move forward daily
        # to provide aggregated data per-day. This is so that the data emitted by the connector
        # covers the aggregated data per day, rather than per time_increment chunk.
        next_date = current_date + timedelta(days=1)

        if next_date < cutoff:
            yield next_date.strftime("%Y-%m-%d")

    except Exception as e:
        log.error(f"Failed to fetch insights page for account {account_id}: {e}")
        raise


async def fetch_changes_insights(
    job_manager: FacebookInsightsJobManager,
    model: type[FacebookInsightsResource],
    account_id: str,
    log: Logger,
    log_cursor: LogCursor,
    lookback_window: int = 28,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    """
    Fetch insights changes (incremental) for a single account using job manager.

    This is used for ongoing incremental syncs. It fetches data from the cursor
    date forward, with a lookback window to account for Facebook's data updates.

    Facebook freezes insight data 28 days after generation, so we re-sync recent
    data to catch metric updates. The cursor moves to 'now' to ensure forward progress
    while the lookback window re-captures recently updated data.

    Args:
        job_manager: FacebookInsightsJobManager instance
        resource_model: The insights resource model
        account_id: Single account ID to fetch insights for
        log: Logger instance
        log_cursor: LogCursor (datetime) representing the last sync end time
        level: Insights level (account, campaign, adset, ad)
        breakdowns: Optional list of breakdown dimensions
        action_breakdowns: Optional list of action breakdown dimensions
        time_increment: Time increment in days (default: 1)
        lookback_window: Days to look back from cursor (default: 28)

    Yields:
        FacebookResource instances with insights data, then new cursor (end_date)
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    # We will be running this every hour, minute or some other interval that is not daily
    # so we do not want to get data from lookback period
    # each time. Instead, we will fetch data for a day range and every time the log cursor changes to a new
    # day, we will fetch data from (new_cursor - lookback) to now.
    now = datetime.now(UTC)
    cursor_date = log_cursor.date()
    today = now.date()

    # If the cursor is from a previous day, apply lookback window since the last fetch
    # was not today
    if cursor_date < today:
        # calculate the start date based on the lookback window while also ensuring
        # we only set to the oldest date or simply use the log cursor if the log_cursor is more than
        # lookback days ago
        lookback_start_date = log_cursor - timedelta(days=lookback_window)
        start_date = min(lookback_start_date, log_cursor)
        end_date = now
    else:
        # Same day - only fetch data from cursor forward to avoid redundant lookback fetches
        start_date = log_cursor
        end_date = now

    date_range = {
        "since": start_date.strftime("%Y-%m-%d"),
        "until": end_date.strftime("%Y-%m-%d"),
    }

    try:
        # Fetch and yield all insights (including lookback window data)
        # Facebook API respects the date_range, so no need to filter
        async for result in job_manager.fetch_insights(
            log=log,
            model=model,
            account_id=account_id,
            date_range=date_range,
        ):
            insight = model.model_validate(result)
            yield insight

    except Exception as e:
        log.error(f"Failed to fetch insights changes for account {account_id}: {e}")
        raise

    # Update cursor to end_date (NOT max date_start from results!)
    # This ensures cursor moves forward while lookback window re-captures updates
    # Example: cursor=2024-12-01, lookback=28d, today=2024-12-31
    #   Fetch: 2024-11-03 to 2024-12-31 (includes lookback window)
    #   New cursor: 2024-12-31 (moves forward)
    # Next run: cursor=2024-12-31, today=2025-01-01
    #   Fetch: 2024-12-03 to 2025-01-01 (still captures updates to Dec data)
    yield end_date
