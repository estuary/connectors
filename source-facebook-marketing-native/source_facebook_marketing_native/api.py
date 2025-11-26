from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
from estuary_cdk.buffer_ordered import buffer_ordered

from .client import FacebookAPIClient, FacebookRequestParams, FacebookCursor
from .job_manager import FacebookInsightsJobManager
from .models import FacebookResource, FacebookInsightsResource, Images


async def snapshot_resource(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    accounts: list[str],
    log: Logger,
    include_deleted: bool,
) -> AsyncGenerator[FacebookResource, None]:
    params = FacebookRequestParams(fields=resource_model.fields)

    async def _generator():
        for account_id in accounts:

            async def task():
                results = []
                async for item in client.fetch_all(
                    resource_model, params, include_deleted, account_id=account_id
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
    include_deleted: bool,
    fetch_thumbnail_images: bool,
) -> AsyncGenerator[FacebookResource, None]:
    params = FacebookRequestParams(fields=resource_model.fields)

    async def fetch_ad_creatives(
        account_id: str,
    ) -> AsyncGenerator[FacebookResource, None]:
        async for creative in client.fetch_all(
            resource_model,
            params,
            include_deleted,
            account_id=account_id,
        ):
            if not fetch_thumbnail_images:
                yield creative
                continue

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
                async for item in fetch_ad_creatives(account_id):
                    results.append(item)
                return results

            yield task()

    async for results in buffer_ordered(_generator(), concurrency=len(accounts)):
        for item in results:
            yield item


async def fetch_page(
    client: FacebookAPIClient,
    model: type[FacebookResource],
    account_id: str,
    start_date: datetime,
    include_deleted: bool,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    """
    Fetch page function for standard incremental Facebook resources for a single account.
    """
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert isinstance(page_cursor, (str, type(None))), (
        f"Expected str or None for page_cursor, got {type(page_cursor)}"
    )

    params = FacebookRequestParams(fields=model.fields)

    # Only certain resources can be filtered by the API
    if model.entity_prefix:
        params.filtering = [
            {
                "field": f"{model.entity_prefix}.{model.cursor_field}",
                "operator": "GREATER_THAN",
                "value": int(start_date.timestamp()),
            },
            {
                "field": f"{model.entity_prefix}.{model.cursor_field}",
                "operator": "LESS_THAN",
                "value": int(cutoff.timestamp()),
            },
        ]

    if page_cursor:
        params.after_cursor = page_cursor

    async for doc in client.fetch_page(
        model, params, include_deleted, account_id=account_id
    ):
        if isinstance(doc, FacebookCursor):
            if doc.cursor:
                yield doc.cursor
        else:
            try:
                cursor_value_str = getattr(doc, model.cursor_field, "")
                if cursor_value_str:
                    doc_cursor = datetime.fromisoformat(
                        cursor_value_str.replace("Z", "+00:00")
                    )
                    if doc_cursor <= cutoff:
                        yield doc
                else:
                    # Raise if no cursor field value in case the document is emitted after an incremental document
                    # for the same resource, which would cause the older document to be the latest one in the collection.
                    raise RuntimeError(
                        f"No cursor field value for {model.name} (ID: {getattr(doc, 'id', 'unknown')}). Cannot ensure correct ordering."
                    )
            except (ValueError, AttributeError) as e:
                raise RuntimeError(
                    f"Could not parse {model.cursor_field} for item {getattr(doc, 'id', 'unknown')} for resource {model.name}: {e}"
                )


async def fetch_changes(
    client: FacebookAPIClient,
    model: type[FacebookResource],
    account_id: str,
    include_deleted: bool,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    """
    Fetch changes for standard incremental Facebook resources for a single account.
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    params = FacebookRequestParams(fields=model.fields)

    if model.entity_prefix:
        params.filtering = [
            {
                "field": f"{model.entity_prefix}.{model.cursor_field}",
                "operator": "GREATER_THAN",
                "value": int(log_cursor.timestamp()),
            }
        ]
    max_updated_at = log_cursor
    has_results = False

    async for doc in client.fetch_all(
        model, params, include_deleted, account_id=account_id
    ):
        try:
            cursor_value_str = getattr(doc, model.cursor_field, "")
            if cursor_value_str:
                doc_cursor = datetime.fromisoformat(
                    cursor_value_str.replace("Z", "+00:00")
                )

                if doc_cursor > log_cursor:
                    has_results = True
                    max_updated_at = max(max_updated_at, doc_cursor)
                    yield doc
            else:
                raise RuntimeError(
                    f"No cursor field value for {model.name} (ID: {getattr(doc, 'id', 'unknown')}). Cannot ensure correct ordering."
                )
        except (ValueError, AttributeError) as e:
            raise RuntimeError(
                f"Could not parse {model.cursor_field} for item {getattr(doc, 'id', 'unknown')} for resource {model.name}: {e}"
            )

    if has_results:
        yield max_updated_at


async def fetch_page_reverse_order(
    client: FacebookAPIClient,
    resource_model: type[FacebookResource],
    account_id: str,
    start_date: datetime,
    include_deleted: bool,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FacebookResource | PageCursor, None]:
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert isinstance(page_cursor, (str, type(None))), (
        f"Expected str or None for page_cursor, got {type(page_cursor)}"
    )

    params = FacebookRequestParams(fields=resource_model.fields)

    if page_cursor:
        params.after_cursor = page_cursor

    async for doc in client.fetch_page(
        resource_model, params, include_deleted, account_id=account_id
    ):
        if isinstance(doc, FacebookCursor):
            if doc.cursor:
                yield doc.cursor
        else:
            try:
                cursor_value_str = getattr(doc, resource_model.cursor_field, "")
                if cursor_value_str:
                    doc_cursor = datetime.fromisoformat(
                        cursor_value_str.replace("Z", "+00:00")
                    )
                    if doc_cursor <= cutoff and doc_cursor >= start_date:
                        yield doc
                else:
                    raise RuntimeError(
                        f"No cursor field value for {resource_model.name} (ID: {getattr(doc, 'id', 'unknown')}). Cannot ensure correct ordering."
                    )
            except (ValueError, AttributeError) as e:
                raise RuntimeError(
                    f"Could not parse {resource_model.cursor_field} for item {getattr(doc, 'id', 'unknown')} for resource {resource_model.name}: {e}"
                )


async def fetch_changes_reverse_order(
    client: FacebookAPIClient,
    model: type[FacebookResource],
    account_id: str,
    include_deleted: bool,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FacebookResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    params = FacebookRequestParams(fields=model.fields)
    max_updated_at = log_cursor
    has_results = False

    async for doc in client.fetch_all(
        model, params, include_deleted, account_id=account_id
    ):
        # Facebook's API returns deleted images regardless of filtering, so filter them
        # client-side if the include_deleted flag is not set
        if model is Images and not include_deleted:
            status = getattr(doc, "status", None)
            if status == "deleted" or status == "DELETED":
                log.debug(
                    f"Skipping deleted image with ID {getattr(doc, 'id', 'unknown')}",
                    {"model": model},
                )
                continue

        try:
            cursor_value_str = getattr(doc, model.cursor_field, "")
            if cursor_value_str:
                doc_cursor = datetime.fromisoformat(
                    cursor_value_str.replace("Z", "+00:00")
                )

                # Since data is in DESC order, stop when we reach items older than cursor
                if doc_cursor <= log_cursor:
                    break

                has_results = True
                max_updated_at = max(max_updated_at, doc_cursor)
                yield doc
            else:
                raise RuntimeError(
                    f"No cursor field value for {model.name} (ID: {getattr(doc, 'id', 'unknown')}). Cannot ensure correct ordering."
                )
        except (ValueError, AttributeError) as e:
            raise RuntimeError(
                f"Could not parse {model.cursor_field} for item {getattr(doc, 'id', 'unknown')} for resource {model.name}: {e}"
            )

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
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"
    assert page_cursor is None or isinstance(page_cursor, str), (
        f"Expected None or str for page_cursor, got {type(page_cursor)}"
    )

    if page_cursor:
        start = datetime.fromisoformat(page_cursor).replace(tzinfo=UTC)
    else:
        start = start_date

    if start >= cutoff:
        return

    end = min(start + timedelta(days=1), cutoff)
    date_range = {
        "since": start.strftime("%Y-%m-%d"),
        "until": end.strftime("%Y-%m-%d"),
    }

    try:
        async for result in job_manager.fetch_insights(
            log=log,
            model=model,
            account_id=account_id,
            time_range=date_range,
        ):
            insight = model.model_validate(result)
            yield insight

        next_start = start + timedelta(days=1)

        if next_start < cutoff:
            yield next_start.isoformat()

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
    Fetch insights changes with lookback window handling.

    Facebook freezes insight data 28 days after generation. To capture changes
    this function fetches insights from the log_cursor to now. The full 28-day
    lookback period is only processed once per day to reduce data volume and minimize duplication.
    """
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime log_cursor, got {type(log_cursor)}"
    )

    now = datetime.now(UTC)
    cursor_date = log_cursor.date()
    today = now.date()

    if cursor_date < today:
        # Apply the lookback window only if the cursor is before today
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

    async for result in job_manager.fetch_insights(
        log=log,
        model=model,
        account_id=account_id,
        time_range=date_range,
    ):
        insight = model.model_validate(result)
        yield insight

    yield end_date
