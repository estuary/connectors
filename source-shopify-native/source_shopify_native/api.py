import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPMixin

from source_shopify_native.graphql.bulk_job_manager import BulkJobManager
from source_shopify_native.models import (
    BaseResponseData,
    ShopifyGraphQLResource,
    StoreValidationContext,
    TShopifyGraphQLResource,
)

from .graphql.client import ShopifyGraphQLClient
from .utils import dt_to_str, str_to_dt

CHECKPOINT_INTERVAL = 1_000
PAGE_SIZE = 250
TARGET_FETCH_PAGE_INVOCATION_RUN_TIME = 60 * 5  # 5 minutes


async def bulk_fetch_incremental(
    http: HTTPMixin,
    window_size: timedelta,
    bulk_job_manager: BulkJobManager,
    model: type[ShopifyGraphQLResource],
    store: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ShopifyGraphQLResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + window_size
    end = min(max_end, datetime.now(tz=UTC))
    query = model.build_query(log_cursor, end)

    url = await bulk_job_manager.execute(model, query)

    if url is None:
        log.info(
            f"[{store}] Bulk query job found no results between {log_cursor} and {end}."
        )
        yield end
        return

    _, lines = await http.request_lines(log, url)
    ctx = StoreValidationContext(store=store)
    async for record in model.process_result(log, lines()):
        resource = model.model_validate(record, context=ctx)
        yield resource

    yield end


async def bulk_fetch_full_refresh(
    http: HTTPMixin,
    start_date: datetime,
    bulk_job_manager: BulkJobManager,
    model: type[ShopifyGraphQLResource],
    store: str,
    log: Logger,
) -> AsyncGenerator[ShopifyGraphQLResource, None]:
    end = datetime.now(tz=UTC)
    query = model.build_query(start_date, end)

    url = await bulk_job_manager.execute(model, query)

    if url is None:
        log.info(
            f"[{store}] Bulk query job found no results between {start_date} and {end} for {model.__name__}."
        )
        return

    _, lines = await http.request_lines(log, url)
    ctx = StoreValidationContext(store=store)
    async for record in model.process_result(log, lines()):
        resource = model.model_validate(record, context=ctx)
        yield resource


async def _paginate_through_resources(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    start: datetime,
    end: datetime,
    store: str,
    log: Logger,
) -> AsyncGenerator[TShopifyGraphQLResource, None]:
    after: str | None = None
    ctx = StoreValidationContext(store=store)

    while True:
        query = model.build_query(
            start=start,
            end=end,
            first=PAGE_SIZE,
            after=after,
        )

        data = await client.request(query, data_model, log, context=ctx)

        for doc in data.nodes:
            yield doc

        page_info = data.page_info
        if not page_info.hasNextPage or not page_info.endCursor:
            return

        after = page_info.endCursor


async def fetch_incremental_unsorted(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TShopifyGraphQLResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    end = datetime.now(tz=UTC)

    last_dt = log_cursor

    async for doc in _paginate_through_resources(
        client=client,
        model=model,
        data_model=data_model,
        start=log_cursor,
        end=end,
        store=store,
        log=log,
    ):
        cursor_value = doc.get_cursor_value()
        if cursor_value > log_cursor:
            yield doc

            if cursor_value > last_dt:
                last_dt = cursor_value

    if last_dt != log_cursor:
        yield last_dt


async def fetch_incremental(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TShopifyGraphQLResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    end = datetime.now(tz=UTC)

    last_dt = log_cursor
    count = 0

    async for doc in _paginate_through_resources(
        client=client,
        model=model,
        data_model=data_model,
        start=log_cursor,
        end=end,
        store=store,
        log=log,
    ):
        cursor_value = doc.get_cursor_value()

        if cursor_value <= log_cursor:
            continue

        if count >= CHECKPOINT_INTERVAL and cursor_value > last_dt:
            yield last_dt
            count = 0

        last_dt = cursor_value
        count += 1
        yield doc

    if last_dt != log_cursor and count > 0:
        yield last_dt


async def backfill_incremental(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TShopifyGraphQLResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)

    last_seen_dt = start
    count = 0
    start_time = time.time()

    async for doc in _paginate_through_resources(
        client=client,
        model=model,
        data_model=data_model,
        start=start,
        end=cutoff,
        store=store,
        log=log,
    ):
        cursor_value = doc.get_cursor_value()

        if cursor_value < start:
            continue

        if cursor_value > cutoff:
            return

        if count >= CHECKPOINT_INTERVAL and cursor_value > last_seen_dt:
            yield dt_to_str(last_seen_dt)
            count = 0

            if time.time() - start_time >= TARGET_FETCH_PAGE_INVOCATION_RUN_TIME:
                return

        yield doc
        count += 1
        last_seen_dt = cursor_value
