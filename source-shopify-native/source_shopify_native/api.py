import asyncio
import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPMixin

from source_shopify_native.graphql.bulk_job_manager import BulkJobManager
from source_shopify_native.models import (
    BaseResponseData,
    ShopifyGraphQLResource,
    StoreCapabilities,
    StoreValidationContext,
    TShopifyGraphQLResource,
)

from .graphql.client import ShopifyGraphQLClient
from .graphql.nested_resolver import NESTED_CONNECTION_CONCURRENCY, resolve_document
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
    capabilities: StoreCapabilities,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ShopifyGraphQLResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + window_size
    end = min(max_end, datetime.now(tz=UTC))
    query = model.build_query(log_cursor, end, capabilities=capabilities)

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


async def bulk_fetch_snapshot(
    http: HTTPMixin,
    bulk_job_manager: BulkJobManager,
    model: type[ShopifyGraphQLResource],
    store: str,
    capabilities: StoreCapabilities,
    log: Logger,
) -> AsyncGenerator[ShopifyGraphQLResource, None]:
    now = datetime.now(tz=UTC)
    query = model.build_query(now, now, capabilities=capabilities)

    url = await bulk_job_manager.execute(model, query)

    if url is None:
        log.info(
            f"[{store}] Bulk query job found no results for {model.__name__}."
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
    capabilities: StoreCapabilities,
    log: Logger,
) -> AsyncGenerator[TShopifyGraphQLResource, None]:
    after: str | None = None
    ctx = StoreValidationContext(store=store)
    # Shared across all pages of this fetch so total concurrent nested re-queries stay bounded.
    nested_sem = asyncio.Semaphore(NESTED_CONNECTION_CONCURRENCY)
    # Streams that inline nested connections use a smaller page to keep per-query cost in budget.
    page_size = model.OUTER_PAGE_SIZE or PAGE_SIZE

    while True:
        query = model.build_query(
            start=start,
            end=end,
            first=page_size,
            after=after,
            capabilities=capabilities,
        )

        data = await client.request(query, data_model, log, context=ctx)

        docs = data.nodes
        if model.NESTED_CONNECTIONS:
            await _stitch_nested_connections(client, model, docs, nested_sem, log)
        for doc in docs:
            yield doc

        page_info = data.page_info
        if not page_info.hasNextPage or not page_info.endCursor:
            return

        after = page_info.endCursor


async def _stitch_nested_connections(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    nodes: list[TShopifyGraphQLResource],
    sem: asyncio.Semaphore,
    log: Logger,
) -> None:
    """Resolve each node's nested connections in place, draining overflow under the passed-in semaphore.

    Per node, we pass the resolver a `document` view - the node's `id` plus the fields its
    connections' paths start from - referencing those fields rather than copying them. So when the
    resolver flattens a connection in place, the change lands on the node itself.
    """

    async def stitch(node: TShopifyGraphQLResource) -> None:
        # `document` carries the node's id plus the field each connection's path starts from.
        document: dict[str, Any] = {"id": getattr(node, "id", None)}
        for connection in model.NESTED_CONNECTIONS:
            key = connection.parent_path[0] if connection.parent_path else connection.field_name
            document[key] = getattr(node, key, None)

        await resolve_document(client, document, model.NESTED_CONNECTIONS, log, sem)

        # Only root connections (an empty parent_path) need this reassignment with setattr.
        # The resolver reassigned them on `document` instead of mutating a shared object,
        # so the node still holds the pre-resolved value.
        for connection in model.NESTED_CONNECTIONS:
            if not connection.parent_path:
                setattr(node, connection.field_name, document[connection.field_name])

    await asyncio.gather(*[stitch(node) for node in nodes])


async def fetch_snapshot(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    capabilities: StoreCapabilities,
    log: Logger,
) -> AsyncGenerator[TShopifyGraphQLResource, None]:
    now = datetime.now(tz=UTC)
    async for doc in _paginate_through_resources(
        client=client,
        model=model,
        data_model=data_model,
        start=now,
        end=now,
        store=store,
        capabilities=capabilities,
        log=log,
    ):
        yield doc


async def fetch_incremental_unsorted(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    capabilities: StoreCapabilities,
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
        capabilities=capabilities,
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
    capabilities: StoreCapabilities,
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
        capabilities=capabilities,
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
    capabilities: StoreCapabilities,
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
        capabilities=capabilities,
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
