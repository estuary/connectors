import asyncio
import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Callable

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
from .graphql.payments.payouts import Payouts
from .utils import dt_to_str, str_to_dt

CHECKPOINT_INTERVAL = 1_000
PAGE_SIZE = 250
# How stale a payout's status may get. A day was agreed as fresh enough, and it keeps the
# refresh's cost flat.
RECENT_PAYOUTS_REFRESH_INTERVAL = timedelta(days=1)
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


async def _paginate_pages[T: ShopifyGraphQLResource](
    client: ShopifyGraphQLClient,
    data_model: type[BaseResponseData[T]],
    build_page_query: Callable[[str | None], str],
    store: str,
    log: Logger,
) -> AsyncGenerator[BaseResponseData[T], None]:
    """Yield every page of a query, first to last.

    `build_page_query` takes the cursor to read after, which is None for the first page.
    """
    after: str | None = None
    ctx = StoreValidationContext(store=store)

    while True:
        data = await client.request(build_page_query(after), data_model, log, context=ctx)
        yield data

        page_info = data.page_info
        if not page_info.hasNextPage:
            return

        after = page_info.endCursor


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
    # Shared across all pages of this fetch so total concurrent nested re-queries stay bounded.
    nested_sem = asyncio.Semaphore(NESTED_CONNECTION_CONCURRENCY)
    # Streams that inline nested connections use a smaller page to keep per-query cost in budget.
    page_size = model.OUTER_PAGE_SIZE or PAGE_SIZE

    def build_page_query(after: str | None) -> str:
        return model.build_query(
            start=start,
            end=end,
            first=page_size,
            after=after,
            capabilities=capabilities,
        )

    async for data in _paginate_pages(client, data_model, build_page_query, store, log):
        docs = data.nodes
        if model.NESTED_CONNECTIONS:
            await _stitch_nested_connections(client, model, docs, nested_sem, log)
        for doc in docs:
            yield doc


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


def _recent_payouts_refresh_is_due(last_refresh: datetime, now: datetime) -> bool:
    return now - last_refresh >= RECENT_PAYOUTS_REFRESH_INTERVAL


async def _read_recent_payouts(
    client: ShopifyGraphQLClient,
    data_model: type[BaseResponseData[Payouts]],
    store: str,
    capabilities: StoreCapabilities,
    start_date: datetime | None,
    window: timedelta,
    last_refresh: datetime,
    now: datetime,
    log: Logger,
) -> AsyncGenerator[Payouts, None]:
    """Read every payout dated from `window` before the refresh fell due through `now`.

            due - window          due    now
        ──────────────┼───────────────┼──────┼─────▶ issued_at
                      │               │      │
        issued_at:>= ─[═══════════════╪══════╪═════▶
        issued_at:<= ═╪═══════════════╪══════]
        read ─────────[═══════════════╪══════]
                      │               │      └─ minutes after due, or days
                      │               │         after it if the capture
                      │               │         was down
                      │               └─ a day after last_refresh
                      └─ start_date instead, if one is set
                         and it's later

    Both bounds are inclusive. Measured back from `due`, not from `now`, the window still
    reaches every payout dated since the last refresh after the capture was down for longer
    than `window`. `start_date` is None when the config leaves it blank.
    """
    due = last_refresh + RECENT_PAYOUTS_REFRESH_INTERVAL
    start = due - window
    if start_date is not None:
        start = max(start_date, start)

    async for payout in _paginate_through_resources(
        client=client,
        model=Payouts,
        data_model=data_model,
        start=start,
        end=now,
        store=store,
        capabilities=capabilities,
        log=log,
    ):
        yield payout


async def fetch_recent_payouts(
    client: ShopifyGraphQLClient,
    data_model: type[BaseResponseData[Payouts]],
    store: str,
    capabilities: StoreCapabilities,
    start_date: datetime | None,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Payouts | LogCursor, None]:
    """Once a day, emit every payout dated within Payouts.RECENT_PAYOUTS_WINDOW before the
    refresh fell due (see _read_recent_payouts), so a payout's status changes after its date
    are captured.

    The cursor records when the last refresh ran. It decides when the next one is due and how
    far back it reads, and filters nothing. `start_date` is None when the config leaves it
    blank.
    """
    assert isinstance(log_cursor, datetime)
    now = datetime.now(tz=UTC)
    if not _recent_payouts_refresh_is_due(log_cursor, now):
        return

    payout_count = 0
    async for payout in _read_recent_payouts(
        client,
        data_model,
        store,
        capabilities,
        start_date,
        Payouts.RECENT_PAYOUTS_WINDOW,
        log_cursor,
        now,
        log,
    ):
        yield payout
        payout_count += 1

    log.info(f"[{store}] Refreshed {payout_count} recent payouts.")
    yield now


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
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TShopifyGraphQLResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is None:
        start = start_date
    else:
        assert isinstance(page, str)
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


async def backfill_incremental_unsorted(
    client: ShopifyGraphQLClient,
    model: type[TShopifyGraphQLResource],
    data_model: type[BaseResponseData[TShopifyGraphQLResource]],
    store: str,
    capabilities: StoreCapabilities,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TShopifyGraphQLResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is not None:
        assert isinstance(page, str)

    query = model.build_query(
        start_date,
        cutoff,
        first=PAGE_SIZE,
        after=page,
        capabilities=capabilities,
    )
    data = await client.request(
        query, data_model, log, context=StoreValidationContext(store=store)
    )

    for doc in data.nodes:
        yield doc

    page_info = data.page_info
    if page_info.hasNextPage:
        yield page_info.endCursor
