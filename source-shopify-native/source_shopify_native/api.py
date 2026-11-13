import asyncio
import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import (
    AssociatedDocument,
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
from .graphql.payments.balance_transactions import BalanceTransactions
from .graphql.payments.payouts import Payouts
from .utils import dt_to_str, str_to_dt

CHECKPOINT_INTERVAL = 1_000
# Each payouts read looks back 30 days, so it runs once a day instead of every 5 minutes.
PAYOUTS_READ_INTERVAL = timedelta(days=1)
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
        if not page_info.hasNextPage:
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


class PayoutTransactionsMismatchError(RuntimeError):
    """A payout's first page of balance transactions held none of its own."""


# A failed payout's money goes into a later payout. Shopify doesn't say whether a failed or
# canceled payout keeps its transactions, so one that holds none of its own may be correct.
MISMATCH_EXEMPT_PAYOUT_STATUSES = {"FAILED", "CANCELED"}
# The payments_transfer_id search may lag behind writes, and no doc says by how long. A payout
# issued within this period may not show its transactions yet, and they're read again later.
MISMATCH_GRACE_PERIOD = timedelta(days=1)


async def _read_payout_transactions(
    client: ShopifyGraphQLClient,
    data_model: type[BaseResponseData[BalanceTransactions]],
    store: str,
    payout: Payouts,
    log: Logger,
) -> AsyncGenerator[BalanceTransactions, None]:
    """Read every balance transaction the payments_transfer_id filter returns for `payout`,
    including any that now belong to another payout.

    Raises PayoutTransactionsMismatchError before yielding anything if the first page holds
    none of the payout's own transactions, including an empty first page. Shopify doesn't say
    what this filter matches or what form associatedPayout ids take, so we can't assume either
    matches the payout.
    """
    ctx = StoreValidationContext(store=store)
    after: str | None = None
    is_first_page = True

    while True:
        query = BalanceTransactions.build_payout_query(
            payout.legacyResourceId, first=PAGE_SIZE, after=after
        )
        data = await client.request(query, data_model, log, context=ctx)

        if is_first_page and not any(t.associatedPayout.id == payout.id for t in data.nodes):
            held_payout_ids = sorted({str(t.associatedPayout.id) for t in data.nodes})
            raise PayoutTransactionsMismatchError(
                f"The balance transactions read for payout {payout.id} ({payout.status} "
                f"{payout.transactionType}, issued {payout.issuedAt}) held none of its own. Its "
                f"first page held {len(data.nodes)} transaction(s)"
                + (f", of payouts {held_payout_ids[:3]}" if held_payout_ids else "")
                + ". Check that payments_transfer_id matches a payout's legacyResourceId, and "
                "that associatedPayout ids take the form of payout ids."
            )
        is_first_page = False

        for transaction in data.nodes:
            yield transaction

        if not data.page_info.hasNextPage:
            return
        after = data.page_info.endCursor


async def fetch_payouts(
    client: ShopifyGraphQLClient,
    payout_data_model: type[BaseResponseData[Payouts]],
    transaction_data_model: type[BaseResponseData[BalanceTransactions]],
    store: str,
    capabilities: StoreCapabilities,
    start_date: datetime | None,
    balance_transactions_binding: int | None,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Payouts | AssociatedDocument[BalanceTransactions] | LogCursor, None]:
    """Once a day, emit every payout issued since Payouts.STATUS_CHANGE_WINDOW before the last
    read, so status changes after a payout is issued are captured. For each payout issued since
    Payouts.TRANSACTIONS_WINDOW before the last read, also emit its balance transactions to
    `balance_transactions_binding`. That fills in associatedPayout on transactions the
    balance_transactions stream captured before their payout existed.

                 cursor − 30d cursor − 7d  cursor     now
        ───────────────┼───────────┼──────────┼────────┼──▶ issued_at
                       │           │          │        │
        issued_at:>= ──[═══════════╪══════════╪════════╪══▶
        issued_at:<= ══╪═══════════╪══════════╪════════]
        payouts      ──[═══════════╪══════════╪════════]
        transactions ──┼───────────[══════════╪════════]
                       │           │          │        │
                       │           │          │        └─ a day or more after
                       │           │          │           the cursor. Before
                       │           │          │           then, it returns
                       │           │          └─ when the last read started
                       │           └─ payouts issued from here
                       │              on also have their
                       │              transactions read
                       └─ a configured start_date
                          instead, if it's later

    Both bounds are inclusive. The windows start from the cursor rather than from now, so a
    read after downtime still covers every payout issued since the last read. The cursor is
    yielded only after everything is read, so a read that fails partway is retried in full.

    `start_date` is None when the config leaves it blank. A transaction processed before it
    isn't emitted, since the balance_transactions backfill doesn't emit it either.
    `balance_transactions_binding` is None when that binding isn't enabled.

    Raises PayoutTransactionsMismatchError if a payout's transactions hold none of its own,
    unless the payout is failed or canceled (MISMATCH_EXEMPT_PAYOUT_STATUSES) or was issued
    within MISMATCH_GRACE_PERIOD of now.
    """
    assert isinstance(log_cursor, datetime)
    now = datetime.now(tz=UTC)
    if now - log_cursor < PAYOUTS_READ_INTERVAL:
        return

    start = log_cursor - Payouts.STATUS_CHANGE_WINDOW
    if start_date is not None:
        start = max(start, start_date)
    transactions_start = log_cursor - Payouts.TRANSACTIONS_WINDOW

    payout_count = 0
    transaction_count = 0
    async for payout in _paginate_through_resources(
        client=client,
        model=Payouts,
        data_model=payout_data_model,
        start=start,
        end=now,
        store=store,
        capabilities=capabilities,
        log=log,
    ):
        yield payout
        payout_count += 1

        if (
            balance_transactions_binding is None
            or payout.get_cursor_value() < transactions_start
        ):
            continue

        try:
            async for transaction in _read_payout_transactions(
                client, transaction_data_model, store, payout, log
            ):
                if start_date is not None and transaction.get_cursor_value() < start_date:
                    continue

                yield AssociatedDocument(doc=transaction, binding=balance_transactions_binding)
                transaction_count += 1
        except PayoutTransactionsMismatchError:
            is_exempt = payout.status in MISMATCH_EXEMPT_PAYOUT_STATUSES
            is_within_grace_period = now - payout.get_cursor_value() < MISMATCH_GRACE_PERIOD
            if not is_exempt and not is_within_grace_period:
                raise

    log.info(
        f"[{store}] Read {payout_count} payouts issued since {dt_to_str(start)}, and "
        f"{transaction_count} balance transactions in the ones issued since "
        f"{dt_to_str(transactions_start)}."
    )
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
