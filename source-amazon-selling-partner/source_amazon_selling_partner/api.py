import asyncio
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Iterable, cast

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
import ijson  # pyright: ignore[reportMissingTypeStubs]  # no stubs exist for this package

from .client import Operation, Priority, SPAPIClient
from .models import (
    LWA_TOKEN_URL,
    AllOrdersRow,
    CatalogItem,
    SearchCatalogItemsResponse,
    FinancialTransaction,
    ListTransactionsResponse,
    MarketplaceParticipationsResponse,
    Order,
    ProcessingStatus,
    Report,
    SalesAndTrafficByDate,
    SalesAndTrafficSpecification,
    SearchOrdersResponse,
    SettlementReportRow,
)
from .reports import (
    REPORT_RETENTION,
    REPORT_TIMEOUT,
    RETENTION_MARGIN,
    RecentReports,
    ReportMismatch,
    ReportRequest,
    ReportRoleMissing,
    _reused,
    list_reports,
    read_json_report,
    read_report,
    request_report,
)

# Amazon's order timestamps have whole-second resolution.
TICK = timedelta(seconds=1)

# searchOrders and listTransactions reject an upper bound later than two
# minutes before the request. The extra margin absorbs clock skew between us
# and Amazon.
SEARCH_LAG = timedelta(minutes=2, seconds=30)

# The narrowest window an incremental poll requests. Without it, an interval
# shorter than SEARCH_LAG polls continuously and starves the backfill of
# searchOrders' rate budget.
MIN_INCREMENTAL_WINDOW = timedelta(minutes=5)

# How far behind the present the realtime pass over financial transactions
# stays. Amazon says financial events may take up to 48 hours to appear; the
# late pass catches those. This lag only lets transactions that land within
# minutes of their postedDate be read on the first pass.
TRANSACTIONS_REALTIME_LAG = timedelta(minutes=15)

# How far behind the present the late pass over financial transactions
# stays: Amazon's 48 hours, plus a day of margin.
TRANSACTIONS_LATE_LAG = timedelta(hours=72)

# listTransactions returns an empty response for windows longer than this.
MAX_TRANSACTIONS_WINDOW = timedelta(days=180)

# How far each poll for reports reaches back before the previous one ended,
# for reports Amazon lists, or finishes, after their createdTime. How late
# that can be is unverified. A report later than this is missed until the
# binding is backfilled, which reads every report Amazon still keeps.
REPORT_LISTING_OVERLAP = timedelta(days=3)

# The longest data range requested for an order report: a day under
# Amazon's documented 30 days, since what Amazon does with a range at or past
# that limit, a 400 or a CANCELLED read as no data, is unverified.
ALL_ORDERS_MAX_WINDOW = timedelta(days=29)

# How far behind the present all orders windows end. A report's range must
# not end in the future, and this leaves Amazon time to record an update
# before a range ending at it is requested. Amazon's order report page says
# order reports can have up to a 30-minute delay. The lag is exactly that
# delay, and ALL_ORDERS_OVERLAP covers an update recorded at the limit.
ALL_ORDERS_LAG = timedelta(minutes=30)

# How far back the all orders backfill starts at most: a day past the two
# years Amazon's order report page gives ("Order reports only support orders
# that are less than two years old"), which span 731 days when they include
# a 29 February. Reaching past the limit leaves Amazon to enforce it; that
# Amazon accepts a dataStartTime a little over two years back is unverified.
ALL_ORDERS_HISTORY_LIMIT = timedelta(days=731) + timedelta(days=1)

# How far each all orders window reaches back before the previous one
# ended, so the seam is covered whether or not Amazon includes each bound.
ALL_ORDERS_OVERLAP = timedelta(hours=1)

# The shortest all orders interval. Each poll reads the overlap again under
# new report keys, so every line is emitted about 1 + overlap / interval
# times.
ALL_ORDERS_MIN_INTERVAL = timedelta(hours=1)

CATALOG_MIN_INTERVAL = timedelta(hours=3)
CATALOG_INTERVAL_ERROR = (
    f"catalog_items needs an interval of at least {CATALOG_MIN_INTERVAL}: a listings report "
    f"may take up to REPORT_TIMEOUT ({REPORT_TIMEOUT}, 2h), and a restart must land in the "
    "same or the next request bucket to reuse it."
)

# Sales and Traffic is read in whole UTC days.
DAY = timedelta(days=1)

# How many complete days each Sales and Traffic poll reads, the newest new
# and the rest again, in case Amazon revises recent days (assumed, not
# documented). Also the backfill's window; Amazon recommends ranges of 7 to
# 30 days.
SALES_AND_TRAFFIC_WINDOW = timedelta(days=30)

# How far back a Sales and Traffic range starts at most when computed: at
# least a day inside the two years (730 days) Amazon allows, so a request
# sent later that day is inside them too. It cancels a report that starts
# earlier.
SALES_AND_TRAFFIC_HISTORY_LIMIT = timedelta(days=730) - DAY


def now() -> datetime:
    return datetime.now(tz=UTC)


def floor_to_tick(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def ceil_to_tick(dt: datetime) -> datetime:
    floored = floor_to_tick(dt)
    return floored if floored == dt else floored + TICK


def ceil_to_day(dt: datetime) -> datetime:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight if midnight == dt else midnight + timedelta(days=1)


def floor_to_day(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class PaginationTokenRejected(RuntimeError):
    """Amazon rejected a pagination token, most likely because it expired."""


async def _search_orders(
    client: SPAPIClient,
    marketplace_ids: list[str],
    request_params: dict[str, str],
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[Order | SearchOrdersResponse, None]:
    """
    Yields every order matching `request_params` across all pages, then the
    remainder of the final page, whose window bounds Amazon echoes back.
    """
    params: dict[str, Any] = {
        **request_params,
        "marketplaceIds": ",".join(marketplace_ids),
        "includedData": ",".join(Order.INCLUDED_DATA),
        "maxResultsPerPage": Order.PAGE_SIZE,
    }

    while True:
        try:
            _, body = await client.get(
                log, Operation.SEARCH_ORDERS, Order.PATH, params, priority
            )
        except HTTPError as err:
            # The access token is refreshed inside this request, so a 400 from
            # Login with Amazon must not be mistaken for a rejected page token.
            if (
                err.code == 400
                and "paginationToken" in params
                and LWA_TOKEN_URL not in err.message
            ):
                raise PaginationTokenRejected(
                    "Amazon rejected an order search pagination token. Tokens expire 24 hours "
                    "after the first page of a window. The connector already tried smaller windows; "
                    "reduce the Window Size so each window "
                    f"pages through faster.\n\n{err.message}"
                ) from err
            raise

        processor = IncrementalJsonProcessor(
            body(),
            "orders.item",
            Order,
            SearchOrdersResponse,
        )
        async for order in processor:
            yield order

        remainder = processor.get_remainder()
        next_token = remainder.pagination.nextToken if remainder.pagination else None
        if not next_token:
            yield remainder
            return

        params["paginationToken"] = next_token


async def fetch_orders(
    client: SPAPIClient,
    marketplace_ids: list[str],
    window_size: timedelta,
    overlap: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Order | LogCursor, None]:
    """
    Captures orders updated since the cursor, one window per invocation.
    Rejected pagination tokens halve the window, down to a one-hour floor.

    ```
          cursor+1s−overlap  cursor+1s        window_end ≤ horizon
    ──────────────┼──────────────┼────────────────────┼──▶ time (1s ticks)
                  │              │                    │
    lastUpdatedAfter ════════════╪════════════════════╪═══▶
    lastUpdatedBefore ═══════════╪════════════════════]
    emitted ──────[══════════════╪════════════════════]
                  │              │                    └─ new cursor
                  │              └─ first tick no earlier poll covered
                  └─ re-reads the overlap, for orders Amazon makes
                     searchable after their lastUpdatedTime
    ```

    Both bounds are inclusive. `horizon` is the last whole second at least
    SEARCH_LAG before now, since searchOrders rejects later upper bounds. A
    window narrower than MIN_INCREMENTAL_WINDOW waits for the next poll.
    That Amazon indexes some orders after their lastUpdatedTime, which the
    overlap exists for, is unverified.
    """
    assert isinstance(log_cursor, datetime)
    cursor = log_cursor

    horizon = floor_to_tick(now() - SEARCH_LAG)
    if horizon - cursor < MIN_INCREMENTAL_WINDOW:
        return

    window_end = min(cursor + window_size, horizon)
    while True:
        request_params = {
            "lastUpdatedAfter": to_iso(cursor + TICK - overlap),
            "lastUpdatedBefore": to_iso(window_end),
        }

        count = 0
        try:
            async for item in _search_orders(
                client, marketplace_ids, request_params, Priority.INCREMENTAL, log
            ):
                if isinstance(item, Order):
                    count += 1
                    yield item
                elif item.lastUpdatedBefore is not None and item.lastUpdatedBefore < window_end:
                    raise RuntimeError(
                        f"Amazon searched orders updated only up to {item.lastUpdatedBefore}, "
                        f"not the requested {window_end}."
                    )
        except PaginationTokenRejected:
            smaller_end = floor_to_tick(cursor + (window_end - cursor) / 2)
            if smaller_end - cursor < timedelta(hours=1):
                raise
            log.warning(
                "shrinking an orders window after Amazon rejected its pagination token",
                {"window_start": cursor, "window_end": window_end, "new_window_end": smaller_end},
            )
            # Re-emitting orders is safe: the collection reduces by lastUpdatedTime.
            window_end = smaller_end
            continue
        break

    log.debug(
        "fetched orders window",
        {"window_start": cursor, "window_end": window_end, "count": count},
    )
    yield window_end


async def backfill_orders(
    client: SPAPIClient,
    marketplace_ids: list[str],
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Order | PageCursor, None]:
    """
    Captures orders created before the cutoff, as they are now, one window
    of creation time per invocation. The page cursor is the next window's
    start, as an ISO 8601 string.
    Rejected pagination tokens halve the window, down to a one-hour floor.

    ```
                window_start      window_end ≤ cutoff−1s   cutoff
    ──────────────────┼──────────────────────┼──────────────┼──▶ time (1s)
                      │                      │              │
    createdAfter ─────[══════════════════════╪══════════════╪═══▶
    createdBefore ════╪══════════════════════]              │
    emitted ──────────[══════════════════════]              │
                      │                      │              └─ incremental
                      │                      │                 starts here
                      │                      └─ next window starts 1s later
                      └─ start_date, or the previous window's end + 1s
    ```

    Both bounds are inclusive. Creation time never changes, so a window's
    membership is fixed and resuming from its start is safe. Nothing is
    checkpointed until a window is fully paged, so a restart mid-window
    re-requests it from its first page. Orders updated
    after the cutoff are captured again by the incremental task. A window can
    commit after the incremental task has committed a newer copy of one of its
    orders; the collection keeps the copy with the latest `lastUpdatedTime`,
    and on equal `lastUpdatedTime` deep-merges the copies, so a field
    missing from the later copy is kept.
    """
    assert isinstance(cutoff, datetime)
    last_tick = cutoff - TICK

    if page is None:
        window_start = ceil_to_tick(start_date)
    else:
        assert isinstance(page, str)
        window_start = datetime.fromisoformat(page)

    if window_start > last_tick:
        return

    window_end = min(window_start + window_size - TICK, last_tick)

    # A cutoff from the last few minutes is too recent for searchOrders' upper
    # bound. Wait it out instead of narrowing the window. The wait can't be
    # interrupted, so stopping a fresh capture may take up to SEARCH_LAG.
    wait = (window_end + SEARCH_LAG - now()).total_seconds()
    if wait > 0:
        await asyncio.sleep(wait)

    while True:
        request_params = {
            "createdAfter": to_iso(window_start),
            "createdBefore": to_iso(window_end),
        }

        count = 0
        try:
            async for item in _search_orders(
                client, marketplace_ids, request_params, Priority.BACKFILL, log
            ):
                if isinstance(item, Order):
                    count += 1
                    yield item
                elif item.createdBefore is not None and item.createdBefore < window_end:
                    raise RuntimeError(
                        f"Amazon searched orders created only up to {item.createdBefore}, "
                        f"not the requested {window_end}."
                    )
        except PaginationTokenRejected:
            smaller_end = floor_to_tick(window_start + (window_end - window_start) / 2)
            if smaller_end - window_start < timedelta(hours=1):
                raise
            log.warning(
                "shrinking an orders window after Amazon rejected its pagination token",
                {"window_start": window_start, "window_end": window_end, "new_window_end": smaller_end},
            )
            # Re-emitting orders is safe: the collection reduces by lastUpdatedTime.
            window_end = smaller_end
            continue
        break

    log.debug(
        "backfilled orders window",
        {"window_start": window_start, "window_end": window_end, "count": count},
    )

    if window_end < last_tick:
        yield to_iso(window_end + TICK)


async def _list_transactions(
    client: SPAPIClient,
    request_params: dict[str, str],
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[FinancialTransaction, None]:
    """
    Yields every transaction matching `request_params` across all pages,
    including pages Amazon returns empty with a nextToken. Every region
    marketplace is included, since filtering by marketplaceId drops US
    merchant-fulfilled orders.
    """
    params: dict[str, Any] = dict(request_params)

    while True:
        _, body = await client.get(
            log, Operation.LIST_TRANSACTIONS, FinancialTransaction.PATH, params, priority
        )

        processor = IncrementalJsonProcessor(
            body(),
            FinancialTransaction.ITEMS_PREFIX,
            FinancialTransaction,
            ListTransactionsResponse,
        )
        async for transaction in processor:
            yield transaction

        next_token = processor.get_remainder().payload.nextToken
        if not next_token:
            return

        # A next page must repeat the arguments of the request that produced it.
        params = {**request_params, "nextToken": next_token}


async def _transactions_window(
    client: SPAPIClient,
    window_start: datetime,
    window_end: datetime,
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[FinancialTransaction, None]:
    assert window_end - window_start <= MAX_TRANSACTIONS_WINDOW
    request_params = {
        FinancialTransaction.SINCE_PARAM: to_iso(window_start),
        FinancialTransaction.BEFORE_PARAM: to_iso(window_end),
    }

    count = 0
    async for transaction in _list_transactions(client, request_params, priority, log):
        count += 1
        yield transaction

    log.debug(
        "fetched financial transactions window",
        {"window_start": window_start, "window_end": window_end, "count": count},
    )


async def fetch_financial_transactions(
    client: SPAPIClient,
    lag: timedelta,
    window_size: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FinancialTransaction | LogCursor, None]:
    """
    Captures transactions posted since the cursor, one window per invocation,
    staying `lag` behind the present. The realtime subtask runs with
    TRANSACTIONS_REALTIME_LAG; the late subtask re-reads the same time with
    TRANSACTIONS_LATE_LAG, catching transactions Amazon posts late; the
    lookback subtask reads it a final time with the configured lookback,
    catching deferred transactions it has since released.

    ```
                   cursor      window_end − ε  window_end ≤ horizon
    ─────────────────┼───────────────┼──────────┼──────▶ time
                     │               │          │
    postedAfter ─────[═══════════════╪══════════╪══════▶
    postedBefore ════╪═══════════════╪══════════)
    emitted ─────────[═══════════════]          │
                     │               │          └─ new cursor, excluded
                     │               │             here and read next poll
                     │               └─ last postedDate collected
                     └─ the previous window's excluded end
    ```

    postedAfter inclusive, postedBefore exclusive; windows meet at window_end.

    `horizon` is the last whole second at least `lag` before now, and ε is
    postedDate's precision. A window narrower than MIN_INCREMENTAL_WINDOW
    waits for the next poll.

    Unverified: whether releasing a deferred transaction keeps its
    postedDate. If it does, a release is captured only when the lookback
    reaches it, so a release after its maturityDate (DeferredContext) falls
    outside a lookback shorter than that; if it moves postedDate forward,
    the realtime pass captures it.

    Unverified: that leaving out transactionStatus returns DEFERRED,
    RELEASED and DEFERRED_RELEASED transactions alike. The model file lists
    the filter as optional and gives no default. If Amazon applies one,
    each status must be requested per window instead.
    """
    assert isinstance(log_cursor, datetime)
    cursor = log_cursor

    horizon = floor_to_tick(now() - max(lag, SEARCH_LAG))
    if horizon - cursor < MIN_INCREMENTAL_WINDOW:
        return

    window_end = min(cursor + min(window_size, MAX_TRANSACTIONS_WINDOW), horizon)
    async for transaction in _transactions_window(
        client, cursor, window_end, Priority.INCREMENTAL, log
    ):
        yield transaction

    yield window_end


async def backfill_financial_transactions(
    client: SPAPIClient,
    start_date: datetime,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[FinancialTransaction | PageCursor, None]:
    """
    Captures transactions posted before the cutoff, one window of posting
    time per invocation. The page cursor is the next window's start, as an
    ISO 8601 string.

    ```
               window_start    window_end − ε  window_end ≤ cutoff
    ─────────────────┼───────────────┼──────────┼──────▶ time
                     │               │          │
    postedAfter ─────[═══════════════╪══════════╪══════▶
    postedBefore ════╪═══════════════╪══════════)
    emitted ─────────[═══════════════]          │
                     │               │          └─ the next window's start;
                     │               │             incremental starts at
                     │               │             the cutoff
                     │               └─ last postedDate collected
                     └─ start_date, or the previous window's end
    ```

    postedAfter inclusive, postedBefore exclusive; windows meet at window_end.

    ε is postedDate's precision. Nothing is checkpointed until a
    window is fully paged, so a restart mid-window re-requests it from its
    first page. Transactions posted late into the backfilled range are left
    to the late and lookback subtasks, which start their passes their lag
    before the cutoff.
    """
    assert isinstance(cutoff, datetime)

    if page is None:
        window_start = floor_to_tick(start_date)
    else:
        assert isinstance(page, str)
        window_start = datetime.fromisoformat(page)

    if window_start >= cutoff:
        return

    window_end = min(window_start + min(window_size, MAX_TRANSACTIONS_WINDOW), cutoff)

    # A window ending near the cutoff is read no sooner than the realtime pass
    # would read it, so transactions landing minutes after their postedDate
    # are not left to the late pass. This also keeps listTransactions' upper
    # bound far enough in the past. The wait can't be interrupted, so
    # stopping a fresh capture may take up to TRANSACTIONS_REALTIME_LAG.
    wait = (window_end + TRANSACTIONS_REALTIME_LAG - now()).total_seconds()
    if wait > 0:
        await asyncio.sleep(wait)

    async for transaction in _transactions_window(
        client, window_start, window_end, Priority.BACKFILL, log
    ):
        yield transaction

    if window_end < cutoff:
        yield to_iso(window_end)


def _retention_floor() -> datetime:
    """The earliest createdSince that still lies inside Amazon's report retention."""
    return ceil_to_tick(now() - REPORT_RETENTION + RETENTION_MARGIN)


async def _done_reports(
    client: SPAPIClient,
    report_type: str,
    since: datetime,
    until: datetime,
    priority: Priority,
    log: Logger,
) -> list[Report]:
    """
    Returns the DONE reports of `report_type` created from `since` through
    `until`, both inclusive, oldest first, with reportId breaking ties.

    The bounds are also applied here, since the inclusivity of getReports'
    createdSince and createdUntil is unverified.
    """
    request_params = {
        "reportTypes": report_type,
        "processingStatuses": ProcessingStatus.DONE.value,
        Report.SINCE_PARAM: to_iso(since),
        Report.BEFORE_PARAM: to_iso(ceil_to_tick(until)),
    }
    reports = await list_reports(client, request_params, priority, log)
    return sorted(
        (r for r in reports if since <= r.createdTime <= until),
        key=lambda r: (r.createdTime, r.reportId),
    )


async def _report_rows(
    client: SPAPIClient,
    report: Report,
    own_fields: Iterable[str],
    priority: Priority,
    log: Logger,
    empty_is_routine: bool = False,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    Yields each data row of a DONE report as its cells plus the report's
    metadata and the row's 1-based rowNumber. A file with a column named
    like one of `own_fields` fails rather than overwriting or being
    overwritten.
    """
    row_number = 0
    async for document, cells in read_report(
        client, report, priority, log, empty_is_routine=empty_is_routine
    ):
        row_number += 1
        if row_number == 1:
            clashing = set(cells) & set(own_fields)
            if clashing:
                raise RuntimeError(
                    f"report {report.reportId} ({report.reportType}) has columns named "
                    f"like the connector's own fields: {sorted(clashing)}"
                )

        yield {
            **cells,
            "reportId": report.reportId,
            "reportDocumentId": document.reportDocumentId,
            "reportCreatedTime": report.createdTime,
            "dataStartTime": report.dataStartTime,
            "dataEndTime": report.dataEndTime,
            "marketplaceIds": report.marketplaceIds,
            "rowNumber": row_number,
        }


async def _settlement_rows(
    client: SPAPIClient,
    report: Report,
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[SettlementReportRow, None]:
    async for fields in _report_rows(
        client, report, SettlementReportRow.model_fields, priority, log
    ):
        yield SettlementReportRow.model_validate(
            {**fields, "summaryRow": fields["rowNumber"] == 1}
        )


async def fetch_settlement_reports(
    client: SPAPIClient,
    recent: RecentReports,
    start_date: datetime,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SettlementReportRow | LogCursor, None]:
    """
    Captures the rows of settlement reports Amazon has created and finished
    since the cursor. Amazon generates these itself; they are never requested.

    ```
                   since            cursor+1s            horizon
    ─────────────────┼──────────────────┼───────────────────┼─▶ time (1s)
                     │                  │                   │
    createdSince ────[══════════════════╪═══════════════════╪══▶
    createdUntil ════╪══════════════════╪═══════════════════]
    emitted ─────────[══════════════════╪═══════════════════]
                     │                  │                   └─ new cursor
                     │                  └─ first second no earlier
                     │                     poll listed
                     └─ cursor+1s−overlap, raised to start_date, but
                        not past cursor+1s, and to 90 days ago; ones
                        this process read are skipped
    ```

    The overlap lists reports again that Amazon lists, or finishes, late.
    Both bounds are applied inclusively here, whatever Amazon's are; the
    overlap makes the seam between polls independent of them. start_date
    never skips reports created after the cursor: a blank start_date moves
    forward with each restart, and as with orders, reports created after
    the cutoff and before a later start_date are captured.

    The cursor asserts that every DONE report created up to it that was
    listed when it was saved has been read in full. createdTime never
    changes, so that holds across any restart gap; a restart re-reads the
    overlap, whose rows overwrite themselves by key. A report listed more
    than the overlap after its createdTime is missed here; backfilling the
    binding reads it while Amazon still keeps it. Whether Amazon ever lists
    a settlement report that late is unverified.
    createdSince never reaches past Amazon's 90-day retention.
    """
    assert isinstance(log_cursor, datetime)
    cursor = log_cursor

    horizon = floor_to_tick(now()) - TICK
    if horizon <= cursor:
        return

    since = max(
        cursor + TICK - REPORT_LISTING_OVERLAP,
        min(floor_to_tick(start_date), cursor + TICK),
    )
    if since < (floor := _retention_floor()):
        log.warning(
            "settlement reports before Amazon's 90-day retention are gone",
            {"cursor": cursor, "listed_from": floor},
        )
        since = floor

    reports = await _done_reports(
        client, SettlementReportRow.REPORT_TYPE, since, horizon, Priority.INCREMENTAL, log
    )
    recent.forget_before(since)

    for report in reports:
        if report in recent:
            continue
        async for row in _settlement_rows(client, report, Priority.INCREMENTAL, log):
            yield row
        recent.add(report)

    yield horizon


async def backfill_settlement_reports(
    client: SPAPIClient,
    recent: RecentReports,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[SettlementReportRow | PageCursor, None]:
    """
    Captures the rows of settlement reports created before the cutoff and
    still kept by Amazon. The page cursor is the createdTime, as an ISO 8601
    string, through which every listed report has been read in full.

    ```
                   since      watermark          cutoff−1s  cutoff
    ─────────────────┼────────────┼──────────────────┼─────────┼──▶ time
                     │            │                  │         │
    createdSince ────[════════════╪══════════════════╪═════════╪═▶
    createdUntil ════╪════════════╪══════════════════]         │
    emitted ─────────┼────────────(══════════════════]         │
                     │            │                  │         └─ incremental
                     │            │                  │            starts here
                     │            │                  └─ last createdTime read
                     │            └─ the page cursor; on the first call,
                     │               every report from since is read
                     └─ start_date, 90 days ago if later, or
                        the watermark floored to the second
    ```

    Reports are read oldest first. A cursor is saved only once every
    report sharing a createdTime is read, so resuming after it, by an
    immutable value, skips nothing; createdTime's precision is unverified,
    so no tick arithmetic is applied to it. A restart mid-report reads that
    report again from its first row. The incremental task re-reads the
    overlap before the cutoff, so the seam is covered twice.
    """
    assert isinstance(cutoff, datetime)
    last_tick = cutoff - TICK

    watermark: datetime | None = None
    if page is None:
        since = floor_to_tick(start_date)
    else:
        assert isinstance(page, str)
        watermark = datetime.fromisoformat(page)
        since = floor_to_tick(watermark)

    since = max(since, _retention_floor())
    if since > last_tick:
        return

    reports = await _done_reports(
        client, SettlementReportRow.REPORT_TYPE, since, last_tick, Priority.BACKFILL, log
    )
    if watermark is not None:
        reports = [r for r in reports if r.createdTime > watermark]

    for i, report in enumerate(reports):
        if report not in recent:
            async for row in _settlement_rows(client, report, Priority.BACKFILL, log):
                yield row
            recent.add(report)

        following = reports[i + 1] if i + 1 < len(reports) else None
        if following is not None and following.createdTime != report.createdTime:
            yield report.createdTime.isoformat()

    log.debug(
        "backfilled settlement reports",
        {"window_start": since, "window_end": last_tick, "count": len(reports)},
    )


async def _all_orders_window(
    client: SPAPIClient,
    request: ReportRequest,
    report: Report | None,
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[AllOrdersRow, None]:
    """
    Yields every row of `report`, the order report `request_report` gave
    for `request`, requested for all its marketplaces at once. The reports
    model says a report covers all specified marketplaces unless its type
    says otherwise, and the order page names no exception. A real account
    has not yet confirmed this. A marketplace left out would not fail. A
    range Amazon cancelled twice, with no report, yields nothing.
    """
    count = 0
    if report is not None:
        async for fields in _report_rows(
            client, report, AllOrdersRow.model_fields, priority, log, empty_is_routine=True
        ):
            count += 1
            # Every column is kept, the buyer's ship-city, ship-state,
            # ship-postal-code, ship-country and cpf (Brazilian taxpayer
            # number) included. Users leave out fields they don't want with the
            # materialization's field selection.
            yield AllOrdersRow.model_validate(fields)

    log.debug(
        "fetched all orders window",
        {"window_start": request.data_start, "window_end": request.data_end, "count": count},
    )


async def fetch_all_orders(
    client: SPAPIClient,
    marketplace_ids: list[str],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AllOrdersRow | LogCursor, None]:
    """
    Captures every line of every order last updated since the cursor, from
    one requested order report per invocation.

    ```
               cursor−overlap  cursor          window_end ≤ horizon
    ──────────────────┼──────────────┼────────────────────┼──▶ time (1s)
                      │              │                    │
    dataStartTime ────[══════════════╪════════════════════╪═══▶
    dataEndTime ══════╪══════════════╪════════════════════]
    emitted ──────────[══════════════╪════════════════════]
                      │              │                    └─ new cursor
                      │              └─ the previous window's end
                      └─ re-read, so the seam is covered whichever
                         way Amazon treats each bound
    ```

    Bounds drawn inclusive; their inclusivity is unverified.

    `horizon` is the last whole second at least ALL_ORDERS_LAG before now.
    A window spans at most ALL_ORDERS_MAX_WINDOW, so after a long pause the
    cursor catches up over several invocations. A window narrower than
    MIN_INCREMENTAL_WINDOW waits for the next poll.

    Every row of the report is emitted, so an order updated in the overlap
    is emitted again from the newer report, under that report's keys. A
    restart before the new cursor is saved reuses the report requested
    before, if requested within REUSE_LOOKBACK: one of the same start that
    ends after the cursor, though the horizon has moved on. It is waited on
    or read again, under the same keys, and its end is the new cursor.
    """
    assert isinstance(log_cursor, datetime)
    cursor = log_cursor

    horizon = floor_to_tick(now() - ALL_ORDERS_LAG)
    if horizon - cursor < MIN_INCREMENTAL_WINDOW:
        return

    window_start = cursor - ALL_ORDERS_OVERLAP
    request = ReportRequest(
        AllOrdersRow.REPORT_TYPE,
        tuple(marketplace_ids),
        window_start,
        min(window_start + ALL_ORDERS_MAX_WINDOW, horizon),
        data_end_after=cursor,
    )
    report = await request_report(client, request, Priority.INCREMENTAL, log)
    async for row in _all_orders_window(client, request, report, Priority.INCREMENTAL, log):
        yield row

    # A reused report may end before the request does. Its end is the new
    # cursor; ReportRequest.made has checked it is set.
    window_end = request.data_end if report is None else report.dataEndTime
    assert window_end is not None
    yield window_end


async def backfill_all_orders(
    client: SPAPIClient,
    marketplace_ids: list[str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[AllOrdersRow | PageCursor, None]:
    """
    Captures every line of every order last updated from start_date, or
    ALL_ORDERS_HISTORY_LIMIT before the request if later, to the cutoff, from
    one requested order report per invocation. The page cursor is the next
    window's start, as an ISO 8601 string.

    ```
                window_start                 window_end ≤ cutoff
    ──────────────────┼────────────────────────────┼──▶ time (1s)
                      │                            │
    dataStartTime ────[════════════════════════════╪═══▶
    dataEndTime ══════╪════════════════════════════]
    emitted ──────────[════════════════════════════]
                      │                            └─ the next window, and
                      │                               incremental, start
                      │                               the overlap before
                      └─ start_date, or the previous window's end
                         less the overlap
    ```

    Bounds drawn inclusive; their inclusivity is unverified.

    Windows span ALL_ORDERS_MAX_WINDOW and overlap by ALL_ORDERS_OVERLAP,
    so no seam depends on how Amazon treats the bounds. The cutoff is at
    least ALL_ORDERS_LAG in the past, so no window waits. A report reads
    orders as they are when it is made: one updated after its window
    appears again, as updated, in a later window or the incremental task's.
    A restart mid-window requests the same window again; the report
    requested before it is reused if it was requested within REUSE_LOOKBACK,
    so at most one window's rows are read again, under the same keys; after
    a longer gap the window is requested again and its rows come back under
    new report ids. The first window's start, when ALL_ORDERS_HISTORY_LIMIT
    moves it, changes at UTC midnight: a restart in that window across
    midnight requests it again, and its rows come back under new report ids.
    """
    assert isinstance(cutoff, datetime)

    if page is None:
        window_start = floor_to_tick(start_date)
    else:
        assert isinstance(page, str)
        window_start = datetime.fromisoformat(page)

    # Measured from now, not the cutoff: a restart may come long after it.
    # Snapped to a UTC midnight, so a restart the same day requests the same
    # first window and reuses its report.
    history_start = ceil_to_day(now() - ALL_ORDERS_HISTORY_LIMIT)
    if window_start < history_start:
        log.warning(
            "Amazon serves order reports for about two years only, so all orders starts later than requested",
            {"start_date": start_date, "window_start": history_start},
        )
        window_start = history_start

    if window_start >= cutoff:
        return

    request = ReportRequest(
        AllOrdersRow.REPORT_TYPE,
        tuple(marketplace_ids),
        window_start,
        min(window_start + ALL_ORDERS_MAX_WINDOW, cutoff),
    )
    report = await request_report(client, request, Priority.BACKFILL, log)
    async for row in _all_orders_window(client, request, report, Priority.BACKFILL, log):
        yield row

    if request.data_end < cutoff:
        yield to_iso(request.data_end - ALL_ORDERS_OVERLAP)


def _sales_and_traffic_history_start() -> datetime:
    """The earliest day Amazon serves Sales and Traffic for, with at least a day's margin."""
    return ceil_to_day(now() - SALES_AND_TRAFFIC_HISTORY_LIMIT)


class _ChunkReader:
    """The file-like reader ijson.parse_async takes, over a stream of byte chunks."""

    def __init__(self, chunks: AsyncGenerator[bytes, None]):
        self.chunks = chunks

    async def read(self, size: int) -> bytes:
        if size == 0:
            return b""
        async for chunk in self.chunks:
            if chunk:
                return chunk
        return b""


async def _sales_and_traffic_rows(
    client: SPAPIClient,
    request: ReportRequest,
    report: Report,
    priority: Priority,
    log: Logger,
) -> list[SalesAndTrafficByDate]:
    """
    The by-date entries of `report`, the Sales and Traffic report
    `request_report` gave for `request`. A report whose specification names
    other options, marketplaces or dates than `request`, as a report reused
    from getReports can, or that has a day twice, fails before any entry is
    returned, as does one without a salesAndTrafficByDate section.

    Only the specification and the by-date entries are kept. The rest of the
    document, mostly the by-ASIN section, which grows with the seller's
    catalog, is skipped as it is read.
    """
    document = await read_json_report(client, report, priority, log)
    # ijson directly rather than IncrementalJsonProcessor, which keeps
    # everything outside its prefix, the by-ASIN section included, in memory
    # for get_remainder().
    spec_builder = ijson.ObjectBuilder()
    entry: ijson.ObjectBuilder | None = None
    has_dates = False
    rows: dict[str, SalesAndTrafficByDate] = {}
    async for prefix, event, value in ijson.parse_async(_ChunkReader(document), use_float=True):
        if prefix == SalesAndTrafficByDate.ITEMS_PREFIX and event == "start_map":
            entry = ijson.ObjectBuilder()
        if entry is not None:
            entry.event(event, value)
            if prefix == SalesAndTrafficByDate.ITEMS_PREFIX and event == "end_map":
                # An entry names neither its marketplace nor its report.
                row = SalesAndTrafficByDate.model_validate(
                    {
                        **cast(dict[str, Any], entry.value),
                        "marketplaceId": request.marketplace_ids[0],
                        "reportId": report.reportId,
                        "reportCreatedTime": report.createdTime,
                    }
                )
                entry = None
                if row.date in rows:
                    raise RuntimeError(
                        f"Sales and Traffic report {report.reportId} has more than one entry for {row.date}."
                    )
                rows[row.date] = row
        elif prefix == "reportSpecification" or prefix.startswith("reportSpecification."):
            spec_builder.event(event, value)
        elif prefix == "salesAndTrafficByDate" and event == "start_array":
            has_dates = True

    if not has_dates:
        raise RuntimeError(
            f"Sales and Traffic report {report.reportId} has no salesAndTrafficByDate section."
        )
    spec = SalesAndTrafficSpecification.model_validate(spec_builder.value)
    if (
        spec.reportOptions != request.report_options
        or sorted(spec.marketplaceIds) != sorted(request.marketplace_ids)
        or spec.dataStartTime != request.data_start.date()
        or spec.dataEndTime != request.data_end.date()
    ):
        raise ReportMismatch(
            f"Sales and Traffic report {report.reportId} covers {spec.marketplaceIds} from "
            f"{spec.dataStartTime} to {spec.dataEndTime} with options {spec.reportOptions}, "
            f"not the requested {list(request.marketplace_ids)} from "
            f"{request.data_start.date()} to {request.data_end.date()} with options "
            f"{request.report_options}."
        )
    return list(rows.values())


async def _sales_and_traffic_window(
    client: SPAPIClient,
    marketplace_ids: list[str],
    start: datetime,
    end: datetime,
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[SalesAndTrafficByDate, None]:
    """
    Yields every by-date entry of the UTC days from `start` up to `end`,
    from one requested report per marketplace, since a report of this type
    covers exactly one. A marketplace whose report Amazon cancels twice
    yields nothing.
    """
    for marketplace_id in marketplace_ids:
        request = ReportRequest(
            SalesAndTrafficByDate.REPORT_TYPE,
            (marketplace_id,),
            start,
            end - DAY,
            report_options=SalesAndTrafficByDate.REPORT_OPTIONS,
        )
        report = await request_report(client, request, priority, log)
        rows = (
            []
            if report is None
            else await _sales_and_traffic_rows(client, request, report, priority, log)
        )
        for row in rows:
            yield row

        log.debug(
            "fetched sales and traffic window",
            {
                "marketplace_id": marketplace_id,
                "window_start": start,
                "window_end": end,
                "count": len(rows),
            },
        )


async def fetch_sales_and_traffic_by_date(
    client: SPAPIClient,
    marketplace_ids: list[str],
    start_date: datetime,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SalesAndTrafficByDate | LogCursor, None]:
    """
    Captures each marketplace's daily sales and traffic totals for the last
    SALES_AND_TRAFFIC_WINDOW of complete UTC days, from one requested report
    per marketplace, once a new day is complete. The cursor is the first day
    not yet read.

    ```
                 start       cursor    end − 1 day   end
    ───────────────┼────────────┼───────────┼─────────┼──▶ time (days)
                   │            │           │         │
    dataStartTime ─[════════════╪═══════════╪═════════╪══▶
    dataEndTime ═══╪════════════╪═══════════]         │
    emitted ───────[════════════╪═══════════]         │
                   │            │           │         └─ new cursor; this day
                   │            │           │            is not over
                   │            │           └─ the last complete day
                   │            └─ the days before it are read again,
                   │               in case Amazon revises them
                   └─ end − 30 days, or start_date's day, but not
                      past the cursor, or the two-year limit if later
    ```

    Bounds are dates, both inclusive: Amazon ignores the time of day.
    start_date never skips days from the cursor on: a blank start_date
    moves forward with each restart, and as with orders, days after the
    cutoff and before a later start_date are captured.

    Once caught up, each poll reads the last SALES_AND_TRAFFIC_WINDOW of
    complete days again, and the copy from the newest report is kept. A capture paused for
    longer than the window catches up a window per invocation. A range
    Amazon cancels twice is skipped, with a warning: the other polls that
    cover its days still read them, but a window skipped while catching up
    stays missing until the binding is backfilled again. A restart reads at
    most one window per marketplace again.
    """
    assert isinstance(log_cursor, datetime)
    cursor = log_cursor

    end = min(cursor + SALES_AND_TRAFFIC_WINDOW, floor_to_day(now()))
    if end <= cursor:
        return

    start = max(
        end - SALES_AND_TRAFFIC_WINDOW,
        min(floor_to_day(start_date), cursor),
        _sales_and_traffic_history_start(),
    )
    # Only a cursor more than two years old leaves no day to read.
    if start < end:
        async for row in _sales_and_traffic_window(
            client, marketplace_ids, start, end, Priority.INCREMENTAL, log
        ):
            yield row

    yield end


async def backfill_sales_and_traffic_by_date(
    client: SPAPIClient,
    marketplace_ids: list[str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[SalesAndTrafficByDate | PageCursor, None]:
    """
    Captures each marketplace's daily sales and traffic totals from
    start_date's UTC day, or SALES_AND_TRAFFIC_HISTORY_LIMIT before today if
    later, up to the cutoff, a UTC midnight, from one requested report per
    marketplace per invocation. The page cursor is the next window's start,
    as an ISO 8601 string.

    ```
             window_start    window_end − 1 day  window_end
    ───────────────┼──────────────────┼───────────────┼──▶ time (days)
                   │                  │               │
    dataStartTime ─[══════════════════╪═══════════════╪══▶
    dataEndTime ═══╪══════════════════]               │
    emitted ───────[══════════════════]               │
                   │                  │               └─ the next window, or
                   │                  │                  incremental at the
                   │                  │                  cutoff, starts here
                   │                  └─ the window's last day
                   └─ start_date's day or the two-year limit, if
                      later, or the previous window's end
    ```

    Bounds are dates, both inclusive: Amazon ignores the time of day.

    Windows span SALES_AND_TRAFFIC_WINDOW and end at the cutoff, the first
    day incremental reads; its first poll reads the 29 days before it
    again. A range Amazon cancels twice is skipped, with a warning: its days
    within the 29 days before the cutoff are read by that poll, and an
    earlier day, including the 30th before the cutoff, stays missing until
    the binding is backfilled again. A restart reads at most one window per
    marketplace again.
    """
    assert isinstance(cutoff, datetime)

    if page is None:
        window_start = floor_to_day(start_date)
    else:
        assert isinstance(page, str)
        window_start = datetime.fromisoformat(page)

    history_start = _sales_and_traffic_history_start()
    if window_start < history_start:
        log.warning(
            "Amazon serves Sales and Traffic for about two years only, so sales_and_traffic_by_date starts later than requested",
            {"start_date": start_date, "window_start": history_start},
        )
        window_start = history_start

    if window_start >= cutoff:
        return

    window_end = min(window_start + SALES_AND_TRAFFIC_WINDOW, cutoff)
    async for row in _sales_and_traffic_window(
        client, marketplace_ids, window_start, window_end, Priority.BACKFILL, log
    ):
        yield row

    if window_end < cutoff:
        yield to_iso(window_end)


async def fetch_marketplace_participations(
    client: SPAPIClient,
    log: Logger,
) -> MarketplaceParticipationsResponse:
    _, body = await client.get(
        log,
        Operation.GET_MARKETPLACE_PARTICIPATIONS,
        "/sellers/v1/marketplaceParticipations",
        {},
        Priority.INCREMENTAL,
    )
    chunks = [chunk async for chunk in body()]
    return MarketplaceParticipationsResponse.model_validate_json(b"".join(chunks))


async def fetch_catalog_items(
    client: SPAPIClient,
    marketplace_ids: list[str],
    interval: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CatalogItem | LogCursor, None]:
    """Re-emit all listed ASIN summaries after each binding interval.

    The cursor is the start of the last completed refresh. It advances only
    after all marketplaces finish, so an interrupted refresh is retried.
    A failure in any marketplace fails the refresh, and the next attempt redoes
    every marketplace. A report for the same or previous request bucket is
    reused within the retention-bounded lookback, only if its bucket is later
    than the last completed refresh's bucket. refreshedAt is the refresh start,
    not the snapshot time.
    """
    assert isinstance(log_cursor, datetime)
    if interval <= timedelta():
        raise ValueError(CATALOG_INTERVAL_ERROR)
    started = now()
    if started - log_cursor < interval:
        return
    # Stable bucket times let restarts reuse reports already requested.
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    report_time = (epoch + ((started - epoch) // interval) * interval).replace(
        microsecond=0
    )
    # A prior completed refresh must never supply the next refresh's snapshot.
    previous_time = report_time - interval
    completed_bucket = epoch + ((log_cursor - epoch) // interval) * interval
    earliest_created = previous_time if previous_time > completed_bucket else report_time
    # Allow clock skew in Amazon's createdTime before the retention clamp.
    # Excluding the last completed refresh still relies on the dataStartTime
    # match and the previous_time > completed_bucket guard below.
    earliest_created -= RETENTION_MARGIN
    for marketplace_id in marketplace_ids:
        # Reuse relies on Amazon echoing the sent dataStartTime/dataEndTime
        # for this snapshot report, unverified until the first live run.
        request = ReportRequest(
            CatalogItem.REPORT_TYPE,
            (marketplace_id,),
            report_time,
            report_time,
            report_options={"preferredReportDocumentLocale": "en_US"},
            earliest_created=earliest_created,
        )
        report = await _reused(client, request, Priority.INCREMENTAL, log)
        if report is None and previous_time > completed_bucket:
            previous_request = ReportRequest(
                CatalogItem.REPORT_TYPE, (marketplace_id,), previous_time, previous_time,
                earliest_created=earliest_created,
            )
            report = await _reused(client, previous_request, Priority.INCREMENTAL, log)
        if report is None or report.processingStatus == ProcessingStatus.CANCELLED:
            report = await request_report(client, request, Priority.INCREMENTAL, log)
        listings: dict[str, tuple[set[str], str | None]] = {}
        # Amazon cancels reports with no data. Treat two cancellations as no listings
        # so empty marketplaces can finish without spending quota on every invocation.
        if report is not None:
            async for _, row in read_report(
                client,
                report,
                Priority.INCREMENTAL,
                log,
                required_columns=("asin1", "seller-sku", "status"),
            ):
                asin = row["asin1"].strip()
                if not asin:
                    continue
                # SKUs may have different statuses. Prefer Active, otherwise
                # keep the first nonblank status in report order.
                skus, status = listings.setdefault(asin, (set(), None))
                if sku := row["seller-sku"].strip():
                    skus.add(sku)
                row_status = row["status"].strip() or None
                listings[asin] = (
                    skus,
                    row_status if row_status == "Active" or status is None else status,
                )
        asins = sorted(listings)
        returned: set[str] = set()
        for offset in range(0, len(asins), CatalogItem.PAGE_SIZE):
            batch = asins[offset:offset + CatalogItem.PAGE_SIZE]
            params: dict[str, Any] = {
                "identifiers": ",".join(batch),
                "identifiersType": "ASIN",
                "marketplaceIds": marketplace_id,
                "includedData": "summaries",
                "pageSize": CatalogItem.PAGE_SIZE,
            }
            while True:
                try:
                    _, body = await client.get(
                        log,
                        Operation.SEARCH_CATALOG_ITEMS,
                        CatalogItem.PATH,
                        params,
                        Priority.INCREMENTAL,
                    )
                except HTTPError as err:
                    if err.code != 403:
                        raise
                    raise ReportRoleMissing(
                        "Amazon refused searchCatalogItems (HTTP 403): the likely cause is "
                        "that the application or selling account lacks the Product Listing role. "
                        "Grant the role, or disable the catalog_items binding.\n\n"
                        f"{err.message}"
                    ) from err
                page = SearchCatalogItemsResponse.model_validate_json(
                    b"".join([chunk async for chunk in body()])
                )
                for item in page.items:
                    if item.asin not in batch or item.asin in returned:
                        continue
                    summary = next(
                        (s for s in item.summaries if s.marketplaceId == marketplace_id),
                        None,
                    )
                    if summary is None:
                        continue
                    returned.add(item.asin)
                    skus, status = listings[item.asin]
                    yield CatalogItem.model_validate({
                        **summary.model_dump(),
                        "asin": item.asin,
                        "marketplaceId": marketplace_id,
                        "listingStatus": status,
                        "sellerSkus": sorted(skus),
                        "refreshedAt": started,
                    })
                if not page.pagination or not page.pagination.nextToken:
                    break
                params["pageToken"] = page.pagination.nextToken
        missing = set(asins) - returned
        for asin in sorted(missing):
            log.debug(
                "catalog ASIN not returned",
                {"asin": asin, "marketplace_id": marketplace_id},
            )
        log.info(
            "refreshed catalog items",
            {
                "marketplace_id": marketplace_id,
                "listed_asins": len(asins),
                "emitted_asins": len(returned),
                "missing_asins": len(missing),
            },
        )
    yield started
