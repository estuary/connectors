from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    AuditLog,
    BaseIncrementalResource,
    BaseSnapshotResource,
    Coupon,
    CursorRemainder,
    Customer,
    CustomerCredit,
    CustomerSegment,
    Feature,
    Invoice,
    InvoicingEntity,
    OffsetRemainder,
    PriceBook,
    PriceConfiguration,
    Product,
    PromotionCode,
    Quote,
    Subscription,
    SubscriptionTransition,
    TaxRate,
    Transaction,
    User,
    Wallet,
    format_rfc3339_ms,
)

API = "https://api.hyperline.co"
SANDBOX_API = "https://sandbox.api.hyperline.co"

# Both paging engines reject page sizes over 100 with a 400; nothing clamps
# server-side, so the connector always asks for exactly the maximum.
MAX_PAGE_SIZE = 100

BACKFILL_CHUNK = timedelta(days=30)


def base_url(api_key: str) -> str:
    # Hyperline key prefixes are environment-bound: test_ keys only work
    # against the sandbox host, prod_ keys only against production.
    return SANDBOX_API if api_key.startswith("test_") else API


async def _cursor_pages[T: BaseIncrementalResource | AuditLog](
    http: HTTPSession,
    url: str,
    request_params: dict[str, str | int],
    model: type[T],
    log: Logger,
) -> AsyncGenerator[T, None]:
    """Yield every document of a keyset-cursor listing (`limit` + opaque
    `cursor`), re-sending the caller's filters on every page."""
    cursor: str | None = None

    while True:
        params: dict[str, str | int] = {"limit": MAX_PAGE_SIZE, **request_params}
        if cursor is not None:
            params["cursor"] = cursor

        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(), "data.item", model, remainder_cls=CursorRemainder
        )

        async for doc in processor:
            yield doc

        remainder = processor.get_remainder()
        if not remainder.has_more or remainder.next_cursor is None:
            return

        cursor = remainder.next_cursor


async def _offset_pages[T: BaseIncrementalResource | BaseSnapshotResource](
    http: HTTPSession,
    url: str,
    request_params: dict[str, str | int],
    model: type[T],
    log: Logger,
) -> AsyncGenerator[T, None]:
    """Yield every document of a `take`/`skip` offset listing. Completion is a
    short page; `meta.taken` is unreliable and `meta.total` is only logged."""
    skip = 0

    while True:
        params: dict[str, str | int] = {
            "take": MAX_PAGE_SIZE,
            "skip": skip,
            **request_params,
        }

        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(), "data.item", model, remainder_cls=OffsetRemainder
        )

        count = 0
        async for doc in processor:
            yield doc
            count += 1

        log.debug(
            "fetched offset page",
            {"url": url, "skip": skip, "total": processor.get_remainder().meta.total},
        )

        if count < MAX_PAGE_SIZE:
            return

        skip += count


async def _drain_window[T: BaseIncrementalResource](
    http: HTTPSession,
    base: str,
    model: type[T],
    request_params: dict[str, str | int],
    log: Logger,
) -> AsyncGenerator[T, None]:
    url = f"{base}{model.PATH}"
    if model.ENGINE == "cursor":
        pages = _cursor_pages(http, url, request_params, model, log)
    else:
        pages = _offset_pages(http, url, request_params, model, log)

    async for doc in pages:
        yield doc


async def _fetch_incremental[T: BaseIncrementalResource](
    model: type[T],
    http: HTTPSession,
    base: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | LogCursor, None]:
    """Incrementally fetch documents updated at `(cursor, horizon]`.

                  cursor   cursor + 1s  horizon = last elapsed second
    ─────────────────┼──────────┼──────────────┼─────▶ time (1s ticks)
                     │          │              │
    updated_at__gt ──(══════════╪══════════════╪═════▶
    updated_at__lte ═╪══════════╪══════════════]
    emitted ─────────┼──────────[══════════════]
                     │          │              └─ the present second is still
                     │          │                 in progress; its docs wait
                     │          │                 for the next poll's window
                     │          └─ first emitted second
                     └─ nothing new can appear here or earlier: this second
                        had fully elapsed when it was walked, and updates
                        always stamp "now"

    `__gt` is exclusive and `__lte` inclusive at millisecond precision, so
    consecutive windows tile the timeline without gap or overlap.
    """
    assert isinstance(log_cursor, datetime)

    # Fetch complete ticks only: the last fully-elapsed second. The 1s guard
    # over the API's 1ms cursor resolution also absorbs clock skew.
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return

    request_params: dict[str, str | int] = {
        model.SINCE_EXCLUSIVE: format_rfc3339_ms(log_cursor),
        model.UNTIL_INCLUSIVE: format_rfc3339_ms(horizon),
    }

    async for doc in _drain_window(http, base, model, request_params, log):
        yield doc

    yield horizon


async def _backfill_incremental[T: BaseIncrementalResource](
    model: type[T],
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[T | PageCursor, None]:
    """Backfill documents updated at `[start_date, cutoff)`, one 30-day chunk
    per invocation.

                start_date        chunk_end   ...    cutoff
    ─────────────────┼────────────────┼───────┼────────┼──▶ time (1ms ticks)
                     │                │                │
    updated_at__gte ═[════════════════╪════════════════╪═▶
    updated_at__lt ══╪════════════════)                │
    window ══════════[════════════════)                │
                     │                │                └─ covered by the first
                     │                │                   incremental poll
                     │                └─ next invocation resumes here
                     └─ start-of-window precision is not load-bearing;
                        the boundary instant just falls out

    The PageCursor is the chunk's end timestamp — a value watermark over
    `updated_at`, not a positional offset, so a resume never skips documents.
    Positional paging state (opaque cursor / skip offset) stays inside a
    single invocation. `updated_at` is mutable: a document updated mid-sweep
    leaves the window and is caught by the incremental backstop.
    """
    assert isinstance(cutoff, datetime)

    if page is None:
        chunk_start = start_date
    else:
        assert isinstance(page, str)
        chunk_start = datetime.fromisoformat(page.replace("Z", "+00:00"))

    if chunk_start >= cutoff:
        return

    chunk_end = min(chunk_start + BACKFILL_CHUNK, cutoff)

    request_params: dict[str, str | int] = {
        model.SINCE_INCLUSIVE: format_rfc3339_ms(chunk_start),
        model.UNTIL_EXCLUSIVE: format_rfc3339_ms(chunk_end),
    }

    async for doc in _drain_window(http, base, model, request_params, log):
        yield doc

    if chunk_end >= cutoff:
        return

    yield format_rfc3339_ms(chunk_end)


async def fetch_customers(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Customer | LogCursor, None]:
    async for item in _fetch_incremental(Customer, http, base, log, log_cursor):
        yield item


async def backfill_customers(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Customer | PageCursor, None]:
    async for item in _backfill_incremental(
        Customer, http, base, start_date, log, page, cutoff
    ):
        yield item


async def fetch_invoices(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Invoice | LogCursor, None]:
    async for item in _fetch_incremental(Invoice, http, base, log, log_cursor):
        yield item


async def backfill_invoices(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Invoice | PageCursor, None]:
    async for item in _backfill_incremental(
        Invoice, http, base, start_date, log, page, cutoff
    ):
        yield item


async def fetch_subscriptions(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Subscription | LogCursor, None]:
    async for item in _fetch_incremental(Subscription, http, base, log, log_cursor):
        yield item


async def backfill_subscriptions(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Subscription | PageCursor, None]:
    async for item in _backfill_incremental(
        Subscription, http, base, start_date, log, page, cutoff
    ):
        yield item


async def fetch_quotes(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Quote | LogCursor, None]:
    async for item in _fetch_incremental(Quote, http, base, log, log_cursor):
        yield item


async def backfill_quotes(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Quote | PageCursor, None]:
    async for item in _backfill_incremental(
        Quote, http, base, start_date, log, page, cutoff
    ):
        yield item


async def fetch_customer_credits(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[CustomerCredit | LogCursor, None]:
    async for item in _fetch_incremental(CustomerCredit, http, base, log, log_cursor):
        yield item


async def backfill_customer_credits(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[CustomerCredit | PageCursor, None]:
    async for item in _backfill_incremental(
        CustomerCredit, http, base, start_date, log, page, cutoff
    ):
        yield item


async def _snapshot[T: BaseSnapshotResource](
    model: type[T],
    http: HTTPSession,
    base: str,
    log: Logger,
) -> AsyncGenerator[T, None]:
    async for doc in _offset_pages(
        http, f"{base}{model.PATH}", dict(model.EXTRA_QUERY), model, log
    ):
        yield doc


async def snapshot_products(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[Product, None]:
    async for doc in _snapshot(Product, http, base, log):
        yield doc


async def snapshot_price_books(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[PriceBook, None]:
    async for doc in _snapshot(PriceBook, http, base, log):
        yield doc


async def snapshot_price_configurations(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[PriceConfiguration, None]:
    async for doc in _snapshot(PriceConfiguration, http, base, log):
        yield doc


async def snapshot_features(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[Feature, None]:
    async for doc in _snapshot(Feature, http, base, log):
        yield doc


async def snapshot_coupons(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[Coupon, None]:
    async for doc in _snapshot(Coupon, http, base, log):
        yield doc


async def snapshot_promotion_codes(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[PromotionCode, None]:
    async for doc in _snapshot(PromotionCode, http, base, log):
        yield doc


async def snapshot_tax_rates(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[TaxRate, None]:
    async for doc in _snapshot(TaxRate, http, base, log):
        yield doc


async def snapshot_invoicing_entities(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[InvoicingEntity, None]:
    async for doc in _snapshot(InvoicingEntity, http, base, log):
        yield doc


async def snapshot_users(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[User, None]:
    async for doc in _snapshot(User, http, base, log):
        yield doc


async def snapshot_customer_segments(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[CustomerSegment, None]:
    async for doc in _snapshot(CustomerSegment, http, base, log):
        yield doc


async def snapshot_transactions(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[Transaction, None]:
    async for doc in _snapshot(Transaction, http, base, log):
        yield doc


async def snapshot_wallets(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[Wallet, None]:
    async for doc in _snapshot(Wallet, http, base, log):
        yield doc


async def snapshot_subscription_transitions(
    http: HTTPSession, base: str, log: Logger
) -> AsyncGenerator[SubscriptionTransition, None]:
    async for doc in _snapshot(SubscriptionTransition, http, base, log):
        yield doc


async def fetch_audit_logs(
    http: HTTPSession, base: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[AuditLog | LogCursor, None]:
    """Incrementally fetch audit-log events with `happened_at >= cursor` by
    walking the strictly-descending listing newest-first.

               log_cursor              max_seen        now
    ──────────────────┼────────────────────┼────────────┼──▶ time (1ms ticks)
                      │                    │            │
    walk (desc) ◀═════[════════════════════╪════════════╡
    emitted ══════════[════════════════════]            │
                      │                    │            └─ the walk starts at
                      │                    │               the newest event
                      │                    └─ next poll's cursor
                      └─ boundary events at exactly this millisecond re-emit
                         every poll; the surrogate key collapses them

    The API offers no timestamp filter, so the lower boundary is client-side
    by necessity: the walk early-terminates at the first event older than the
    cursor. The `>=` boundary closes the same-millisecond-arrival gap a strict
    `>` would leave. Descending order forbids mid-walk checkpoints — the
    cursor is yielded only after the walk completes, so an interrupted walk
    restarts from the top (redundant re-reads, collapsed by the key).
    """
    assert isinstance(log_cursor, datetime)

    url = f"{base}{AuditLog.PATH}"
    cursor: str | None = None
    max_seen = log_cursor
    emitted = False

    while True:
        params: dict[str, str | int] = {"limit": MAX_PAGE_SIZE}
        if cursor is not None:
            params["cursor"] = cursor

        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(), "data.item", AuditLog, remainder_cls=CursorRemainder
        )

        reached_cursor = False
        async for doc in processor:
            if doc.happened_at < log_cursor:
                reached_cursor = True
                break
            yield doc
            emitted = True
            if doc.happened_at > max_seen:
                max_seen = doc.happened_at

        if reached_cursor:
            break

        remainder = processor.get_remainder()
        if not remainder.has_more or remainder.next_cursor is None:
            break

        cursor = remainder.next_cursor

    if emitted:
        yield max_seen


async def backfill_audit_logs(
    http: HTTPSession,
    base: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[AuditLog | PageCursor, None]:
    """Backfill audit-log events with `start_date <= happened_at < cutoff`,
    one page per invocation, walking newest-first.

            start_date                     cutoff        newest
    ─────────────┼────────────────────────────┼────────────┼──▶ time (1ms ticks)
                 │                            │            │
    walk (desc) ◀╪════════════════════════════╪════════════╡
    emitted ═════[════════════════════════════)            │
                 │                            │            └─ pages newer than
                 │                            │               the cutoff are
                 │                            │               walked, suppressed
                 │                            └─ incremental owns >= cutoff
                 └─ the walk terminates at the first event older than this

    The PageCursor is the provider's keyset token — pinned to an event id, not
    a position; the log is append-only and descending, so new events insert
    strictly above any resumed position and the resumed suffix is frozen.
    Long-horizon token durability is unverified: an expired token fails as a
    loud 400 "Invalid cursor", on which the walk restarts from the newest
    event — pure re-emission, collapsed by the surrogate key, never loss.
    """
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    url = f"{base}{AuditLog.PATH}"

    while True:
        params: dict[str, str | int] = {"limit": MAX_PAGE_SIZE}
        if page is not None:
            params["cursor"] = page

        try:
            _, body = await http.request_stream(log, url, params=params)
            break
        except HTTPError as err:
            if page is not None and err.code == 400 and "Invalid cursor" in err.message:
                log.warning(
                    "audit-logs backfill cursor no longer valid; restarting from the newest event",
                )
                page = None
                continue
            raise

    processor = IncrementalJsonProcessor(
        body(), "data.item", AuditLog, remainder_cls=CursorRemainder
    )

    reached_start = False
    async for doc in processor:
        if doc.happened_at < start_date:
            reached_start = True
            break
        if doc.happened_at >= cutoff:
            continue
        yield doc

    if reached_start:
        return

    remainder = processor.get_remainder()
    if not remainder.has_more or remainder.next_cursor is None:
        return

    yield remainder.next_cursor
