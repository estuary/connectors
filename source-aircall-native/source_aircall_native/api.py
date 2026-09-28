from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    AircallListRemainder,
    AircallReferenceEntity,
    Call,
    CallsResponse,
    CompanyEnvelope,
    Contact,
    ContactsResponse,
)
from .shared import API, now, to_unix

# per_page values above 50 are silently clamped to 50.
PER_PAGE = 50
# Aircall rejects any list request whose page * per_page exceeds 10,000 with a
# 400, regardless of how many results actually match.
MAX_OFFSET = 10_000
MAX_PAGE = MAX_OFFSET // PER_PAGE

ONE_SECOND = timedelta(seconds=1)
# Bounds how much time a single incremental invocation covers, so catching up
# checkpoints at least once per day of calls.
CALLS_WINDOW = timedelta(days=1)
# /v1/calls only serves the last six months of calls. Backfill windows stay
# comfortably inside that horizon, since a window straddling the edge would
# lose rows while being paged and shift the pages after them.
CALLS_RETENTION_CLAMP = timedelta(days=175)
# Deletions between page reads shift the `updated_at` walk; it restarts at most
# this many times before giving up.
MAX_CONTACTS_WALK_RESTARTS = 5


async def snapshot_list(
    http: HTTPSession,
    model: type[AircallReferenceEntity],
    log: Logger,
) -> AsyncGenerator[AircallReferenceEntity, None]:
    url = f"{model.base_url}/{model.path}"
    page = 1

    while True:
        if page * PER_PAGE > MAX_OFFSET:
            # Stopping here would truncate the snapshot and infer deletions for
            # every document past the cap, so fail loudly instead.
            msg = f"{model.name} has more than {MAX_OFFSET} items, and Aircall rejects pages past its {MAX_OFFSET}-result cap. Please contact Estuary support."
            raise RuntimeError(msg)

        _, body = await http.request_stream(
            log, url, params={"page": page, "per_page": PER_PAGE}
        )
        processor = IncrementalJsonProcessor(
            body(),
            f"{model.response_key}.item",
            model,
            remainder_cls=AircallListRemainder,
        )

        async for doc in processor:
            yield doc

        meta = processor.get_remainder().meta
        if meta.count == 0 or meta.current_page * PER_PAGE >= meta.total:
            return

        page += 1


async def snapshot_object(
    http: HTTPSession,
    model: type[AircallReferenceEntity],
    log: Logger,
) -> AsyncGenerator[AircallReferenceEntity, None]:
    response = CompanyEnvelope.model_validate_json(
        await http.request(log, f"{model.base_url}/{model.path}")
    )
    yield response.company


async def _fetch_calls_page(
    http: HTTPSession,
    log: Logger,
    lo: int,
    hi: int,
    page: int,
) -> CallsResponse:
    params: dict[str, str | int] = {
        "from": lo,
        "to": hi,
        "order": "asc",
        "per_page": PER_PAGE,
        "page": page,
    }
    return CallsResponse.model_validate_json(
        await http.request(log, f"{API}/{Call.path}", params=params)
    )


async def _fetch_calls_window(
    http: HTTPSession,
    log: Logger,
    lo: int,
    hi: int,
) -> AsyncGenerator[Call, None]:
    """Yields every call with lo < started_at <= hi.

    Windows are narrowed until each one fits under Aircall's 10,000-result
    pagination cap, so no request ever pages past it.
    """
    first = await _fetch_calls_page(http, log, lo, hi, 1)

    if first.meta.total >= MAX_OFFSET:
        mid = _bisect_calls_window(lo, hi)
        async for call in _fetch_calls_window(http, log, lo, mid):
            yield call
        async for call in _fetch_calls_window(http, log, mid, hi):
            yield call
        return

    async for call in _drain_calls_window(http, log, lo, hi, first):
        yield call


def _bisect_calls_window(lo: int, hi: int) -> int:
    if hi - lo <= 1:
        msg = f"More than {MAX_OFFSET} calls started within the same second ({hi}), which cannot be paged under Aircall's {MAX_OFFSET}-result cap. Please contact Estuary support."
        raise RuntimeError(msg)

    return lo + (hi - lo) // 2


async def _drain_calls_window(
    http: HTTPSession,
    log: Logger,
    lo: int,
    hi: int,
    first: CallsResponse,
) -> AsyncGenerator[Call, None]:
    """Yields every call with lo < started_at <= hi, given the window's first
    page, whose `meta.total` must be under Aircall's 10,000-result cap.
    """
    response = first
    page = 1
    seen: set[int] = set()

    while True:
        for call in response.calls:
            seen.add(call.id)
            yield call

        if response.meta.next_page_link is None:
            break

        if page >= MAX_PAGE:
            msg = f"Calls window ({lo}, {hi}] grew past Aircall's {MAX_OFFSET}-result cap while being paged. Please contact Estuary support."
            raise RuntimeError(msg)

        page += 1
        response = await _fetch_calls_page(http, log, lo, hi, page)

    if len(seen) < first.meta.total:
        log.warning(
            "Captured fewer calls than Aircall reported for the window.",
            {"from": lo, "to": hi, "total": first.meta.total, "seen": len(seen)},
        )


async def fetch_calls(
    http: HTTPSession,
    lag: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Call | LogCursor, None]:
    """Yields calls started after `log_cursor`, up to `lag` behind the present.

    A zero `lag` tails calls as they start. A non-zero `lag` re-reads calls once
    they have finalized, capturing the endings, tags, comments and recordings
    added after a call starts.

    ```
               cursor   cursor + 1s    upper = min(cursor + 1d,
                                                   now − 1s − lag)
    ──────────────┼──────────┼───────────────────┼─────▶ time (1s ticks)
                  │          │                   │
    from ─────────(══════════╪═══════════════════╪═════▶
    to ═══════════╪══════════╪═══════════════════]
    emitted ──────┼──────────[═══════════════════]
                  │          │                   └─ yielded as the next
                  │          │                      cursor, even if empty
                  │          └─ first emitted second
                  └─ already emitted by the previous window
    ```

    `from` is exclusive and `to` is inclusive, both on `started_at`.
    """
    assert isinstance(log_cursor, datetime)

    horizon = now().replace(microsecond=0) - ONE_SECOND - lag
    if horizon <= log_cursor:
        return

    upper = min(log_cursor + CALLS_WINDOW, horizon)

    async for call in _fetch_calls_window(
        http, log, to_unix(log_cursor), to_unix(upper)
    ):
        yield call

    yield upper


async def backfill_calls(
    http: HTTPSession,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Call | PageCursor, None]:
    """Yields calls started after `start_date` and before `cutoff`, fewer than
    10,000 at a time. `page` is the upper bound of the last completed window,
    in unix seconds.

    ```
              lo = page or start_date   hi <= cutoff − 1s
    ────────────────────┼─────────────────────┼─────▶ time (1s ticks)
                        │                     │
    from ───────────────(═════════════════════╪═════▶
    to ═════════════════╪═════════════════════]
    window ─────────────┼─[═══════════════════]
                        │                     └─ halved from cutoff − 1s until
                        │                        under the cap; the next page
                        │                        unless it reaches cutoff − 1s
                        └─ clamped to 175 days ago, inside Aircall's
                           six-month history
    ```

    `from` is exclusive and `to` is inclusive, both on `started_at`.
    """
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    retention_start = to_unix(now() - CALLS_RETENTION_CLAMP)
    lo = page if page is not None else to_unix(start_date)
    if lo < retention_start:
        log.info(
            "Aircall only serves six months of call history. Starting the calls backfill at its retention horizon instead.",
            {"requested_start": lo, "retention_start": retention_start},
        )
        lo = retention_start

    end = to_unix(cutoff) - 1
    if lo >= end:
        return

    # Take as much of the remaining range as fits under the 10,000-result cap,
    # so each invocation checkpoints after at most one capped window.
    hi = end
    first = await _fetch_calls_page(http, log, lo, hi, 1)
    while first.meta.total >= MAX_OFFSET:
        hi = _bisect_calls_window(lo, hi)
        first = await _fetch_calls_page(http, log, lo, hi, 1)

    async for call in _drain_calls_window(http, log, lo, hi, first):
        yield call

    if hi < end:
        yield hi


async def _fetch_contacts_page(
    http: HTTPSession,
    log: Logger,
    params: dict[str, str | int],
) -> ContactsResponse:
    return ContactsResponse.model_validate_json(
        await http.request(log, f"{API}/{Contact.path}", params=params)
    )


class _ContactsWalkShifted(Exception):
    pass


class _ContactsWalkCapped(Exception):
    pass


async def _walk_contacts_by_update(
    http: HTTPSession,
    log: Logger,
    cursor: int,
    horizon: int,
) -> AsyncGenerator[Contact, None]:
    """Yields contacts with cursor < updated_at <= horizon, newest first.

    Raises _ContactsWalkShifted if a deletion may have shifted rows between
    pages, and _ContactsWalkCapped if the cursor is not reached within
    Aircall's 10,000-result cap.
    """
    first_total: int | None = None

    for page in range(1, MAX_PAGE + 1):
        response = await _fetch_contacts_page(
            http,
            log,
            {
                "order_by": "updated_at",
                "order": "desc",
                "per_page": PER_PAGE,
                "page": page,
            },
        )

        if first_total is None:
            first_total = response.meta.total
        elif response.meta.total < first_total:
            raise _ContactsWalkShifted()

        for contact in response.contacts:
            if contact.updated_at > horizon:
                continue
            if contact.updated_at <= cursor:
                return
            yield contact

        if response.meta.next_page_link is None:
            return

    raise _ContactsWalkCapped()


async def _contacts_created_from(
    http: HTTPSession,
    log: Logger,
    watermark: int,
    upper: int,
) -> AsyncGenerator[Contact | int, None]:
    """Yields a batch of contacts with watermark <= created_at <= upper, then
    the watermark to resume from, unless every such contact has been yielded.
    """
    params: dict[str, str | int] = {
        "from": watermark,
        "to": upper,
        "order_by": "created_at",
        "order": "asc",
        "per_page": PER_PAGE,
        "page": 1,
    }
    response = await _fetch_contacts_page(http, log, params)

    for contact in response.contacts:
        yield contact

    if response.meta.next_page_link is None:
        return

    last = response.contacts[-1].created_at
    if last > watermark:
        # The next batch re-reads second `last`, which only duplicates.
        yield last
        return

    # A full page of contacts was created within second `watermark`. Drain
    # that second on its own before moving past it.
    params["to"] = watermark
    page = 1
    while response.meta.next_page_link is not None:
        page += 1
        if page > MAX_PAGE:
            msg = f"More than {MAX_OFFSET} contacts were created within the same second ({watermark}), which cannot be paged under Aircall's {MAX_OFFSET}-result cap. Please contact Estuary support."
            raise RuntimeError(msg)
        params["page"] = page
        response = await _fetch_contacts_page(http, log, params)
        for contact in response.contacts:
            yield contact

    if watermark + 1 <= upper:
        yield watermark + 1


async def fetch_contacts(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Contact | LogCursor, None]:
    """Yields contacts updated after `log_cursor`.

    ```
                  cursor   cursor + 1s  horizon = last elapsed second
    ─────────────────┼──────────┼──────────────┼─────▶ time (1s ticks)
                     │          │              │
    walk stop ───────(══════════╪══════════════╪═════▶
    horizon skip ════╪══════════╪══════════════]
    emitted ─────────┼──────────[══════════════]
                     │          │              └─ in-progress second waits
                     │          │                 for the next poll
                     │          └─ first emitted second
                     └─ walk stops at the first doc with updated_at <= cursor
    ```

    Both bounds are applied client-side while walking contacts by descending
    `updated_at`, since Aircall has no `updated_at` filter.
    """
    assert isinstance(log_cursor, datetime)

    horizon_dt = now().replace(microsecond=0) - ONE_SECOND
    if horizon_dt <= log_cursor:
        return

    cursor = to_unix(log_cursor)
    horizon = to_unix(horizon_dt)
    emitted = False

    for _ in range(MAX_CONTACTS_WALK_RESTARTS):
        try:
            async for contact in _walk_contacts_by_update(http, log, cursor, horizon):
                emitted = True
                yield contact
            break
        except _ContactsWalkShifted:
            log.info("Contacts were deleted while paging. Restarting the walk.")
        except _ContactsWalkCapped:
            log.warning(
                f"More than {MAX_OFFSET} contacts changed since the last poll. Re-reading every contact to catch up.",
                {"cursor": cursor, "horizon": horizon},
            )
            watermark: int | None = 0
            while watermark is not None:
                batch_start, watermark = watermark, None
                async for item in _contacts_created_from(
                    http, log, batch_start, horizon
                ):
                    if isinstance(item, int):
                        watermark = item
                    elif cursor < item.updated_at <= horizon:
                        emitted = True
                        yield item
            break
    else:
        msg = f"Contacts kept being deleted while paging, even after {MAX_CONTACTS_WALK_RESTARTS} attempts. Please contact Estuary support."
        raise RuntimeError(msg)

    if emitted:
        yield horizon_dt


async def backfill_contacts(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Contact | PageCursor, None]:
    """Yields every contact created before `cutoff`. `page` is the
    `created_at` watermark to resume from, in unix seconds.

    ```
              0 (epoch)          W = page    cutoff − 1s   cutoff
    ─────────────────┼──────────────┼──────────────┼───────────┼──▶ created_at
    from ────────────┼──────────────[══════════════╪═══════════╪══▶
    to ══════════════╪══════════════╪══════════════]           │
    emitted ─────────┼──────────────[══════════════]           │
                                    │              │           └─ incremental
                                    │              │              takes over
                                    │              └─ inclusive
                                    └─ resume watermark; second W re-read
    ```

    `from` and `to` are both inclusive, on `created_at`.
    """
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    watermark = page if page is not None else 0
    upper = to_unix(cutoff) - 1
    if watermark > upper:
        return

    async for item in _contacts_created_from(http, log, watermark, upper):
        yield item
