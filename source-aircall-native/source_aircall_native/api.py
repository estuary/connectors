from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from logging import Logger
from typing import TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.http import HTTPSession

from .models import (
    CALLS,
    CONTACTS,
    NUMBERS,
    TAGS,
    TEAMS,
    USER_AVAILABILITY,
    USERS,
    WEBHOOKS,
    AircallMeta,
    Call,
    CompanyResponse,
    Contact,
    Endpoint,
    ListPage,
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
# A lagging subtask's cursor always trails the present by more than its
# interval, so the CDK re-invokes it right after every checkpoint. Skipping
# shorter windows lets it sleep instead of polling every second.
MIN_CALLS_WINDOW = timedelta(minutes=1)
# /v1/calls only serves the last six months of calls. Backfill windows stay
# comfortably inside that horizon, since a window straddling the edge would
# lose rows while being paged and shift the pages after them.
CALLS_RETENTION_CLAMP = timedelta(days=175)
# Deletions between page reads shift the `updated_at` walk, which then
# restarts at most this many times.
MAX_CONTACTS_WALK_ATTEMPTS = 5

_Doc = TypeVar("_Doc", bound=BaseDocument)


async def _fetch_page(
    log: Logger,
    http: HTTPSession,
    endpoint: Endpoint[_Doc],
    params: dict[str, str | int],
) -> ListPage[_Doc]:
    return ListPage[endpoint.model].model_validate_json(
        await http.request(log, endpoint.url, params=params),
        context={"response_key": endpoint.response_key},
    )


def _is_last_page(meta: AircallMeta) -> bool:
    # Completion is driven by `total`, since /v1/webhooks never sends
    # `next_page_link`.
    return meta.current_page * PER_PAGE >= meta.total


async def _pages(
    log: Logger,
    http: HTTPSession,
    endpoint: Endpoint[_Doc],
    params: dict[str, str | int],
) -> AsyncGenerator[ListPage[_Doc], None]:
    """Yields each page of a list request in order, and raises rather than
    request a page past Aircall's 10,000-result cap."""
    for page in range(1, MAX_PAGE + 1):
        response = await _fetch_page(
            log, http, endpoint, {**params, "per_page": PER_PAGE, "page": page}
        )
        yield response

        if not response.items or _is_last_page(response.meta):
            return

    msg = f"{endpoint.url} has more than {MAX_OFFSET} results, and Aircall rejects pages past its {MAX_OFFSET}-result cap. Please contact Estuary support."
    raise RuntimeError(msg)


async def _snapshot_list(
    log: Logger,
    http: HTTPSession,
    endpoint: Endpoint[BaseDocument],
) -> AsyncGenerator[BaseDocument, None]:
    async for response in _pages(log, http, endpoint, {}):
        for doc in response.items:
            yield doc


async def snapshot_users(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, USERS):
        yield doc


async def snapshot_user_availability(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, USER_AVAILABILITY):
        yield doc


async def snapshot_teams(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, TEAMS):
        yield doc


async def snapshot_numbers(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, NUMBERS):
        yield doc


async def snapshot_tags(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, TAGS):
        yield doc


async def snapshot_webhooks(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    async for doc in _snapshot_list(log, http, WEBHOOKS):
        yield doc


async def snapshot_company(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[BaseDocument, None]:
    response = CompanyResponse.model_validate_json(
        await http.request(log, f"{API}/company")
    )
    yield response.company


def _calls_params(lo: int, hi: int) -> dict[str, str | int]:
    return {"from": lo, "to": hi, "order": "asc"}


async def _count_calls(log: Logger, http: HTTPSession, lo: int, hi: int) -> int:
    response = await _fetch_page(
        log, http, CALLS, {**_calls_params(lo, hi), "per_page": 1, "page": 1}
    )
    return response.meta.total


async def _fetch_calls_window(
    log: Logger,
    http: HTTPSession,
    lo: int,
    hi: int,
) -> AsyncGenerator[Call, None]:
    """Yields every call with lo < started_at <= hi, halving the window until
    each part fits under Aircall's 10,000-result cap."""
    async for response in _pages(log, http, CALLS, _calls_params(lo, hi)):
        if response.meta.current_page == 1 and response.meta.total >= MAX_OFFSET:
            break

        for call in response.items:
            yield call
    else:
        return

    if hi - lo <= 1:
        msg = f"More than {MAX_OFFSET} calls started within the same second ({hi}), which cannot be paged under Aircall's {MAX_OFFSET}-result cap. Please contact Estuary support."
        raise RuntimeError(msg)

    mid = lo + (hi - lo) // 2
    async for call in _fetch_calls_window(log, http, lo, mid):
        yield call
    async for call in _fetch_calls_window(log, http, mid, hi):
        yield call


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
                  │          │                   └─ next cursor, even if empty
                  │          └─ first emitted second
                  └─ already emitted by the previous window
    ```

    `from` is exclusive and `to` is inclusive, both on `started_at`.
    """
    assert isinstance(log_cursor, datetime)

    horizon = now().replace(microsecond=0) - ONE_SECOND - lag
    if horizon - log_cursor < MIN_CALLS_WINDOW:
        return

    upper = min(log_cursor + CALLS_WINDOW, horizon)

    async for call in _fetch_calls_window(
        log, http, to_unix(log_cursor), to_unix(upper)
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
                        │                     └─ sized to fit under the cap
                        └─ no earlier than 175 days ago
    ```

    `from` is exclusive and `to` is inclusive, both on `started_at`.
    """
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    retention_start = to_unix(now() - CALLS_RETENTION_CLAMP)
    lo = page if page is not None else to_unix(start_date)
    if lo < retention_start:
        log.info(
            "starting the calls backfill at aircall's six-month retention horizon",
            {"requested_start": lo, "retention_start": retention_start},
        )
        lo = retention_start

    end = to_unix(cutoff) - 1
    if lo >= end:
        return

    # Size the window from the remaining range's count so it holds about half
    # the cap, shrinking it further when calls are bunched toward its start.
    hi = end
    total = await _count_calls(log, http, lo, hi)
    while total >= MAX_OFFSET and hi - lo > 1:
        hi = lo + max(1, (hi - lo) * (MAX_OFFSET // 2) // total)
        total = await _count_calls(log, http, lo, hi)

    async for call in _fetch_calls_window(log, http, lo, hi):
        yield call

    if hi < end:
        yield hi


async def _contacts_updated_between(
    log: Logger,
    http: HTTPSession,
    cursor: int,
    horizon: int,
) -> list[Contact] | None:
    """Returns contacts with cursor < updated_at <= horizon, or None if more
    than 10,000 contacts changed and the walk can't reach the cursor."""
    params: dict[str, str | int] = {"order_by": "updated_at", "order": "desc"}

    for _ in range(MAX_CONTACTS_WALK_ATTEMPTS):
        changed: list[Contact] = []
        first_total: int | None = None

        async for response in _pages(log, http, CONTACTS, params):
            contacts, meta = response.items, response.meta
            if first_total is None:
                first_total = meta.total
            elif meta.total < first_total:
                break

            changed.extend(c for c in contacts if cursor < c.updated_at <= horizon)

            if contacts and contacts[-1].updated_at <= cursor:
                return changed
            if meta.current_page == MAX_PAGE and not _is_last_page(meta):
                return None
        else:
            return changed

        log.info("restarting the contacts walk after a deletion shifted its pages")

    msg = f"Contacts kept being deleted while paging, even after {MAX_CONTACTS_WALK_ATTEMPTS} attempts. Please contact Estuary support."
    raise RuntimeError(msg)


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
                     │          │              └─ waits for the next poll
                     │          └─ first emitted second
                     └─ walk stops at the first updated_at <= cursor
    ```

    Both bounds are applied client-side while walking contacts by descending
    `updated_at`, since Aircall has no `updated_at` filter. When more than
    10,000 contacts changed, every contact is re-read by `created_at` instead.
    """
    assert isinstance(log_cursor, datetime)

    horizon_dt = now().replace(microsecond=0) - ONE_SECOND
    if horizon_dt <= log_cursor:
        return

    cursor = to_unix(log_cursor)
    horizon = to_unix(horizon_dt)

    changed = await _contacts_updated_between(log, http, cursor, horizon)
    if changed is None:
        log.warning(
            "more than 10,000 contacts changed since the last poll, re-reading every contact",
            {"cursor": cursor, "horizon": horizon},
        )
        changed = []
        page: PageCursor = None
        while True:
            next_page: PageCursor = None
            async for item in backfill_contacts(
                http, log, page, horizon_dt + ONE_SECOND
            ):
                if not isinstance(item, Contact):
                    next_page = item
                elif cursor < item.updated_at <= horizon:
                    changed.append(item)

            if next_page is None:
                break
            page = next_page

    for contact in changed:
        yield contact

    if changed:
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

    params: dict[str, str | int] = {
        "from": watermark,
        "to": upper,
        "order_by": "created_at",
        "order": "asc",
    }
    response = await _fetch_page(
        log, http, CONTACTS, {**params, "per_page": PER_PAGE, "page": 1}
    )
    for contact in response.items:
        yield contact

    if _is_last_page(response.meta):
        return

    last = response.items[-1].created_at
    if last > watermark:
        # The next batch re-reads second `last`, which only duplicates.
        yield last
        return

    # A full page of contacts was created within second `watermark`. Drain
    # that second on its own before moving past it.
    async for response in _pages(log, http, CONTACTS, {**params, "to": watermark}):
        for contact in response.items:
            yield contact

    if watermark < upper:
        yield watermark + 1
