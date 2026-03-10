import asyncio
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, assert_never

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from source_gong.models import (
    Call,
    DateFormat,
    GongResource,
    GongResponseEnvelope,
    HttpMethod,
    IncrementalGongResource,
    ResponseContext,
)

# Minimum interval between start and end dates for incremental queries.
MIN_INCREMENTAL_INTERVAL = timedelta(minutes=1)

# Lag to account for Gong API eventual consistency.
LAG = timedelta(minutes=5)


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _ts_to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def _format_date(dt: datetime, fmt: DateFormat) -> str:
    if fmt == DateFormat.DATE:
        return dt.strftime("%Y-%m-%d")
    return dt.isoformat().replace("+00:00", "Z")


def _parse_response[T: GongResource](
    cls: type[T], items_key: str, response: bytes
) -> tuple[list[T], str | None]:
    """Parse Gong API response in a single pass via model_validate_json with context."""
    envelope: GongResponseEnvelope[T] = GongResponseEnvelope[T].model_validate_json(
        response,
        context=ResponseContext(item_cls=cls, items_key=items_key),
    )
    return envelope.items, envelope.next_cursor


class GongRateLimiter:
    """
    Sliding window rate limiter for Gong API.

    Gong API has a rate limit of approximately 3 requests/second.
    We use 170 requests per 60 seconds for safety margin.
    """

    def __init__(self, max_requests: int = 170, window_seconds: float = 60.0):
        self._timestamps: deque[float] = deque()
        self._max_requests = max_requests
        self._window = window_seconds
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()

                while self._timestamps and (now - self._timestamps[0]) > self._window:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._max_requests:
                    self._timestamps.append(now)
                    return

                sleep_time = self._window - (now - self._timestamps[0])

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


async def _make_request(
    cls: type[IncrementalGongResource],
    http: HTTPSession,
    rate_limiter: GongRateLimiter,
    base_url: str,
    from_val: str,
    to_val: str,
    log: Logger,
    cursor: str | None,
) -> bytes:
    """Make a single API request using the model's ClassVars for endpoint config."""
    url = f"{base_url}{cls.URL_PATH}"

    match cls.METHOD:
        case HttpMethod.GET:
            params: dict[str, str] = {
                cls.FROM_PARAM: from_val,
                cls.TO_PARAM: to_val,
            }
            if cursor:
                params["cursor"] = cursor
            await rate_limiter.acquire()
            return await http.request(log, url, params=params)

        case HttpMethod.POST:
            body: dict[str, object] = {cls.FROM_PARAM: from_val, cls.TO_PARAM: to_val}
            if cls.FILTER_WRAPPER:
                body = {"filter": body}
            if cursor:
                body["cursor"] = cursor
            await rate_limiter.acquire()
            return await http.request(log, url, method="POST", json=body)

        case _ as unreachable:
            assert_never(unreachable)


async def snapshot_resource(
    cls: type[GongResource],
    url_path: str,
    items_key: str,
    http: HTTPSession,
    rate_limiter: GongRateLimiter,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[GongResource, None]:
    url = f"{base_url}{url_path}"
    cursor: str | None = None

    while True:
        await rate_limiter.acquire()

        params: dict[str, str] = {}
        if cursor:
            params["cursor"] = cursor

        response = await http.request(log, url, params=params if params else None)
        items, cursor = _parse_response(cls, items_key, response)

        for item in items:
            yield item

        if not cursor:
            break


async def fetch_page(
    cls: type[IncrementalGongResource],
    http: HTTPSession,
    rate_limiter: GongRateLimiter,
    base_url: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalGongResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(page, str | None)

    from_val = _format_date(start_date, cls.DATE_FORMAT)
    to_val = _format_date(cutoff, cls.DATE_FORMAT)

    response = await _make_request(
        cls, http, rate_limiter, base_url, from_val, to_val, log, page
    )
    items, next_cursor = _parse_response(cls, cls.ITEMS_KEY, response)

    for item in items:
        yield item

    if next_cursor:
        yield next_cursor


async def fetch_calls_changes(
    http: HTTPSession,
    rate_limiter: GongRateLimiter,
    base_url: str,
    lookback_window: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Call | LogCursor, None]:
    """
    Fetch call changes with a lookback window to capture post-call enrichment.

    On the first run of each day, queries from cursor - lookback_window to now,
    re-emitting calls that may have been enriched since last seen. Deduplication
    happens via the primary key at the Flow runtime level. On subsequent same-day
    runs, only fetches new calls from the cursor forward.
    """
    assert isinstance(log_cursor, int)

    cursor_dt = _ts_to_dt(log_cursor)
    now = datetime.now(tz=UTC) - LAG

    if now <= cursor_dt:
        return

    # Apply lookback on first run of each day to catch post-call enrichment.
    if cursor_dt.date() < now.date():
        start_date = cursor_dt - timedelta(days=lookback_window)
    else:
        start_date = cursor_dt

    end_date = now

    if (end_date - start_date) < MIN_INCREMENTAL_INTERVAL:
        return

    page_cursor: str | None = None

    while True:
        from_val = _format_date(start_date, Call.DATE_FORMAT)
        to_val = _format_date(end_date, Call.DATE_FORMAT)

        response = await _make_request(
            Call, http, rate_limiter, base_url, from_val, to_val, log, page_cursor
        )
        items, page_cursor = _parse_response(Call, Call.ITEMS_KEY, response)

        if not items:
            break

        for doc in items:
            yield doc

        if not page_cursor:
            break

    yield _dt_to_ts(end_date)


async def fetch_changes(
    cls: type[IncrementalGongResource],
    http: HTTPSession,
    rate_limiter: GongRateLimiter,
    base_url: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalGongResource | LogCursor, None]:
    assert isinstance(log_cursor, int)

    start_date = _ts_to_dt(log_cursor)
    now = datetime.now(tz=UTC) - LAG
    end_date = min(start_date + timedelta(days=1), now)

    if end_date < start_date or (end_date - start_date) < MIN_INCREMENTAL_INTERVAL:
        return

    cursor: str | None = None
    max_cursor_value = start_date
    has_results = False

    while True:
        from_val = _format_date(start_date, cls.DATE_FORMAT)
        to_val = _format_date(end_date, cls.DATE_FORMAT)

        response = await _make_request(
            cls, http, rate_limiter, base_url, from_val, to_val, log, cursor
        )
        items, cursor = _parse_response(cls, cls.ITEMS_KEY, response)

        if not items:
            break

        has_results = True

        for doc in items:
            if doc.cursor_value < log_cursor or doc.cursor_value > end_date.timestamp():
                continue
            max_cursor_value = max(max_cursor_value, _ts_to_dt(doc.cursor_value))
            yield doc

        if not cursor:
            break

    if has_results:
        yield _dt_to_ts(max_cursor_value + timedelta(seconds=1))
    else:
        yield _dt_to_ts(end_date)
