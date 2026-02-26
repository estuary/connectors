from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Callable,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, Triggers
from estuary_cdk.http import HTTPSession


# Hubspot returns a 500 internal server error if querying for data at the EPOCH.
EPOCH_PLUS_ONE_SECOND = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=1)
HUB = "https://api.hubapi.com"

# How far back the delayed stream trails behind realtime. This needs to be far
# back enough that the HubSpot APIs return consistent data.
DELAYED_LAG = timedelta(hours=1)

# Limit the maximum time window subtasks will fetch in a single call.
# This prevents huge checkpoints when catching up after the connector falls behind.
# The HubSpot APIs are flaky and large checkpoints can become impossible to complete.
MAX_REALTIME_WINDOW = timedelta(hours=1)
MAX_DELAYED_WINDOW = timedelta(hours=1)

# Minimum time window required before polling the delayed stream.
# Prevents excessive API calls when Resource.interval is small.
MIN_DELAYED_WINDOW = timedelta(minutes=5)


class MustBackfillBinding(Exception):
    pass


FetchRecentFn = Callable[
    [Logger, HTTPSession, bool, datetime, datetime | None],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
"""
Returns a stream of (timestamp, key, document) tuples that represent a
potentially incomplete stream of very recent documents. The timestamp is used to
checkpoint the next log cursor. The key is used for updating the emitted changes
cache with the timestamp of the document.

Documents may be returned in any order, but iteration will be stopped upon
seeing an entry that's as-old or older than the datetime cursor.

The first datetime parameter represents the "since" value, which is the oldest
documents that are required. The second datetime parameter is an "until" value,
which if not None is a hint that more recent documents are not needed.
"""

FetchDelayedFn = Callable[
    [Logger, HTTPSession, bool, datetime, datetime],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
"""
Returns a stream of (timestamp, key, document) tuples that represent a complete
stream of not-so-recent documents. The key is used for seeing if a more recent
change event document has already been emitted by the FetchRecentFn.

Similar to FetchRecentFn, documents may be returned in any order and iteration
stops when seeing an entry that's as-old or older than the "since" datetime
cursor, which is the first datetime parameter. The second datetime parameter
represents an "until" value, and documents more recent than this are discarded
and should usually not even be retrieved if possible.
"""


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def chunk_props(props: list[str], max_bytes: int) -> list[list[str]]:
    result: list[list[str]] = []

    current_chunk: list[str] = []
    current_size = 0

    for p in props:
        sz = len(p.encode("utf-8"))

        if current_size + sz > max_bytes:
            result.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(p)
        current_size += sz

    if current_chunk:
        result.append(current_chunk)

    return result


async def fetch_realtime_changes(
    object_name: str,
    fetch_recent: FetchRecentFn,
    http: HTTPSession,
    with_history: bool,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Any | LogCursor, None]:
    """
    This function uses fast but potentially incomplete APIs to capture very
    recent documents. Each emitted document is added to the cache so the
    DELAYED subtask can deduplicate.
    """
    assert isinstance(log_cursor, datetime)

    now = datetime.now(UTC)
    lower_bound = log_cursor

    upper_bound: datetime | None = None
    if now - lower_bound > MAX_REALTIME_WINDOW:
        # Limit the maximum requested window for FetchRecentFn if it's been a
        # while since the LogCursor was updated.
        upper_bound = lower_bound + MAX_REALTIME_WINDOW

    max_ts = log_cursor

    try:
        async for ts, key, obj in fetch_recent(
            log, http, with_history, lower_bound, upper_bound
        ):
            if upper_bound and ts > upper_bound:
                continue
            elif ts > lower_bound:
                max_ts = max(max_ts, ts)
                if cache.should_yield(object_name, key, ts):
                    yield obj
            else:
                break
    except MustBackfillBinding:
        log.info("triggering automatic backfill for %s", object_name)
        yield Triggers.BACKFILL

    # Assume all recent documents up until upper_bound were
    # returned by FetchRecentFn. It's fine if this is not strictly true
    # since FetchDelayedFn will eventually fill in any missed documents.
    # This also ensures that the cursor is always kept moving forward even
    # if there are no new recent documents for a long time.
    if upper_bound:
        max_ts = upper_bound

    if max_ts != lower_bound:
        yield max_ts


async def fetch_delayed_changes(
    object_name: str,
    fetch_delayed: FetchDelayedFn,
    http: HTTPSession,
    with_history: bool,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Any | LogCursor, None]:
    """
    This function uses consistent APIs to ensure all documents are
    captured. It uses cache.should_yield() to skip documents that were already
    emitted by the REALTIME subtask with the same or newer timestamp.
    """
    assert isinstance(log_cursor, datetime)

    now = datetime.now(UTC)
    lower_bound = log_cursor
    horizon = now - DELAYED_LAG

    # Don't poll unless at least MIN_DELAYED_WINDOW of data has accumulated
    # to prevent excessive API calls.
    if horizon - lower_bound < MIN_DELAYED_WINDOW:
        return

    # Limit the window to prevent huge checkpoints when catching up.
    upper_bound = min(horizon, log_cursor + MAX_DELAYED_WINDOW)

    if lower_bound >= upper_bound:
        return

    max_ts = lower_bound
    cache_hits = 0
    emitted = 0

    try:
        async for ts, key, obj in fetch_delayed(
            log,
            http,
            with_history,
            lower_bound,
            upper_bound,
        ):
            if ts > upper_bound:
                # In case the FetchDelayedFn is unable to filter based on upper_bound.
                continue
            elif ts > lower_bound:
                max_ts = max(max_ts, ts)
                if cache.should_yield(object_name, key, ts):
                    emitted += 1
                    yield obj
                else:
                    cache_hits += 1
            else:
                break
    except MustBackfillBinding:
        log.info("triggering automatic backfill for %s", object_name)
        yield Triggers.BACKFILL

    # If we limited the window, advance cursor to upper_bound.
    if upper_bound < horizon:
        max_ts = upper_bound

    if max_ts != lower_bound:
        evicted = cache.cleanup(object_name, max_ts)
        log.info(
            "fetched delayed events for stream",
            {
                "object_name": object_name,
                "since": lower_bound,
                "until": upper_bound,
                "emitted": emitted,
                "cache_hits": cache_hits,
                "evicted": evicted,
                "new_size": cache.count_for(object_name),
            },
        )
        yield max_ts
