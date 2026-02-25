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

# In-memory cache of how far along the "delayed" change stream is.
last_delayed_fetch_end: dict[str, datetime] = {}

# How far back the delayed stream will search up until. This needs to be far
# back enough that the HubSpot APIs return consistent data.
delayed_offset = timedelta(hours=1)

# The minimum amount of time must be available for the delay stream to be
# called. It can't be called every time `process_changes` is invoked since
# FetchDelayedFn is usually using a slower API than FetchRecentFn, and calling
# FetchDelayedFn every time would negate the benefit of having a faster
# FetchRecentFn.
delayed_fetch_minimum_window = timedelta(minutes=5)

# Limit the maximum time window we will ask FetchRecentFn to get changes for.
# This is to prevent the time window from becoming excessively large if there
# are relatively brief outages in the connector (on the order of a day or two),
# which otherwise would require a checkpoint containing all of the documents
# from the last log_cursor up until the present time. This allows for a form of
# incremental progress to be made, which is helpful if there are ~millions of
# documents to catch up on. The HubSpot APIs are pretty flaky and will
# occasionally return non-retryable errors for no reason and a successful
# checkpoint requires that to never happen, so really large checkpoints can
# become effectively impossible to complete.
#
# Generally moving the cursor forward in these fixed windows is a bad idea when
# trying to cover time windows on the scales of ~years (as in for a backfill),
# but incremental bindings shouldn't fall more than ~days behind in the worst of
# cases so requesting an hour at a time won't take too long.
max_fetch_recent_window = timedelta(hours=1)


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

async def process_changes(
    object_name: str,
    fetch_recent: FetchRecentFn,
    fetch_delayed: FetchDelayedFn,
    http: HTTPSession,
    with_history: bool,
    # Remainder is common.FetchChangesFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Any | LogCursor, None]:
    """
    High-level processing of changes event documents, where there is both a
    stream of very recent documents that is potentially incomplete, and a
    trailing stream that is delayed but 100% complete.

    Every time this function is invoked, the FetchRecentFn is called and any
    recent documents are emitted. If a sufficient amount of time has passed
    since the last time the delayed stream was polled, the FetchDelayedFn is
    called and those documents are emitted to ensure all documents are captured.
    This process will inevitably result in some duplicate documents being
    emitted, but some duplicates are better than missing data completely.

    The LogCursor is based on the position of the recent stream in time. To keep
    track of the progress of both the recent and delayed streams with a single
    LogCursor, there's a few of important details to note:

    - The delayed stream will only ever read documents as recent as
      `delayed_offset` (1 hour).
    - The delayed stream will always be polled up to the LogCursor timestamp
      minus `delayed_offset` if the LogCursor to be emitted would represent a
      delayed stream "window" of at least `delayed_fetch_minimum_window` (5
      minutes).
    - On connector initialization, the delayed stream is assumed to have last
      polled up to the persisted LogCursor, less `delayed_offset` _and_
      `delayed_fetch_minimum_window`. This ensures that when the connector
      restarts the delayed stream does not resume any later than where it last
      left off, and will in fact always resume from just prior to the oldest
      point it may have left off.
    - While the connector is running, the delayed stream progress is kept in
      memory.
    """
    assert isinstance(log_cursor, datetime)

    now_time = datetime.now(UTC)
    fetch_recent_end_time: datetime | None = None
    if now_time - log_cursor > max_fetch_recent_window:
        # Limit the maximum requested window for FetchRecentFn if it's been a
        # while since the LogCursor was updated.
        fetch_recent_end_time = log_cursor + max_fetch_recent_window

    max_ts: datetime = log_cursor
    try:
        async for ts, key, obj in fetch_recent(
            log, http, with_history, log_cursor, fetch_recent_end_time
        ):
            if fetch_recent_end_time and ts > fetch_recent_end_time:
                continue
            elif ts > log_cursor:
                max_ts = max(max_ts, ts)
                cache.add_recent(object_name, key, ts)
                yield obj
            else:
                break
    except MustBackfillBinding:
        log.info("triggering automatic backfill for %s", object_name)
        yield Triggers.BACKFILL

    if fetch_recent_end_time:
        # Assume all recent documents up until fetch_recent_end_time were
        # returned by FetchRecentFn. It's fine if this is not strictly true
        # since FetchDelayedFn will eventually fill in any missed documents.
        # This also ensures that the cursor is always kept moving forward even
        # if there are no new recent documents for a long time, which is
        # important since it moves the delayed stream forward in that case.
        max_ts = fetch_recent_end_time

    delayed_fetch_next_start = last_delayed_fetch_end.setdefault(
        object_name, log_cursor - delayed_offset - delayed_fetch_minimum_window
    )
    delayed_fetch_next_end = max_ts - delayed_offset

    cache_hits = 0
    delayed_emitted = 0
    if delayed_fetch_next_end - delayed_fetch_next_start > delayed_fetch_minimum_window:
        # Poll the delayed stream for documents if we need to.
        try:
            async for ts, key, obj in fetch_delayed(
                log,
                http,
                with_history,
                delayed_fetch_next_start,
                delayed_fetch_next_end,
            ):
                if ts > delayed_fetch_next_end:
                    # In case the FetchDelayedFn is unable to filter based on
                    # `delayed_fetch_next_end`.
                    continue
                elif ts > delayed_fetch_next_start:
                    if cache.has_as_recent_as(object_name, key, ts):
                        cache_hits += 1
                        continue

                    delayed_emitted += 1
                    yield obj
                else:
                    break
        except MustBackfillBinding:
            log.info("triggering automatic backfill for %s", object_name)
            yield Triggers.BACKFILL

        last_delayed_fetch_end[object_name] = delayed_fetch_next_end
        evicted = cache.cleanup(object_name, delayed_fetch_next_end)

        log.info(
            "fetched delayed events for stream",
            {
                "object_name": object_name,
                "since": delayed_fetch_next_start,
                "until": delayed_fetch_next_end,
                "emitted": delayed_emitted,
                "cache_hits": cache_hits,
                "evicted": evicted,
                "new_size": cache.count_for(object_name),
            },
        )

    if max_ts != log_cursor:
        yield max_ts

