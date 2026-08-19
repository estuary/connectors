import asyncio
import re
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Callable,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPError, HTTPSession

from ..models import TimestampedObject

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

# HubSpot words this differently per endpoint and per authentication method, so
# the list grows as new wordings turn up.
MISSING_SCOPE_REGEX = (
    # "This app hasn't been granted all required scopes to make this call. Read
    # more about required scopes here: ..."
    r"This app hasn't been granted all required scopes to make this call.|"
    # "EXTERNAL auth request is missing required 'workflows-access-public-api' scope."
    r"auth request is missing required '.+' scope|"
    # "This oauth-token (...) does not have proper permissions!" and "You do not
    # have permissions to view_schema object type ... (requires one of [leads-read])"
    r"do(es)? not have (proper )?permissions|"
    # '"category": "MISSING_SCOPES"', the one part of a refusal HubSpot is
    # unlikely to reword -- but only some responses carry it.
    r'"category": "MISSING_SCOPES"'
)


def is_missing_scope_error(log: Logger, err: HTTPError) -> bool:
    """
    Reports whether err is HubSpot refusing a call because the token wasn't
    granted a scope, as opposed to any other flavor of 403.

    Callers treat an unrecognized 403 as fatal, so log the message: it's the
    only signal that HubSpot has a wording MISSING_SCOPE_REGEX doesn't cover.
    """
    if err.code != 403:
        return False

    if re.search(MISSING_SCOPE_REGEX, err.message):
        return True

    log.warning("Unrecognized 403 response; treating it as fatal.", {
        "message": err.message,
    })
    return False


FetchRecentFn = Callable[
    [Logger, HTTPSession, bool, datetime, datetime | None],
    AsyncGenerator[TimestampedObject[Any], None],
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
    AsyncGenerator[TimestampedObject[Any] | datetime, None],
]
"""
Returns a stream of (timestamp, key, document) tuples that represent a complete
stream of not-so-recent documents. The key is used for seeing if a more recent
change event document has already been emitted by the FetchRecentFn.

A bare datetime may be interleaved between tuples to mark an intermediate
checkpoint boundary: every document the window holds at or before that
timestamp has already been yielded, so fetch_delayed_changes can safely
checkpoint there. Chunked delayed fetchers use this to checkpoint progress
within a large window.

Documents may be returned in any order, and a document's timestamp may fall
outside the requested window when the provider's own notion of the document's
modification time disagrees with the timestamp it reports. Every document is
emitted regardless; fetch_delayed_changes checkpoints on the window's bounds,
not on document timestamps. The first datetime parameter is the "since" value
and the second is the "until" value; documents outside them should not be
retrieved where the API allows it.
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

    Every document the FetchDelayedFn yields is emitted, whatever timestamp it
    carries. Checkpoints come from the window's bounds, never from a document:
    the datetimes the fetcher interleaves mark intermediate progress, and the
    window's upper bound is the final checkpoint.
    """
    assert isinstance(log_cursor, datetime)

    now = datetime.now(UTC)
    lower_bound = log_cursor
    horizon = now - DELAYED_LAG

    # Don't poll until at least MIN_DELAYED_WINDOW of data has accumulated
    # to prevent excessive API calls.
    if horizon - lower_bound < MIN_DELAYED_WINDOW:
        await asyncio.sleep(
            (MIN_DELAYED_WINDOW - (horizon - lower_bound)).total_seconds()
        )
        now = datetime.now(UTC)
        horizon = now - DELAYED_LAG

    # Limit the window to prevent huge checkpoints when catching up.
    upper_bound = min(horizon, log_cursor + MAX_DELAYED_WINDOW)

    if lower_bound >= upper_bound:
        return

    last_checkpoint = lower_bound
    saw_anything = False
    cache_hits = 0
    emitted = 0

    def _log_progress(cursor: datetime) -> None:
        evicted = cache.cleanup(object_name, cursor)
        log.info(
            "fetched delayed events for stream",
            {
                "object_name": object_name,
                "since": lower_bound,
                "until": cursor,
                "emitted": emitted,
                "cache_hits": cache_hits,
                "evicted": evicted,
                "new_size": cache.count_for(object_name),
            },
        )

    async for item in fetch_delayed(
        log,
        http,
        with_history,
        lower_bound,
        upper_bound,
    ):
        saw_anything = True

        if isinstance(item, datetime):
            # An intermediate checkpoint boundary yielded by a chunked
            # fetcher. Every document at or before this timestamp has
            # already been yielded.
            if item > last_checkpoint:
                _log_progress(item)
                last_checkpoint = item
                yield item
            continue

        ts, key, obj = item
        if cache.should_yield(object_name, key, ts):
            emitted += 1
            yield obj
        else:
            cache_hits += 1

    is_catching_up = upper_bound < horizon

    # A catch-up window always checkpoints so progress is made even through
    # an empty hour. A caught-up window that returned nothing leaves the
    # cursor where it is so idle streams don't checkpoint on every poll;
    # the next poll just reads a slightly wider window.
    if (saw_anything or is_catching_up) and upper_bound > last_checkpoint:
        _log_progress(upper_bound)
        yield upper_bound
