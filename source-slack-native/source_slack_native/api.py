import asyncio
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

import aiohttp
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, RootModel

from .models import (
    AuthTestResponse,
    Channel,
    ChannelIdValidationContext,
    ChannelMember,
    HistoryRemainder,
    Message,
    SlackResponse,
    ThreadReply,
    User,
)

API = "https://slack.com/api"

# Slack has been observed leaving requests unanswered indefinitely. Every
# request carries an explicit timeout so a stalled connection fails and is
# retried instead of hanging the capture.
HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=300,
    sock_connect=30,
    sock_read=120,
)

# Minimum spacing between calls to each Web API method, derived from the
# method's published rate-limit tier: Tier 2 is 20+ req/min, Tier 3 is 50+
# req/min, Tier 4 is 100+ req/min. Limits are per method, per workspace, per
# app, so pacing each method independently keeps one busy stream from
# starving another.
TIER_2 = 60 / 20
TIER_3 = 60 / 50
TIER_4 = 60 / 100

METHOD_MIN_INTERVAL: dict[str, float] = {
    "auth.test": TIER_4,
    "users.list": TIER_2,
    "conversations.list": TIER_2,
    "conversations.members": TIER_4,
    "conversations.history": TIER_3,
    "conversations.replies": TIER_3,
    "conversations.join": TIER_3,
}


class SlackAPIError(RuntimeError):
    def __init__(self, method: str, error: str | None):
        super().__init__(f"Slack API method {method} failed: {error}")
        self.method = method
        self.error = error


class _MethodThrottle:
    """Spaces calls to each Slack method at least its tier's interval apart,
    across every concurrent caller in the process."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._next_allowed: dict[str, float] = {}

    async def wait(self, method: str) -> None:
        interval = METHOD_MIN_INTERVAL.get(method, TIER_3)
        lock = self._locks.setdefault(method, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            delay = self._next_allowed.get(method, now) - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_allowed[method] = time.monotonic() + interval


_throttle = _MethodThrottle()


def dt_to_ts(dt: datetime) -> str:
    """Format a datetime as a Slack timestamp, exact to the microsecond."""
    delta = dt - datetime.fromtimestamp(0, tz=UTC)
    seconds, remainder = divmod(delta, timedelta(seconds=1))
    return f"{seconds}.{remainder // timedelta(microseconds=1):06d}"


def ts_to_dt(ts: str) -> datetime:
    """Parse a Slack timestamp into a datetime, exact to the microsecond."""
    seconds, _, fraction = ts.partition(".")
    micros = int(fraction.ljust(6, "0")[:6]) if fraction else 0
    return datetime.fromtimestamp(0, tz=UTC) + timedelta(
        seconds=int(seconds), microseconds=micros
    )


def raise_for_slack_error(method: str, response: SlackResponse) -> None:
    if not response.ok:
        raise SlackAPIError(method, response.error)


async def call[ResponseT: SlackResponse](
    http: HTTPSession,
    method: str,
    response_cls: type[ResponseT],
    log: Logger,
    params: dict[str, Any] | None = None,
    http_method: str = "GET",
) -> ResponseT:
    """Call a Slack Web API method and return its parsed, successful
    response."""
    await _throttle.wait(method)
    response = response_cls.model_validate_json(
        await http.request(
            log,
            f"{API}/{method}",
            method=http_method,
            params=params,
            timeout=HTTP_TIMEOUT,
        )
    )
    raise_for_slack_error(method, response)
    return response


async def stream_page[ItemT: BaseModel, RemainderT: SlackResponse](
    http: HTTPSession,
    method: str,
    items_key: str,
    item_cls: type[ItemT],
    remainder_cls: type[RemainderT],
    log: Logger,
    params: dict[str, Any],
    validation_context: object | None = None,
) -> AsyncGenerator[ItemT | RemainderT, None]:
    """Stream one page of a Slack list method. Yields each item, then the
    page's remainder (envelope, pagination metadata) last."""
    await _throttle.wait(method)
    _, body = await http.request_stream(
        log,
        f"{API}/{method}",
        params=params,
        timeout=HTTP_TIMEOUT,
    )
    processor = IncrementalJsonProcessor(
        body(),
        f"{items_key}.item",
        item_cls,
        remainder_cls,
        validation_context=validation_context,
    )
    async for item in processor:
        yield item

    remainder = processor.get_remainder()
    raise_for_slack_error(method, remainder)
    yield remainder


async def auth_test(http: HTTPSession, log: Logger) -> AuthTestResponse:
    return await call(http, "auth.test", AuthTestResponse, log)


async def _paginate[ItemT: BaseModel](
    http: HTTPSession,
    method: str,
    items_key: str,
    item_cls: type[ItemT],
    log: Logger,
    request_params: dict[str, Any],
    validation_context: object | None = None,
) -> AsyncGenerator[ItemT, None]:
    """Every item of a cursor-paginated Slack list method."""
    params = dict(request_params)

    while True:
        next_cursor = ""
        async for item in stream_page(
            http,
            method,
            items_key,
            item_cls,
            SlackResponse,
            log,
            params,
            validation_context,
        ):
            if isinstance(item, SlackResponse):
                next_cursor = item.response_metadata.next_cursor
            else:
                yield item

        if not next_cursor:
            return
        params["cursor"] = next_cursor


async def list_channels(
    http: HTTPSession,
    include_private_channels: bool,
    log: Logger,
) -> AsyncGenerator[Channel, None]:
    """Every public channel in the workspace, archived ones included, plus the
    private channels the bot is a member of when requested."""
    types = (
        "public_channel,private_channel"
        if include_private_channels
        else "public_channel"
    )
    params: dict[str, Any] = {
        "types": types,
        "exclude_archived": "false",
        "limit": Channel.page_size,
    }
    async for channel in _paginate(
        http, Channel.path, Channel.items_key, Channel, log, params
    ):
        yield channel


async def snapshot_users(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[User, None]:
    """Every user in the workspace, deactivated users and bots included."""
    params: dict[str, Any] = {"limit": User.page_size}
    async for user in _paginate(http, User.path, User.items_key, User, log, params):
        yield user


async def snapshot_channels(
    http: HTTPSession,
    include_private_channels: bool,
    log: Logger,
) -> AsyncGenerator[Channel, None]:
    async for channel in list_channels(http, include_private_channels, log):
        yield channel


class _MemberID(RootModel[str]):
    pass


async def snapshot_channel_members(
    http: HTTPSession,
    channel_ids: list[str],
    log: Logger,
) -> AsyncGenerator[ChannelMember, None]:
    """One document per (channel, member) pair, across the given channels."""
    for channel_id in channel_ids:
        params: dict[str, Any] = {
            "channel": channel_id,
            "limit": ChannelMember.page_size,
        }
        async for member in _paginate(
            http,
            ChannelMember.path,
            ChannelMember.items_key,
            _MemberID,
            log,
            params,
        ):
            yield ChannelMember(channel_id=channel_id, user_id=member.root)


async def join_channel(http: HTTPSession, channel_id: str, log: Logger) -> None:
    await call(
        http,
        "conversations.join",
        SlackResponse,
        log,
        params={"channel": channel_id},
        http_method="POST",
    )


# The most recent `lookback_window` of each channel is re-read at most this
# often, to pick up edits, reactions, and new thread replies.
LOOKBACK_REFRESH_INTERVAL = timedelta(hours=1)

_logged_is_limited = False


def _log_is_limited_once(log: Logger, channel_id: str) -> None:
    global _logged_is_limited
    if not _logged_is_limited:
        _logged_is_limited = True
        log.warning(
            "Slack is withholding messages older than the free plan's history limit; they cannot be captured",
            {"channel_id": channel_id},
        )


def horizon_now() -> datetime:
    """The exclusive upper bound of an incremental window: the start of the
    current, still-elapsing second."""
    return datetime.now(tz=UTC).replace(microsecond=0)


@dataclass(frozen=True, slots=True)
class _HistoryPage:
    messages: list[Message]
    has_more: bool


async def _history_page(
    http: HTTPSession,
    channel_id: str,
    oldest: str,
    latest: str,
    log: Logger,
) -> _HistoryPage:
    """One page of a channel's top-level messages with `oldest < ts < latest`,
    newest first. Further pages are read by passing the oldest returned `ts`
    as the next `latest`."""
    params: dict[str, Any] = {
        "channel": channel_id,
        "oldest": oldest,
        "latest": latest,
        "limit": Message.page_size,
    }
    messages: list[Message] = []
    has_more = False
    async for item in stream_page(
        http,
        Message.path,
        Message.items_key,
        Message,
        HistoryRemainder,
        log,
        params,
        ChannelIdValidationContext(channel_id),
    ):
        if isinstance(item, HistoryRemainder):
            has_more = item.has_more
            if item.is_limited:
                _log_is_limited_once(log, channel_id)
        else:
            messages.append(item)

    return _HistoryPage(messages, has_more)


async def _history_window(
    http: HTTPSession,
    channel_id: str,
    oldest: str,
    latest: str,
    log: Logger,
) -> AsyncGenerator[Message, None]:
    """Every top-level message with `oldest < ts < latest`, newest first."""
    while True:
        page = await _history_page(http, channel_id, oldest, latest, log)
        for message in page.messages:
            yield message

        if not page.has_more or not page.messages:
            return
        latest = page.messages[-1].ts


async def _thread_replies(
    http: HTTPSession,
    channel_id: str,
    parent_ts: str,
    oldest: str | None,
    log: Logger,
) -> AsyncGenerator[ThreadReply, None]:
    """Every reply in a thread with `ts > oldest`, excluding the parent."""
    params: dict[str, Any] = {
        "channel": channel_id,
        "ts": parent_ts,
        "limit": ThreadReply.page_size,
    }
    if oldest is not None:
        params["oldest"] = oldest

    try:
        async for reply in _paginate(
            http,
            ThreadReply.path,
            ThreadReply.items_key,
            ThreadReply,
            log,
            params,
            ChannelIdValidationContext(channel_id),
        ):
            if reply.ts != parent_ts:
                yield reply
    except SlackAPIError as err:
        # The parent was deleted between listing it and reading its thread.
        if err.error != "thread_not_found":
            raise
        log.info(
            "thread no longer exists",
            {"channel_id": channel_id, "thread_ts": parent_ts},
        )


def _active_parents(messages: list[Message], since: str | None) -> list[str]:
    """The `ts` of each thread parent with a reply newer than `since`."""
    return [
        m.ts
        for m in messages
        if m.reply_count
        and m.latest_reply is not None
        and (since is None or ts_to_dt(m.latest_reply) > ts_to_dt(since))
    ]


async def _replies_for_parents(
    http: HTTPSession,
    channel_id: str,
    parents: list[str],
    oldest: str | None,
    log: Logger,
) -> AsyncGenerator[ThreadReply, None]:
    for parent_ts in parents:
        async for reply in _thread_replies(http, channel_id, parent_ts, oldest, log):
            yield reply


async def fetch_messages(
    http: HTTPSession,
    channel_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Message | LogCursor, None]:
    """Top-level messages posted to the channel since the cursor.

    ```
                cursor          horizon = start of current second
    ──────────────┼─────────────────┼──────▶ time (1µs ticks)
                  │                 │
    oldest ───────(═════════════════╪══════▶
    latest ═══════╪═════════════════)
    emitted ──────┼─[═════════════]─┼
                  │                 └─ the current second is in progress;
                  │                    it is read by the next poll
                  └─ already emitted by the previous poll or the backfill
    ```

    Both bounds are exclusive. The next cursor is `horizon - 1µs`, so the next
    poll's first emitted tick is exactly `horizon`.
    """
    assert isinstance(log_cursor, datetime)
    horizon = horizon_now()
    next_cursor = horizon - timedelta(microseconds=1)
    if next_cursor <= log_cursor:
        return

    async for message in _history_window(
        http, channel_id, dt_to_ts(log_cursor), dt_to_ts(horizon), log
    ):
        yield message

    yield next_cursor


async def fetch_messages_lookback(
    http: HTTPSession,
    channel_id: str,
    lookback_window: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Message | LogCursor, None]:
    """Re-reads the channel's most recent `lookback_window` of messages, so
    edits, reactions, and reply counts on recent messages are captured. The
    cursor is the horizon of the last completed re-read; a new re-read starts
    once `LOOKBACK_REFRESH_INTERVAL` has passed since it.

    ```
            horizon - lookback_window      horizon
    ──────────────┼─────────────────────────┼──────▶ time (1µs ticks)
                  │                         │
    oldest ───────(═════════════════════════╪══════▶
    latest ═══════╪═════════════════════════)
    emitted ──────┼─[═════════════════════]─┼
                  │                         └─ start of current second
                  └─ older messages keep the last version this re-read saw
    ```
    """
    assert isinstance(log_cursor, datetime)
    horizon = horizon_now()
    if horizon - log_cursor < LOOKBACK_REFRESH_INTERVAL:
        return

    async for message in _history_window(
        http,
        channel_id,
        dt_to_ts(horizon - lookback_window),
        dt_to_ts(horizon),
        log,
    ):
        yield message

    yield horizon


async def backfill_messages(
    http: HTTPSession,
    channel_id: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Message | PageCursor, None]:
    """Top-level messages posted between `start_date` and the cutoff, read
    newest first. The page cursor is the `ts` of the oldest message emitted
    so far; everything newer than it, up to the cutoff, has been captured.

    ```
            start_date            page cursor        cutoff
    ──────────────┼────────────────────┼───────────────┼──▶ time (1µs ticks)
                  │                    │               │
    oldest ───────(════════════════════╪═══════════════╪══▶
    latest ═══════╪════════════════════)               │
    emitted ──────┼──────[═══════════]─┼───────────────┼
                  │                    │               └─ the first page
                  │                    │                  uses latest = cutoff
                  │                    └─ captured by earlier pages
                  └─ not load-bearing; queried as-is
    ```

    `latest` is exclusive, so the cutoff instant itself is left to the
    incremental task, whose cursor starts at `cutoff - 1µs`.
    """
    assert isinstance(cutoff, datetime)
    latest = page if isinstance(page, str) else dt_to_ts(cutoff)

    result = await _history_page(http, channel_id, dt_to_ts(start_date), latest, log)

    for message in result.messages:
        yield message

    if result.has_more and result.messages:
        yield result.messages[-1].ts


async def _thread_replies_in_window(
    http: HTTPSession,
    channel_id: str,
    oldest: str,
    latest: str,
    log: Logger,
) -> AsyncGenerator[ThreadReply, None]:
    """Replies newer than `oldest` to threads whose parent has
    `oldest < ts < latest`."""
    parents = _active_parents(
        [m async for m in _history_window(http, channel_id, oldest, latest, log)],
        oldest,
    )
    async for reply in _replies_for_parents(http, channel_id, parents, oldest, log):
        yield reply


async def fetch_threads(
    http: HTTPSession,
    channel_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ThreadReply | LogCursor, None]:
    """Replies to threads whose parent was posted since the cursor.

    ```
                cursor          horizon = start of current second
    ──────────────┼─────────────────┼──────▶ time (1µs ticks)
                  │                 │
    parent oldest (═════════════════╪══════▶
    parent latest ╪═════════════════)
    reply oldest ─(═════════════════╪══════▶
    emitted ──────┼─[═══════════════╪══════▶
                  │                 └─ replies newer than the horizon are
                  │                    emitted now; their parents are not
                  │                    revisited by later polls
                  └─ replies to older parents come from the lookback
    ```
    """
    assert isinstance(log_cursor, datetime)
    horizon = horizon_now()
    next_cursor = horizon - timedelta(microseconds=1)
    if next_cursor <= log_cursor:
        return

    async for reply in _thread_replies_in_window(
        http, channel_id, dt_to_ts(log_cursor), dt_to_ts(horizon), log
    ):
        yield reply

    yield next_cursor


async def fetch_threads_lookback(
    http: HTTPSession,
    channel_id: str,
    lookback_window: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ThreadReply | LogCursor, None]:
    """Re-reads replies to threads whose parent was posted within the most
    recent `lookback_window`, so replies and edits to recent threads are
    captured. Replies to parents older than that are only captured by a
    re-backfill. The cursor is the horizon of the last completed re-read; a
    new re-read starts once `LOOKBACK_REFRESH_INTERVAL` has passed since it.

    ```
            horizon - lookback_window      horizon
    ──────────────┼─────────────────────────┼──────▶ time (1µs ticks)
                  │                         │
    parent oldest (═════════════════════════╪══════▶
    parent latest ╪═════════════════════════)
    reply oldest ─(═════════════════════════╪══════▶
    emitted ──────┼─[═══════════════════════╪══════▶
                  │                         └─ start of current second
                  └─ only threads with a reply newer than this are read
    ```
    """
    assert isinstance(log_cursor, datetime)
    horizon = horizon_now()
    if horizon - log_cursor < LOOKBACK_REFRESH_INTERVAL:
        return

    async for reply in _thread_replies_in_window(
        http,
        channel_id,
        dt_to_ts(horizon - lookback_window),
        dt_to_ts(horizon),
        log,
    ):
        yield reply

    yield horizon


async def backfill_threads(
    http: HTTPSession,
    channel_id: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ThreadReply | PageCursor, None]:
    """Every reply to threads whose parent was posted between `start_date` and
    the cutoff. Parents are walked newest first; the page cursor is the `ts`
    of the oldest parent whose thread has been fully captured.

    ```
            start_date            page cursor        cutoff
    ──────────────┼────────────────────┼───────────────┼──▶ time (1µs ticks)
                  │                    │               │
    parent oldest (════════════════════╪═══════════════╪══▶
    parent latest ╪════════════════════)               │
    parents ──────┼──────[═══════════]─┼───────────────┼
                  │                    └─ threads of newer parents were
                  │                       captured by earlier pages
                  └─ not load-bearing; queried as-is
    ```

    Each selected thread is read in full, replies newer than the cutoff
    included. `latest` is exclusive, so parents at the cutoff instant are
    left to the incremental task, whose cursor starts at `cutoff - 1µs`.
    """
    assert isinstance(cutoff, datetime)
    latest = page if isinstance(page, str) else dt_to_ts(cutoff)

    result = await _history_page(http, channel_id, dt_to_ts(start_date), latest, log)
    parents = _active_parents(result.messages, None)
    async for reply in _replies_for_parents(http, channel_id, parents, None, log):
        yield reply

    if result.has_more and result.messages:
        yield result.messages[-1].ts
