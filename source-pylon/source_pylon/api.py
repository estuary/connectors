import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import JsonValue

from .models import (
    Issue,
    IssueContext,
    IssueMessage,
    ListResponse,
    PylonDocument,
)

API_URLS = {"US": "https://api.usepylon.com", "EU": "https://api.eu.usepylon.com"}
TICK = timedelta(seconds=1)
WINDOW = timedelta(days=1)
# Issue search may surface an update some time after its updated_at. Issues
# trail the present by LAG so late arrivals are visible before the cursor
# passes them.
LAG = timedelta(minutes=5)
# Polls with less new time than this wait for the next interval instead of
# immediately requesting a few seconds of changes.
MIN_WINDOW = timedelta(minutes=1)


class PylonClient:
    """Read Pylon resources, sharing a conservative request budget across bindings."""

    def __init__(self, http: HTTPSession, base_url: str):
        self.http: HTTPSession = http
        self.base_url: str = base_url
        self._request_lock: asyncio.Lock = asyncio.Lock()
        self._next_request: float = 0.0

    async def _pace(self) -> None:
        # At most 100 requests/minute across bindings, below the narrowest
        # per-endpoint limit used (120/min):
        # https://docs.usepylon.com/pylon-docs/developer/api/rate-limits
        async with self._request_lock:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0, self._next_request - loop.time()))
            self._next_request = loop.time() + 0.6

    async def pages[T: PylonDocument](
        self,
        log: Logger,
        model: type[T],
        path: str | None = None,
        request_args: dict[str, JsonValue] | None = None,
        validation_context: object | None = None,
    ) -> AsyncGenerator[T, None]:
        """Yield every page; fail on a broken envelope or a nonadvancing cursor."""
        # POST search takes its arguments in the body; GETs take query params.
        args: dict[str, JsonValue] = {**model.request_params, **(request_args or {})}
        if model.paginated:
            args["limit"] = model.page_size
        seen_cursors: set[str] = set()

        while True:
            await self._pace()
            _, response = await self.http.request_stream(
                log,
                f"{self.base_url}{path or model.path}",
                method=model.method,
                params=args if model.method == "GET" else None,
                json=args if model.method == "POST" else None,
            )
            processor = IncrementalJsonProcessor(
                response(),
                "data.item",
                model,
                remainder_cls=ListResponse,
                validation_context=validation_context,
            )
            async for item in processor:
                yield item
            pagination = processor.get_remainder().pagination
            if pagination is None or not pagination.has_next_page:
                return
            if not model.paginated:
                raise RuntimeError(f"Unexpected pagination on {model.name}")
            if not pagination.cursor or pagination.cursor in seen_cursors:
                raise RuntimeError(
                    f"Missing or repeated pagination cursor on {model.name}"
                )
            seen_cursors.add(pagination.cursor)
            args["cursor"] = pagination.cursor


async def snapshot_resources(
    client: PylonClient, model: type[PylonDocument], log: Logger
) -> AsyncGenerator[PylonDocument, None]:
    """Read every partition of a resource."""
    for partition in model.partitions:
        async for document in client.pages(log, model, request_args=partition):
            yield document


async def snapshot_issue_messages(
    client: PylonClient, log: Logger
) -> AsyncGenerator[IssueMessage, None]:
    """Read current messages across all issues, independently of the issues binding."""
    # Message changes aren't known to update their issue's updated_at, so every
    # issue's messages are read, including old and closed issues outside the
    # issues start date. Parent IDs are drained before any message request so
    # no response stays open, and deduplicated since issues can shift between
    # search pages.
    parent_ids = {parent.id async for parent in client.pages(log, Issue)}
    for issue_id in sorted(parent_ids):
        async for message in client.pages(
            log,
            IssueMessage,
            path=IssueMessage.path_for(issue_id),
            validation_context=IssueContext(issue_id),
        ):
            yield message


async def _issue_window(
    client: PylonClient, log: Logger, lower: datetime, upper: datetime
) -> AsyncGenerator[Issue, None]:
    # Pylon's filters keep subsecond precision but returned timestamps are
    # whole seconds, so the filter's inclusivity can't be pinned. Query a
    # one-second margin and keep issues within the inclusive bounds.
    window: dict[str, JsonValue] = {
        "filter": Issue.window_filter(lower - TICK, upper + TICK)
    }
    async for issue in client.pages(log, Issue, request_args=window):
        if lower <= issue.get_cursor() <= upper:
            yield issue


async def fetch_issues(
    client: PylonClient, log: Logger, cursor: LogCursor
) -> AsyncGenerator[Issue | LogCursor, None]:
    """Emit issues updated after the cursor, up to LAG before the present.

    The cursor is the last second already emitted. Each poll covers at most one
    window and checkpoints only after draining it.
    """
    assert isinstance(cursor, datetime)
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - LAG
    if horizon - cursor < MIN_WINDOW:
        return
    upper = min(horizon, cursor + WINDOW)
    log.info(
        "Pylon issues incremental window",
        extra={"cursor": cursor.isoformat(), "window_end": upper.isoformat()},
    )
    async for issue in _issue_window(client, log, cursor + TICK, upper):
        yield issue
    yield upper


async def backfill_issues(
    client: PylonClient,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Issue | PageCursor, None]:
    """Emit issues updated from the page cursor (or start date) up to the cutoff.

    Each call drains one window and checkpoints the next window's start time,
    never a Pylon page token. The cutoff second belongs to incremental capture.
    """
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)
    lower = start_date if page is None else datetime.fromisoformat(page)
    if lower >= cutoff:
        return
    next_window = min(lower + WINDOW, cutoff)
    log.info(
        "Pylon issues backfill window",
        extra={
            "cursor": lower.isoformat(),
            "window_end": (next_window - TICK).isoformat(),
            "next_cursor": next_window.isoformat(),
        },
    )
    async for issue in _issue_window(client, log, lower, next_window - TICK):
        yield issue
    if next_window < cutoff:
        yield next_window.isoformat()
