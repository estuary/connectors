from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from source_monday.graphql import (
    execute_query,
    fetch_activity_logs,
    fetch_boards_by_ids,
    fetch_boards_minimal,
    fetch_items_by_ids,
    fetch_items_minimal,
    fetch_boards_with_retry,
    TEAMS,
    USERS,
    TAGS,
)
from source_monday.models import (
    FullRefreshResource,
    Board,
    Item,
)
from source_monday.item_cache import ItemIdCache
from source_monday.utils import parse_monday_timestamp


MAX_WINDOW_SIZE = timedelta(hours=2)
FULL_REFRESH_RESOURCES: list[tuple[str, str]] = [
    ("tags", TAGS),
    ("users", USERS),
    ("teams", TEAMS),
]


_item_id_cache = ItemIdCache()


async def fetch_boards_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Board | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    has_updates = False
    board_ids_to_fetch: set[str] = set()

    minimal_boards_checked = 0
    async for board in fetch_boards_minimal(http, log):
        minimal_boards_checked += 1

        if board.updated_at > log_cursor:
            if board.state == "deleted":
                board.meta_ = Board.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, board.updated_at)
                yield board
                log.debug(
                    f"Board {board.id} marked as deleted (updated: {board.updated_at})"
                )
            else:
                board_ids_to_fetch.add(board.id)
                max_updated_at = max(max_updated_at, board.updated_at)
                log.debug(
                    f"Board {board.id} added to fetch list (updated: {board.updated_at})"
                )

    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "delete" in activity_log.event:
            deleted_board = Board(
                id=activity_log.resource_id,
                updated_at=created_at,
                state="deleted",
                workspace=None,
                _meta=Board.Meta(op="d"),
            )
            has_updates = True
            max_updated_at = max(max_updated_at, created_at)
            yield deleted_board
            log.debug(
                f"Board {activity_log.resource_id} marked as deleted in activity logs"
            )
        else:
            board_ids_to_fetch.add(activity_log.resource_id)
            log.debug(
                f"Activity log found for board {activity_log.resource_id} "
                f"(event created_at: {created_at})"
            )

    log.debug(f"Total unique board IDs to fetch: {len(board_ids_to_fetch)}")

    boards_yielded = 0
    if board_ids_to_fetch:
        async for board in fetch_boards_by_ids(http, log, ids=list(board_ids_to_fetch)):
            if board.updated_at > log_cursor:
                if board.state == "deleted":
                    board.meta_ = Board.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, board.updated_at)
                yield board
                boards_yielded += 1

    log.debug(
        f"Board sync: {boards_yielded} yielded, {minimal_boards_checked} checked, {activity_logs_processed} activity logs"
    )

    if has_updates:
        new_cursor = max_updated_at + timedelta(seconds=1)
        log.debug(f"Incremental sync complete. New cursor: {new_cursor}")
        yield new_cursor
    else:
        log.debug("Incremental sync complete. No updates found.")


async def fetch_boards_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Board | PageCursor, None]:
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    if not page:
        page = 1

    limit = 256
    should_yield_page = True
    docs_emitted = 0

    async for board in fetch_boards_with_retry(
        http,
        log,
        page=page,
        limit=limit,
    ):
        if board.updated_at <= cutoff and board.state != "deleted":
            docs_emitted += 1
            yield board

        should_yield_page = True

    if should_yield_page:
        log.debug(
            f"Boards backfill completed for page {page}",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "next_page": page + 1,
            },
        )
        yield page + 1
    else:
        log.debug(
            f"Boards backfill completed for page {page} - no more qualifying boards",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "reason": "no_more_boards",
            },
        )
        return


async def fetch_items_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Item | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    window_start = log_cursor
    now = datetime.now(UTC)
    window_end = min(window_start + MAX_WINDOW_SIZE, now)

    window_has_updates = False
    max_updated_at_in_window = window_start
    item_ids_to_fetch: set[str] = set()

    minimal_items_checked = 0
    async for item in fetch_items_minimal(http, log):
        minimal_items_checked += 1

        if window_start <= item.updated_at <= window_end:
            if item.state == "deleted":
                item.meta_ = Item.Meta(op="d")
                window_has_updates = True
                max_updated_at_in_window = max(
                    max_updated_at_in_window, item.updated_at
                )
                yield item
            else:
                item_ids_to_fetch.add(item.id)
                max_updated_at_in_window = max(
                    max_updated_at_in_window, item.updated_at
                )

    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        window_start.replace(microsecond=0).isoformat(),
        window_end.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "delete" in activity_log.event:
            deleted_item = Item(
                id=activity_log.resource_id,
                updated_at=created_at,
                state="deleted",
                _meta=Item.Meta(op="d"),
            )
            window_has_updates = True
            max_updated_at_in_window = max(max_updated_at_in_window, created_at)
            yield deleted_item
        else:
            item_ids_to_fetch.add(activity_log.resource_id)

    items_yielded = 0
    if item_ids_to_fetch:
        async for item in fetch_items_by_ids(
            http,
            log,
            list(item_ids_to_fetch),
        ):
            if window_start <= item.updated_at <= window_end:
                if item.state == "deleted":
                    item.meta_ = Item.Meta(op="d")

                window_has_updates = True
                max_updated_at_in_window = max(
                    max_updated_at_in_window, item.updated_at
                )
                yield item
                items_yielded += 1

    if window_has_updates:
        yield max_updated_at_in_window + timedelta(seconds=1)
    else:
        yield window_end


async def fetch_items_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Item | PageCursor, None]:
    assert page is None or isinstance(page, str)
    assert isinstance(cutoff, datetime)

    await _item_id_cache.populate_if_needed(http, log, page, cutoff)

    cache_info = _item_id_cache.get_cache_info()
    log.debug(
        f"Items backfill: {cache_info['total_items']} items available",
        {
            "total_items": cache_info["total_items"],
            "cutoff": cutoff,
            "page_cursor": page,
        },
    )

    batch_size = 1000
    items_yielded = 0
    max_updated_at = datetime.min.replace(tzinfo=UTC)

    item_ids, cursor = _item_id_cache.get_next_batch(batch_size)

    if item_ids and cursor:
        async for item in fetch_items_by_ids(http, log, item_ids):
            if item.updated_at <= cutoff:
                items_yielded += 1
                max_updated_at = max(max_updated_at, item.updated_at)
                yield item

        cursor_dt = datetime.fromisoformat(cursor)
        if cursor_dt != max_updated_at:
            raise ValueError(
                f"Cursor {cursor} does not match max updated at {max_updated_at}."
            )

        log.debug(f"Yielding checkpoint cursor: {cursor}")
        yield cursor

    processed, total = _item_id_cache.get_progress()
    log.debug(
        f"Items page complete: {items_yielded} items yielded, {processed}/{total} processed from cache"
    )


async def snapshot_resource(
    http: HTTPSession,
    json_path: str,
    query: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    async for resource_data in execute_query(
        FullRefreshResource,
        http,
        log,
        json_path,
        query,
    ):
        yield resource_data
