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
    fetch_boards_paginated,
    fetch_items_by_id,
    get_items_from_boards,
)
from source_monday.models import (
    FullRefreshResource,
    Board,
    Item,
    ItemsBackfillCursor,
)
from source_monday.utils import parse_monday_17_digit_timestamp


# Limit incremental sync windows to 2 hours to balance completeness with performance.
# Smaller windows reduce memory usage and improve error recovery, while ensuring
# we don't miss updates due to Monday.com's 10k activity log retention limit per board.
MAX_WINDOW_SIZE = timedelta(hours=2)
DEFAULT_BOARDS_PAGE_SIZE = 100
# This is the number of boards we will fetch items for in a single fetch_page call.
# However, the underlying GraphQL query will fetch items in smaller batches to balance
# complexity and other API limitations.
BOARDS_PER_ITEMS_PAGE = 25


async def fetch_boards_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Board | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    has_updates = False
    board_ids_to_fetch: set[str] = set()
    board_ids: set[str] = set()

    minimal_boards_checked = 0
    async for board in fetch_boards_minimal(http, log):
        board_ids.add(board.id)
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
        list(board_ids),
        log_cursor.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_17_digit_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "board_deleted" in activity_log.event:
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

    if page is None:
        page = 1

    limit = DEFAULT_BOARDS_PAGE_SIZE
    docs_emitted = 0
    boards_fetched = 0

    log.debug(f"Processing page {page} with limit {limit}")

    async for board in fetch_boards_paginated(http, log, page, limit):
        boards_fetched += 1

        if board.updated_at <= cutoff and board.state != "deleted":
            docs_emitted += 1
            yield board

    if boards_fetched > 0:
        log.debug(
            f"Boards backfill completed for page {page}",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "boards_fetched": boards_fetched,
                "next_page": page + 1,
            },
        )
        yield page + 1
    else:
        log.debug(
            f"Boards backfill completed for page {page} - no more pages",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "boards_fetched": boards_fetched,
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

    max_updated_at_in_window = window_start
    item_ids_to_fetch: set[str] = set()
    board_ids_to_backfill: set[str] = set()
    docs_emitted = 0

    board_ids = []
    async for board in fetch_boards_minimal(http, log):
        if board.state != "deleted":
            board_ids.append(board.id)

    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        board_ids,
        window_start.replace(microsecond=0).isoformat(),
        window_end.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_17_digit_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "delete_pulse" in activity_log.event:
            deleted_item = Item(
                id=activity_log.resource_id,
                updated_at=created_at,
                state="deleted",
                _meta=Item.Meta(op="d"),
            )
            max_updated_at_in_window = max(max_updated_at_in_window, created_at)
            docs_emitted += 1
            yield deleted_item
        # TODO(justin): handle other events that might require fetching items (e.g., duplicating a board with items)
        elif "board_state_change" in activity_log.event:
            # When a board is changing state, it can potentially be moved from the trash (deleted)
            # to active or archived state, so we need to backfill items from that board since the
            # activity log does not show item updates and the backfill might not backfill the items
            # if the board was already deleted before the capture started.
            log.debug(
                f"Board state change activity log found for board {activity_log.resource_id} "
                f"(event created_at: {created_at})"
            )
            board_ids_to_backfill.add(activity_log.resource_id)
        else:
            item_ids_to_fetch.add(activity_log.resource_id)

    if item_ids_to_fetch:
        async for item in fetch_items_by_id(
            http,
            log,
            list(item_ids_to_fetch),
        ):
            if window_start <= item.updated_at <= window_end:
                if item.state == "deleted":
                    item.meta_ = Item.Meta(op="d")

                max_updated_at_in_window = max(
                    max_updated_at_in_window, item.updated_at
                )
                docs_emitted += 1
                yield item

    if board_ids_to_backfill:
        # We need to first get the boards and filter them to only those that are not deleted
        # in case the state changed was to "deleted". This is to prevent fetching items from deleted boards
        # which would cause internal server errors.
        async for board in fetch_boards_minimal(http, log):
            if board.id in board_ids_to_backfill and board.state == "deleted":
                board_ids_to_backfill.remove(board.id)

    if board_ids_to_backfill:
        log.debug(
            f"Backfilling items for boards: {board_ids_to_backfill}",
            {
                "board_ids": list(board_ids_to_backfill),
            },
        )

        while True:
            async for item in get_items_from_boards(
                http,
                log,
                list(board_ids_to_backfill),
            ):
                if window_start <= item.updated_at <= window_end:
                    if item.state == "deleted":
                        item.meta_ = Item.Meta(op="d")

                    max_updated_at_in_window = max(
                        max_updated_at_in_window, item.updated_at
                    )
                    docs_emitted += 1
                    yield item

    if docs_emitted > 0:
        yield max_updated_at_in_window + timedelta(seconds=1)
    else:
        # Advance cursor even without updates to prevent missing data due to Monday.com's
        # 10k activity log retention limit per board. Staying at the same cursor risks
        # missing logs that get purged between sync cycles.
        yield window_end


async def fetch_items_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Item | PageCursor, None]:
    """
    Fetches items for a page of boards using the `BoardItemIterator` for consistent performance
    and complexity control. The `BoardItemIterator` abstracts away Monday's short-lived cursors
    and multiple GraphQL queries necessary to get all items from a board.

    This function uses ItemsBackfillCursor dataclass to provide type safety with methods for
    cursor operations. The cursor tracks board IDs that need processing:

    Example progression:
    1. Initial: ItemsBackfillCursor(boards={"board1": False, "board2": False})
    2. After processing board1: {"board1": null} (merge patch to remove)
    3. Remaining state: ItemsBackfillCursor(boards={"board2": False})
    """

    assert page is None or isinstance(page, dict)
    assert isinstance(cutoff, datetime)

    total_items_count = 0
    emitted_items_count = 0

    if page is None:
        board_ids = []
        async for board in fetch_boards_minimal(http, log):
            # Cannot query items for deleted boards, so we skip them
            # otherwise, the API returns ambiguous internal server errors.
            if board.updated_at <= cutoff and board.state != "deleted":
                board_ids.append(board.id)

        if not board_ids:
            log.debug("No boards found for items backfill")
            return

        cursor = ItemsBackfillCursor.from_board_ids(board_ids)
        # Emit a cursor to checkpoint the complete dicionary of boards
        yield cursor.create_initial_cursor()
    else:
        # Create typed cursor from existing page data
        cursor = ItemsBackfillCursor.from_cursor_dict(page)

    board_ids = cursor.get_next_boards(BOARDS_PER_ITEMS_PAGE)

    if not board_ids:
        log.debug("No boards to process for items backfill")
        return

    log.debug(
        f"{len(cursor.boards)} boards to process for items backfill. Backfilling items for {len(board_ids)} boards.",
        {
            "cutoff": cutoff.isoformat(),
            "board_ids_for_page": board_ids,
        },
    )

    async for item in get_items_from_boards(http, log, board_ids):
        total_items_count += 1
        if item.updated_at <= cutoff and item.state != "deleted":
            emitted_items_count += 1
            yield item

    log.debug(
        f"Items backfill completed for {len(board_ids)} boards with {emitted_items_count} items out of {total_items_count} total items.",
        {
            "completed_boards": board_ids,
            "remaining_boards": len(cursor.boards) - len(board_ids),
            "emitted_items_count": emitted_items_count,
            "total_items_count": total_items_count,
        },
    )

    completion_patch = cursor.create_completion_patch(board_ids)
    yield completion_patch

    if len(cursor.boards) <= len(board_ids):
        return


async def snapshot_resource_paginated(
    http: HTTPSession,
    name: str,
    query: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    variables = {
        "limit": 100,
        "page": 1,
    }

    while True:
        docs_count = 0

        async for resource_data in execute_query(
            FullRefreshResource,
            http,
            log,
            f"data.{name}.item",
            query,
            variables,
        ):
            yield resource_data
            docs_count += 1

        if docs_count < variables["limit"]:
            log.debug(f"Snapshot completed for {name} with {docs_count} documents")
            break

        variables["page"] += 1


async def snapshot_resource(
    http: HTTPSession,
    name: str,
    query: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    async for resource_data in execute_query(
        FullRefreshResource,
        http,
        log,
        f"data.{name}.item",
        query,
    ):
        yield resource_data
