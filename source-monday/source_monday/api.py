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
    all_boards: set[str] = set()

    minimal_boards_checked = 0
    async for board in fetch_boards_minimal(http, log):
        all_boards.add(board.id)
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
        list(all_boards),
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
    Fetches items for a page of boards using BoardItemIterator for consistent
    performance and complexity control.

    Since boards can only be sorted by created_at (DESC) but return updated_at,
    we use the updated_at field which reflects when boards were created or modified.
    The page cursor is an ISO 8601 string of the oldest updated_at timestamp
    from previously fetched boards plus one millisecond, allowing us to resume
    from the last page without being affected by changes in board counts.
    """

    assert page is None or isinstance(page, str)
    assert isinstance(cutoff, datetime)

    page_cutoff = datetime.fromisoformat(page) if page else cutoff

    item_iterator = BoardItemIterator(http, log)

    total_items_count = 0
    emitted_items_count = 0

    # Pre-filter boards to avoid fetching items from deleted boards (which causes GraphQL query failures)
    # This also filters out boards that are after our page cursor cutoff, so we only fetch items
    # from boards that are relevant to the current page
    qualifying_board_ids = set()
    max_items_count = 0
    async for board in fetch_boards_minimal(http, log):
        if board.updated_at <= page_cutoff and board.state != "deleted":
            qualifying_board_ids.add(board.id)
            if board.items_count is not None:
                max_items_count = max(max_items_count, board.items_count)
            
            if len(qualifying_board_ids) >= BOARDS_PER_ITEMS_PAGE:
                break


    log.debug(f"Found {len(qualifying_board_ids)} qualifying boards for items backfill")

    items_generator, get_board_cursor = await item_iterator.get_items_from_boards(list(qualifying_board_ids))

    async for item in items_generator:
        total_items_count += 1
        if item.updated_at <= cutoff and item.state != "deleted":
            emitted_items_count += 1
            yield item

    board_cursor = get_board_cursor()

    if board_cursor:
        if board_cursor > page_cutoff or board_cursor > cutoff:
            raise ValueError(
                f"Board cursor {board_cursor} is after cutoff {cutoff} or page cutoff {page_cutoff}. "
                "This may indicate a race condition or unexpected updated_at values in boards."
            )

        # Note: we are subtracting since the board (page cursor) is moving backwards in time
        # to the oldest updated_at until we have no more boards to fetch items from
        next_board_cursor = (board_cursor - timedelta(milliseconds=1)).isoformat()

        log.debug(
            f"Items backfill completed for page {page}",
            {
                "qualifying_items": emitted_items_count,
                "total_items": total_items_count,
                "next_page_cutoff": next_board_cursor,
            },
        )

        yield next_board_cursor


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
