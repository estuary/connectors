from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from source_monday.graphql import (
    TAGS,
    TEAMS,
    USERS,
    execute_query,
    fetch_activity_logs,
    fetch_boards,
    fetch_boards_minimal,
    fetch_items_by_boards,
    fetch_items_by_ids,
    fetch_items_minimal,
)
from source_monday.models import (
    TeamsResponse,
    UsersResponse,
    TagsResponse,
    User,
    Tag,
    Board,
    Item,
    Team,
)


def _parse_monday_timestamp(timestamp_str: str, log: Logger) -> datetime | None:
    """
    Parse a Monday.com timestamp string into a datetime object.
    This value will be a 17-digit UNIX timestamp which can be converted to milliseconds
    by dividing by 10,000 and 10,000,000 to get seconds.
    """
    try:
        # 17-digit timestamp format
        timestamp_17_digit = int(timestamp_str)
        timestamp_seconds = timestamp_17_digit / 10_000_000
        return datetime.fromtimestamp(timestamp_seconds, tz=UTC)
    except Exception:
        log.warning(f"Unable to parse timestamp: {timestamp_str}")
        raise


async def fetch_boards_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Board | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    has_updates = False
    board_ids_to_fetch: set[str] = set()

    # Phase 1: Check for boards updated since log_cursor
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

    # Phase 2: Process activity logs for deletions and additional updates
    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = _parse_monday_timestamp(activity_log.created_at, log)

        if created_at is None:
            continue

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
                state="deleted",
                updated_at=created_at,
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

    # Phase 3: Fetch full board data for updated boards
    boards_yielded = 0
    if board_ids_to_fetch:
        async for board in fetch_boards(http, log, ids=list(board_ids_to_fetch)):
            if board.updated_at > log_cursor:
                if board.state == "deleted":
                    board.meta_ = Board.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, board.updated_at)
                yield board
                boards_yielded += 1

    # Log final stats
    log.debug(
        "Board incremental sync complete",
        {
            "minimal_boards_checked": minimal_boards_checked,
            "activity_logs_processed": activity_logs_processed,
            "unique_board_ids_to_fetch": len(board_ids_to_fetch),
            "boards_yielded": boards_yielded,
            "has_updates": has_updates,
            "original_cursor": log_cursor,
            "new_cursor": max_updated_at + timedelta(seconds=1)
            if has_updates
            else None,
        },
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
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Board | PageCursor, None]:
    """Fetch a page of boards for backfill."""
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    doc_count = 0

    async for board in fetch_boards(http, log, page=page):
        if board.updated_at < cutoff and board.state != "deleted":
            doc_count += 1
            yield board

    log.debug(
        f"Boards page {page} completed",
        {
            "page": page,
            "qualifying_boards": doc_count,
            "has_more": doc_count > 0,
            "cutoff": cutoff,
        },
    )

    if doc_count > 0:
        yield page + 1
    else:
        log.debug(
            f"Boards backfill completed at page {page}",
            {"final_page": page, "reason": "no_boards_from_api"},
        )


async def fetch_items_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Item | LogCursor, None]:
    """Fetch items that have changed since the log cursor."""
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    has_updates = False
    item_ids_to_fetch: set[str] = set()

    # Phase 1: Check for items updated since log_cursor
    minimal_items_checked = 0
    async for item in fetch_items_minimal(http, log):
        minimal_items_checked += 1

        if item.updated_at > log_cursor:
            if item.state == "deleted":
                item.meta_ = Item.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, item.updated_at)
                yield item
                log.debug(
                    f"Item {item.id} marked as deleted (updated: {item.updated_at})"
                )
            else:
                item_ids_to_fetch.add(item.id)
                max_updated_at = max(max_updated_at, item.updated_at)
                log.debug(
                    f"Item {item.id} added to fetch list (updated: {item.updated_at})"
                )

    # Phase 2: Process activity logs for deletions and additional updates
    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = _parse_monday_timestamp(activity_log.created_at, log)

        if created_at is None:
            continue

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
                state="deleted",
                updated_at=created_at,
            )
            deleted_item.meta_ = Item.Meta(op="d")
            has_updates = True
            max_updated_at = max(max_updated_at, created_at)
            yield deleted_item
            log.debug(
                f"Item {activity_log.resource_id} marked as deleted in activity logs"
            )
        else:
            item_ids_to_fetch.add(activity_log.resource_id)
            log.debug(
                f"Activity log found for item {activity_log.resource_id} "
                f"(event created_at: {created_at})"
            )

    log.debug(f"Total unique item IDs to fetch: {len(item_ids_to_fetch)}")

    # Phase 3: Fetch full item data for updated items
    items_yielded = 0
    if item_ids_to_fetch:
        async for item in fetch_items_by_ids(
            http,
            log,
            list(item_ids_to_fetch),
        ):
            if item.updated_at > log_cursor:
                if item.state == "deleted":
                    item.meta_ = Item.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, item.updated_at)
                yield item
                items_yielded += 1

    # Log final stats
    log.debug(
        "Item incremental sync complete",
        {
            "minimal_items_checked": minimal_items_checked,
            "activity_logs_processed": activity_logs_processed,
            "unique_item_ids_to_fetch": len(item_ids_to_fetch),
            "items_yielded": items_yielded,
            "has_updates": has_updates,
            "original_cursor": log_cursor,
            "new_cursor": max_updated_at + timedelta(seconds=1)
            if has_updates
            else None,
        },
    )

    if has_updates:
        new_cursor = max_updated_at + timedelta(seconds=1)
        log.debug(f"Incremental sync complete. New cursor: {new_cursor}")
        yield new_cursor
    else:
        log.debug("Incremental sync complete. No updates found.")


async def fetch_items_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Item | PageCursor, None]:
    """Fetch a page of items for backfill."""
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    has_board = False
    board_id = None
    items_count = 0
    total_items_seen = 0

    async for item in fetch_items_by_boards(
        http,
        log,
        page=page,
    ):
        if isinstance(item, str):
            has_board = True
            board_id = item
            log.debug(f"Processing board {board_id} on page {page}")
            continue

        total_items_seen += 1

        # For backfill, include items with updated_at < cutoff (and not deleted)
        if item.updated_at < cutoff and item.state != "deleted":
            yield item
            items_count += 1

    if has_board:
        log.debug(
            f"Items backfill completed for board page {page}",
            {
                "board_page": page,
                "board_id": board_id,
                "qualifying_items": items_count,
                "total_items_seen": total_items_seen,
                "next_page": page + 1,
            },
        )
        yield page + 1
    else:
        log.debug(
            f"Items backfill completed at board page {page}",
            {
                "final_board_page": page,
                "reason": "no_board_found",
                "total_items_processed": items_count,
            },
        )


async def snapshot_teams(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Team, None]:
    """Fetch all teams snapshot."""
    response = await execute_query(TeamsResponse, http, log, TEAMS)

    teams_yielded = 0
    if response.data and response.data.teams:
        for team in response.data.teams:
            yield team
            teams_yielded += 1


async def snapshot_users(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[User, None]:
    """Fetch all users snapshot with pagination."""
    limit = 100
    page = 1
    pages_processed = 0
    total_users_yielded = 0

    while True:
        variables = {"limit": limit, "page": page}
        response = await execute_query(
            UsersResponse,
            http,
            log,
            USERS,
            variables,
        )

        if not response.data or not response.data.users:
            break

        users_in_page = 0
        for user in response.data.users:
            yield user
            users_in_page += 1
            total_users_yielded += 1

        pages_processed += 1

        if len(response.data.users) < limit:
            break

        page += 1


async def snapshot_tags(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Tag, None]:
    """Fetch all tags snapshot."""
    response = await execute_query(TagsResponse, http, log, TAGS)

    tags_yielded = 0
    if response.data and response.data.tags:
        for tag in response.data.tags:
            yield tag
            tags_yielded += 1
