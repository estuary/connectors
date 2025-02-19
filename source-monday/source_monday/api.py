from datetime import datetime, timedelta
from typing import AsyncGenerator, Union
from logging import Logger

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import LogCursor, PageCursor
from source_monday.models import (
    FullRefreshResource,
    IncrementalResource,
)
from source_monday.graphql import (
    execute_query,
    fetch_recently_updated,
    BOARDS,
    TEAMS,
    USERS,
    TAGS,
    ITEMS,
    NEXT_ITEMS,
    ITEMS_BY_IDS,
)


# Boards functions
async def fetch_boards_changes(
    http: HTTPSession,
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Union[IncrementalResource, LogCursor], None]:
    """
    Fetch changes to boards since the last cursor using activity logs.

    API Docs: https://developer.monday.com/api-reference/reference/boards

    Yields:
        Union[IncrementalResource, LogCursor]: Either a board document or next log cursor
    """
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor

    # First get IDs of recently updated boards from activity logs
    updated_ids = await fetch_recently_updated(
        "board",
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    )

    if not updated_ids:
        return

    has_updates = False
    # Then fetch the full board details for each updated ID
    async for board in _fetch_boards(http, log, limit=limit, ids=updated_ids):
        doc = IncrementalResource.model_validate(board)
        has_updates = True
        max_updated_at = max(max_updated_at, doc.updated_at)
        yield doc

    if not has_updates:
        # If there were no documents, don't update the cursor.
        return
    else:
        # Add 1 second to avoid re-fetching the same records
        yield max_updated_at + timedelta(seconds=1)


async def fetch_boards_page(
    http: HTTPSession,
    limit: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Union[IncrementalResource, PageCursor], None]:
    """
    Fetch a page of boards for backfilling.

    Yields:
        Union[IncrementalResource, PageCursor]: Either a board document or next page token
    """
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    doc_count = 0
    async for board in _fetch_boards(http, log, page=page, limit=limit):
        doc = IncrementalResource.model_validate(board)

        # Only yield boards updated before the cutoff
        if doc.updated_at < cutoff:
            yield doc
            doc_count += 1

    # If we got any results, yield next page token
    if doc_count == limit:
        yield page + 1


# Items functions
async def fetch_items_changes(
    http: HTTPSession,
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Union[IncrementalResource, LogCursor], None]:
    """
    Fetch changes to items since the last cursor using activity logs.

    API Docs: https://developer.monday.com/api-reference/reference/items

    Yields:
        Union[IncrementalResource, LogCursor]: Either a item document or next log cursor
    """
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor

    # First get IDs of recently updated parent items from activity logs
    parent_item_ids = await fetch_recently_updated(
        "item",
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    )

    if not parent_item_ids:
        return

    has_updates = False
    async for item in _fetch_items_by_ids(
        http, log, item_ids=parent_item_ids, limit=limit
    ):
        doc = IncrementalResource.model_validate(item)
        max_updated_at = max(max_updated_at, doc.updated_at)
        has_updates = True
        yield doc

    # Yield new cursor position if we found any updates
    if not has_updates:
        # If there were no documents, don't update the cursor.
        return
    else:
        # Add 1 second to avoid re-fetching the same records
        yield max_updated_at + timedelta(seconds=1)


async def fetch_items_page(
    http: HTTPSession,
    limit: int,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Union[IncrementalResource, PageCursor], None]:
    """
    Fetch a page of items for all boards.

    Yields:
        Union[IncrementalResource, PageCursor]: Either a item document or next page token
    """
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    doc_count = 0
    async for item in _fetch_items_by_boards(
        http,
        log,
        limit=limit,
    ):
        doc = IncrementalResource.model_validate(item)

        # Only yield items updated before the cutoff
        if doc.updated_at < cutoff:
            yield doc
            doc_count += 1

    # If we got any results, yield next page token
    if doc_count == limit:
        yield page + 1


# Helper functions
async def _fetch_boards(
    http: HTTPSession,
    log: Logger,
    page: int | None = None,
    limit: int | None = None,
    ids: list[int] | None = None,
) -> AsyncGenerator[dict, None]:
    """
    Helper function to fetch boards from the Monday.com API.
    Handles pagination and supports filtering by IDs.

    Yields:
        dict: Raw board data from API
    """
    if page is not None and limit is None:
        raise ValueError("limit is required when specifying page")

    variables = {
        "limit": limit,
        "page": page if page is not None else 1,
        "ids": ids,
    }

    while True:
        response = await execute_query(http, log, BOARDS, variables)
        boards = response.data.get("boards", [])

        if not boards:
            break

        for board in boards:
            yield board

        # If fetching specific page or got less than limit, we're done
        if page is not None or (limit and len(boards) < limit):
            break

        variables["page"] += 1


async def _process_items(
    items: list[dict],
    processed_parent_items: set[str] | None = None,
) -> AsyncGenerator[dict, None]:
    """Helper function to process and filter items."""
    for item in items:
        item_id = str(item.get("id"))
        # If we're tracking processed items, only yield unprocessed parent items
        if processed_parent_items is not None:
            if not item.get("parent_item") and item_id not in processed_parent_items:
                processed_parent_items.add(item_id)
                yield item
        else:
            yield item


async def _fetch_items_by_ids(
    http: HTTPSession,
    log: Logger,
    item_ids: list[int],
    limit: int | None = None,
) -> AsyncGenerator[dict, None]:
    """Helper function to fetch items by their IDs."""
    if not item_ids:
        raise ValueError("No item IDs provided.")

    page = 1
    while True:
        response = await execute_query(
            http, log, ITEMS_BY_IDS, {"limit": limit, "ids": item_ids, "page": page}
        )
        items = response.data.get("items", [])
        if not items:
            break

        async for item in _process_items(items):
            yield item

        if len(items) < limit:
            break
        page += 1


async def _fetch_items_by_boards(
    http: HTTPSession,
    log: Logger,
    limit: int | None = None,
    board_ids: list[int] | None = None,
) -> AsyncGenerator[dict, None]:
    """Helper function to fetch items by boards."""
    response = await execute_query(
        http, log, ITEMS, {"limit": limit, "boardIds": board_ids}
    )
    boards_data = response.data.get("boards", [])
    if not boards_data:
        return

    processed_parent_items = set()
    board_cursors = {}

    # Process initial items and collect cursors
    for board in boards_data:
        board_id = board.get("id")
        if not board_id:
            continue

        items_page = board.get("items_page", {})
        if items_page.get("cursor"):
            board_cursors[board_id] = items_page["cursor"]

        async for item in _process_items(
            items_page.get("items", []), processed_parent_items
        ):
            yield item

    # Continue fetching with cursors
    while board_cursors:
        for b_id, cur in list(board_cursors.items()):
            # Fetch and process items for current cursor
            response = await execute_query(
                http,
                log,
                NEXT_ITEMS,
                {"limit": limit, "cursor": cur, "boardId": b_id},
            )
            items = response.data.get("next_items_page", {}).get("items", [])
            async for item in _process_items(items, processed_parent_items):
                yield item

            # Update cursor
            next_cursor = response.data.get("next_items_page", {}).get("cursor")
            if not next_cursor:
                board_cursors.pop(b_id)
            else:
                board_cursors[b_id] = next_cursor


async def snapshot_teams(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all teams.

    API Docs: https://developer.monday.com/api-reference/reference/teams
    """
    response = await execute_query(http, log, TEAMS)

    for team in response.data["teams"]:
        yield FullRefreshResource.model_validate(team)


async def snapshot_users(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all users.

    API Docs: https://developer.monday.com/api-reference/reference/users
    """
    limit = 10
    variables = {
        "limit": limit,
        "page": 1,
    }

    while True:
        response = await execute_query(http, log, USERS, variables)

        if not response.data["users"]:
            break

        for user in response.data["users"]:
            yield FullRefreshResource.model_validate(user)

        if len(response.data["users"]) < limit:
            break

        variables["page"] += 1


async def snapshot_tags(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all tags.

    API Docs: https://developer.monday.com/api-reference/reference/tags
    """
    response = await execute_query(http, log, TAGS)

    for tags in response.data["tags"]:
        yield FullRefreshResource.model_validate(tags)
