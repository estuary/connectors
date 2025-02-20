from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Union

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from source_monday.graphql import (
    TAGS,
    TEAMS,
    USERS,
    execute_query,
    fetch_boards,
    fetch_items_by_boards,
    fetch_items_by_ids,
    fetch_recently_updated,
)
from source_monday.models import (
    FullRefreshResource,
    IncrementalResource,
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
    async for board in fetch_boards(http, log, limit=limit, ids=updated_ids):
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
    async for board in fetch_boards(http, log, page=page, limit=limit):
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
    async for item in fetch_items_by_ids(
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
    async for item in fetch_items_by_boards(
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


async def snapshot_teams(
    http: HTTPSession,
    _: int,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all teams.

    API Docs: https://developer.monday.com/api-reference/reference/teams
    """
    response = await execute_query(http, log, TEAMS)

    for team in response.data["teams"]:
        yield FullRefreshResource.model_validate(team)


async def snapshot_users(
    http: HTTPSession,
    limit: int,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all users.

    API Docs: https://developer.monday.com/api-reference/reference/users
    """
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
    http: HTTPSession,
    _: int,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    """
    Fetch all tags.

    API Docs: https://developer.monday.com/api-reference/reference/tags
    """
    response = await execute_query(http, log, TAGS)

    for tags in response.data["tags"]:
        yield FullRefreshResource.model_validate(tags)
