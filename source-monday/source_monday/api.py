from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from source_monday.graphql import (
    TAGS,
    TEAMS,
    USERS,
    execute_query,
    fetch_recently_updated,
    fetch_boards,
    fetch_items_by_boards,
    fetch_items_by_ids,
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


# Boards functions
async def fetch_boards_changes(
    http: HTTPSession,
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Board | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor

    updated_ids = await fetch_recently_updated(
        "board",
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    )

    if not updated_ids:
        return

    has_updates = False
    async for board in fetch_boards(http, log, limit=limit, ids=updated_ids):
        has_updates = True
        max_updated_at = max(max_updated_at, board.updated_at)
        yield board

    if not has_updates:
        return
    else:
        yield max_updated_at + timedelta(seconds=1)


async def fetch_boards_page(
    http: HTTPSession,
    limit: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Board | PageCursor, None]:
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    doc_count = 0
    async for board in fetch_boards(http, log, page=page, limit=limit):
        if board.updated_at < cutoff:
            yield board
            doc_count += 1

    if doc_count > 0:
        yield page + 1


async def fetch_items_changes(
    http: HTTPSession,
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Item | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor

    item_ids = await fetch_recently_updated(
        "pulse",
        http,
        log,
        log_cursor.replace(microsecond=0).isoformat(),
    )

    if not item_ids:
        return

    has_updates = False
    async for item in fetch_items_by_ids(http, log, item_ids=item_ids, limit=limit):
        has_updates = True
        max_updated_at = max(max_updated_at, item.updated_at)
        yield item

    if not has_updates:
        return
    else:
        yield max_updated_at + timedelta(seconds=1)


async def fetch_items_page(
    http: HTTPSession,
    limit: int,
    log: Logger,
    _: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Item | PageCursor, None]:
    """
    Note: This function does not use the `page` parameter due to how items need to be queried.
    """
    assert isinstance(cutoff, datetime)

    async for item in fetch_items_by_boards(
        http,
        log,
        limit=limit,
    ):
        if item.updated_at < cutoff:
            yield item


async def snapshot_teams(
    http: HTTPSession,
    _: int,
    log: Logger,
) -> AsyncGenerator[Team, None]:
    response = await execute_query(TeamsResponse, http, log, TEAMS)

    if not response.data or not response.data.teams:
        return

    for team in response.data.teams:
        yield team


async def snapshot_users(
    http: HTTPSession,
    limit: int,
    log: Logger,
) -> AsyncGenerator[User, None]:
    variables = {
        "limit": limit,
        "page": 1,
    }

    while True:
        response = await execute_query(
            UsersResponse,
            http,
            log,
            USERS,
            variables,
        )

        if not response.data or not response.data.users:
            break

        for user in response.data.users:
            yield user

        if len(response.data.users) < limit:
            break

        variables["page"] += 1


async def snapshot_tags(
    http: HTTPSession,
    _: int,
    log: Logger,
) -> AsyncGenerator[Tag, None]:
    response = await execute_query(TagsResponse, http, log, TAGS)

    if not response.data or not response.data.tags:
        return

    for tag in response.data.tags:
        yield tag
