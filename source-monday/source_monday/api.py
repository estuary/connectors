from typing import AsyncGenerator
from logging import Logger

from estuary_cdk.http import HTTPSession
from source_monday.models import GraphQLDocument
from source_monday.graphql import (
    execute_query,
    BOARDS,
    TEAMS,
    USERS,
    TAGS,
    ITEMS_INITIAL,
    NEXT_ITEMS_PAGE,
)


async def snapshot_boards(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[GraphQLDocument, None]:
    """
    Fetch all boards.

    API Docs: https://developer.monday.com/api-reference/reference/boards
    """
    page = 1
    limit = 1
    variables = {
        "order_by": "created_at",
        "limit": limit,
        "page": page,
    }

    while True:
        response = await execute_query(http, log, BOARDS, variables)

        if not response.data["boards"]:
            log.info("No more boards to fetch.")
            break

        for board in response.data["boards"]:
            log.debug(f"Processing board {board['id']}")
            yield GraphQLDocument.model_validate(board)

        if len(response.data["boards"]) < limit:
            break

        variables["page"] = page + 1
        page += 1


async def snapshot_teams(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[GraphQLDocument, None]:
    """
    Fetch all teams.

    API Docs: https://developer.monday.com/api-reference/reference/teams
    """
    response = await execute_query(http, log, TEAMS)

    for team in response.data["teams"]:
        yield GraphQLDocument.model_validate(team)


async def snapshot_users(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[GraphQLDocument, None]:
    """
    Fetch all users.

    API Docs: https://developer.monday.com/api-reference/reference/users
    """
    page = 1
    limit = 1
    variables = {
        "limit": limit,
        "page": page,
    }

    while True:
        response = await execute_query(http, log, USERS, variables)

        if not response.data["users"]:
            log.info("No more users to fetch.")
            break

        for user in response.data["users"]:
            log.debug(f"Processing user {user['id']}")
            yield GraphQLDocument.model_validate(user)

        if len(response.data["users"]) < limit:
            break

        variables["page"] = page + 1
        page += 1


async def snapshot_tags(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[GraphQLDocument, None]:
    """
    Fetch all tags.

    API Docs: https://developer.monday.com/api-reference/reference/tags
    """
    response = await execute_query(http, log, TAGS)

    for tags in response.data["tags"]:
        yield GraphQLDocument.model_validate(tags)


async def snapshot_items(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[GraphQLDocument, None]:
    """
    Fetch all items across all boards, handling pagination for both boards and items.

    API Docs:
    - https://developer.monday.com/api-reference/reference/items
    - https://developer.monday.com/api-reference/reference/items-page
    """
    variables = {
        "boards_order_by": "created_at",
        "boards_limit": 10,
        "boards_page": 1,
        "items_limit": 10,
        "items_order_by": "__last_updated__",
        "items_order_direction": "asc",
    }

    while True:
        initial_response = await execute_query(http, log, ITEMS_INITIAL, variables)

        if not initial_response.data["boards"]:
            log.warning("No more boards to fetch.")
            break

        for board in initial_response.data["boards"]:
            items_page_cursor = None
            log.warning(f"Processing board {board.get('id')}")

            while True:
                if items_page_cursor:
                    variables["cursor"] = items_page_cursor
                    next_page_response = await execute_query(
                        http, log, NEXT_ITEMS_PAGE, variables
                    )

                    for item in next_page_response.data["next_items_page"]["items"]:
                        yield GraphQLDocument.model_validate(item)

                    items_page_cursor = next_page_response.data["next_items_page"][
                        "cursor"
                    ]
                else:
                    for item in board["items_page"]["items"]:
                        yield GraphQLDocument.model_validate(item)

                    items_page_cursor = board["items_page"]["cursor"]

                if not items_page_cursor:
                    log.warning("No more items in current board.")
                    break

        if len(initial_response.data["boards"]) < variables["boards_limit"]:
            log.warning("No more boards to fetch.")
            break

        variables["boards_page"] += 1
        variables.pop("cursor", None)
