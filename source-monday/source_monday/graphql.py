import json
import asyncio
from logging import Logger
from typing import Any, AsyncGenerator, Dict, Literal, Callable

from estuary_cdk.http import HTTPSession

from source_monday.models import (
    GraphQLResponse,
    GraphQLError,
    ResponseObject,
    BoardsResponse,
    Board,
    ActivityLogsResponse,
    ItemsByIdResponse,
    ItemsByBoardPageResponse,
    ItemsByBoardResponse,
    Item,
)

API = "https://api.monday.com/v2"


class GraphQLQueryError(RuntimeError):
    def __init__(self, errors: list[GraphQLError]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


async def execute_query(
    cls: type[ResponseObject],
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: Dict[str, Any] | None = None,
) -> GraphQLResponse[ResponseObject]:
    attempt = 1
    while True:
        try:
            log.debug(f"Executing query {query} with variables: {variables}")
            res: dict[str, Any] = json.loads(
                await http.request(
                    log,
                    API,
                    method="POST",
                    json=(
                        {"query": query, "variables": variables}
                        if variables
                        else {"query": query}
                    ),
                )
            )

            if "errors" in res:
                raise GraphQLQueryError(
                    [GraphQLError.model_validate(e) for e in res["errors"]]
                )

            response = GraphQLResponse[cls].model_validate(res)

            if response.errors:
                raise GraphQLQueryError(response.errors)

            return response
        except Exception as e:
            if attempt == 5:
                raise

            log.warning(
                "failed to execute query (will retry)",
                {
                    "error": str(e),
                    "attempt": attempt,
                    "query": query,
                    "variables": variables,
                },
            )
            await asyncio.sleep(attempt * 2)
            attempt += 1


async def fetch_recently_updated(
    resource: Literal["board", "pulse"],
    http: HTTPSession,
    log: Logger,
    start: str,
) -> list[str] | None:
    """
    Fetch IDs of recently updated resources.

    Note:
    - Monday.com calls items a "pulse" in the Activity Logs API.
    - For `pulse` resource, this function will return IDs of parent items affected by updates.
    So, if a subitem is updated, the parent item ID will be returned. If a parent item is updated,
    the parent item ID will be returned.
    """
    ids: set[str] = set()
    variables: dict[str, Any] = {
        "start": start,
        "limit": 5,
        "page": 1,
    }

    while True:
        response = await execute_query(
            ActivityLogsResponse,
            http,
            log,
            ACTIVITY_LOGS,
            variables,
        )

        if not response.data or not response.data.boards:
            break

        variables["page"] += 1

        for board in response.data.boards:
            if not board.activity_logs:
                continue
            for activity_log in board.activity_logs:
                id = activity_log.data.get(f"{resource}_id")

                if id is not None:
                    try:
                        ids.add(str(id))
                    except (ValueError, TypeError) as e:
                        log.error(f"Failed to convert ID to string: {id}, error: {e}")
                        raise

    return list(ids)


async def _fetch_single_board(
    http: HTTPSession,
    log: Logger,
    page: int,
) -> AsyncGenerator[Board, None]:
    variables: Dict[str, Any] = {"limit": 1, "page": page}
    try:
        log.info(f"Trying to fetch board {page} with workspace 'kind' field")
        async for board in _fetch_boards(http, log, BOARDS_WITH_KIND, variables):
            yield board
    except GraphQLQueryError:
        log.info(f"Trying to fetch board {page} without workspace 'kind' field")
        async for board in _fetch_boards(http, log, BOARDS_WITHOUT_KIND, variables):
            yield board


async def _fetch_boards(
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: Dict[str, Any],
) -> AsyncGenerator[Board, None]:
    response = await execute_query(
        BoardsResponse,
        http,
        log,
        query,
        variables,
    )
    if not response.data or not response.data.boards:
        return

    for board in response.data.boards:
        yield board


async def _fetch_in_chunks[T](
    http: HTTPSession,
    log: Logger,
    ids: list[str],
    chunk_size: int,
    fetch_fn: Callable[
        [HTTPSession, Logger, list[str], int | None], AsyncGenerator[T, None]
    ],
    limit: int | None = None,
) -> AsyncGenerator[T, None]:
    if not ids:
        raise ValueError("No IDs provided.")

    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        async for item in fetch_fn(http, log, chunk, limit):
            yield item


async def _fetch_items_chunk(
    http: HTTPSession,
    log: Logger,
    chunk: list[str],
    limit: int | None = None,
) -> AsyncGenerator[Item, None]:
    page = 1
    while True:
        response = await execute_query(
            ItemsByIdResponse,
            http,
            log,
            ITEMS_BY_IDS,
            {
                "limit": limit,
                "ids": chunk,
                "page": page,
            },
        )

        if not response.data or not response.data.items:
            break

        for item in response.data.items:
            yield item

        if limit is not None and len(response.data.items) < limit:
            break

        page += 1


async def _fetch_boards_chunk(
    http: HTTPSession,
    log: Logger,
    chunk: list[str],
    limit: int | None = None,
) -> AsyncGenerator[Board, None]:
    try:
        variables = {
            "limit": limit,
            "page": 1,
            "ids": chunk,
        }
        async for board in _fetch_boards(http, log, BOARDS_WITH_KIND, variables):
            yield board
    except GraphQLQueryError:
        log.info(
            "Boards query with workspace 'kind' field failed, switching to querying one board at a time."
        )
        for board_id in chunk:
            variables = {
                "limit": 1,
                "page": 1,
                "ids": [board_id],
            }
            try:
                async for board in _fetch_boards(
                    http, log, BOARDS_WITH_KIND, variables
                ):
                    yield board
            except GraphQLQueryError:
                log.info(
                    f"Trying to fetch board {board_id} without workspace 'kind' field"
                )
                async for board in _fetch_boards(
                    http, log, BOARDS_WITHOUT_KIND, variables
                ):
                    yield board


async def fetch_boards(
    http: HTTPSession,
    log: Logger,
    page: int | None = None,
    limit: int | None = None,
    ids: list[str] | None = None,
) -> AsyncGenerator[Board, None]:
    """
    Fetch boards, either by IDs (handling the API's 100-item limit) or paginated.

    Note: If `page` is specified, `limit` is required.
    If `ids` is not provided, all boards will be fetched.

    This function first attempts to query with the 'kind' field in the workspace section.
    If that fails for a page, it switches to querying one board at a time for that page,
    trying with 'kind' first and falling back to without 'kind' if necessary. This is necessary for
    boards that are set up as templates. Template boards error when the workspace 'kind' field is queried
    and there is not a way to query them otherwise.
    """
    if page is not None and limit is None:
        raise ValueError("limit is required when specifying page")

    if ids:
        async for board in _fetch_in_chunks(
            http, log, ids, chunk_size=100, fetch_fn=_fetch_boards_chunk, limit=limit
        ):
            yield board
        return

    current_page = 1 if not page else page
    current_limit = 10 if not limit else limit

    while True:
        try:
            variables = {
                "limit": current_limit,
                "page": current_page,
            }

            boards_in_page = 0
            async for board in _fetch_boards(http, log, BOARDS_WITH_KIND, variables):
                yield board
                boards_in_page += 1

            if page is not None or boards_in_page < current_limit:
                break

        except GraphQLQueryError:
            log.info(
                f"Boards query with workspace 'kind' field failed for page {current_page}, switching to querying one board at a time."
            )

            # Calculate the starting item index based on the current page and limit
            # For example, if we're on page 139 with limit 5, we start from item 691 (138*5 + 1)
            start_item = ((current_page - 1) * current_limit) + 1
            boards_in_page = 0

            for i in range(int(current_limit)):
                item_page = start_item + i

                try:
                    async for board in _fetch_single_board(http, log, item_page):
                        yield board
                        boards_in_page += 1

                except GraphQLQueryError:
                    log.error(
                        f"Failed to fetch board {item_page} with or without workspace 'kind' field"
                    )
                    raise GraphQLQueryError(
                        [
                            GraphQLError(
                                message=f"Failed to fetch board {item_page} with or without workspace 'kind' field"
                            )
                        ]
                    )

            if page is not None or boards_in_page < current_limit:
                break

        current_page += 1


async def fetch_items_by_ids(
    http: HTTPSession,
    log: Logger,
    item_ids: list[str],
    limit: int | None = None,
) -> AsyncGenerator[Item, None]:
    async for item in _fetch_in_chunks(
        http, log, item_ids, chunk_size=100, fetch_fn=_fetch_items_chunk, limit=limit
    ):
        yield item


async def fetch_items_by_boards(
    http: HTTPSession,
    log: Logger,
    page: int,
    itemsLimit: int,
) -> AsyncGenerator[Item | str, None]:
    board_cursors: dict[str, str] = {}

    response = await execute_query(
        ItemsByBoardResponse,
        http,
        log,
        ITEMS,
        {
            "boardsPage": page,
            "boardsLimit": 1,
            "itemsLimit": itemsLimit,
        },
    )

    if not response.data or not response.data.boards:
        log.debug(
            f"Received empty response for page {page} with itemsLimit {itemsLimit}"
        )
        return

    board = response.data.boards[0]
    items_page = board.items_page
    board_id = board.id

    if items_page.cursor:
        board_cursors[board.id] = items_page.cursor
    else:
        log.debug(f"Board {board.id} has no cursor.")

    for item in items_page.items:
        log.debug(f"Yielding item {item.id} from board {board.id}")
        yield item

    # Monday returns a cursor for each board's items_page. We use this cursor to fetch next items.
    # Note that the cursor will expire in 60 minutes.
    while board_cursors:
        log.debug(f"Fetching next items for boards: {board_cursors.keys()}")
        for b_id, cur in list(board_cursors.items()):
            log.debug(f"Board {b_id} has cursor: {cur}")
            variables = {
                "limit": itemsLimit,
                "cursor": cur,
                "boardId": b_id,
            }

            response = await execute_query(
                ItemsByBoardPageResponse,
                http,
                log,
                NEXT_ITEMS,
                variables,
            )
            if not response.data or not response.data.next_items_page:
                log.debug(f"Received empty response for board {b_id}")
                board_cursors.pop(b_id)
                continue

            items_page = response.data.next_items_page
            if not items_page.items:
                log.debug(f"Board {b_id} has no items in next page.")
                board_cursors.pop(b_id)
                continue

            for item in items_page.items:
                yield item

            next_cursor = response.data.next_items_page.cursor
            if not next_cursor:
                log.debug(f"Board {b_id} has no next cursor.")
                board_cursors.pop(b_id)
            else:
                board_cursors[b_id] = next_cursor

    log.debug(
        f"Finished fetching items for board {board_id}. Remaining cursors: {board_cursors.keys()}"
    )
    # Yield the board ID in case we did not have items to yield for this board.
    # This ensures that the caller continues to the next page of boards.
    yield board_id


ACTIVITY_LOGS = """
query GetActivityLogs($start: ISO8601DateTime!, $limit: Int = 5, $page: Int = 1) {
  boards(limit: $limit, page: $page) {
    id
    activity_logs(from: $start) {
      id
      entity
      event
      data
      created_at
    }
  }
}
"""

BOARDS_TEMPLATE = """
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10, $ids: [ID!]) {
  boards(order_by: $order_by, page: $page, limit: $limit, ids: $ids) {
    id
    name
    board_kind
    type
    columns {
      archived
      description
      id
      settings_str
      title
      type
      width
    }
    communication
    description
    groups {
      archived
      color
      deleted
      id
      position
      title
    }
    owners {
      id
    }
    creator {
      id
    }
    permissions
    state
    subscribers {
      id
    }
    tags {
      id
      color
      name
    }
    top_group {
      id
    }
    updated_at
    updates {
      id
    }
    views {
      id
      name
      settings_str
      type
      view_specific_data_str
    }
    workspace {
      id
      name
      description
      %s
    }
  }
}
"""

BOARDS_WITH_KIND = BOARDS_TEMPLATE % "kind"
BOARDS_WITHOUT_KIND = BOARDS_TEMPLATE % ""

TEAMS = """
query {
  teams {
    id
    name
    picture_url
    users {
      id
    }
    owners {
      id
    }
  }
}
"""

USERS = """
query ($limit: Int = 10, $page: Int = 1) {
  users(limit: $limit, page: $page) {
    birthday
    country_code
    created_at
    join_date
    email
    enabled
    id
    is_admin
    is_guest
    is_pending
    is_view_only
    is_verified
    location
    mobile_phone
    name
    phone
    photo_original
    photo_small
    photo_thumb
    photo_thumb_small
    photo_tiny
    time_zone_identifier
    title
    url
    utc_hours_diff
  }
}
"""

TAGS = """
query {
  tags {
    id
    name
    color
  }
}
"""


_ITEM_FIELDS = """
fragment _ItemFields on Item {
  id
  name
  assets {
    created_at
    file_extension
    file_size
    id
    name
    original_geometry
    public_url
    uploaded_by {
      id
    }
    url
    url_thumbnail
  }
  board {
    id
    name
  }
  column_values {
    id
    text
    type
    value
  }
  created_at
  creator_id
  group {
    id
  }
  parent_item {
    id
  }
  state
  subscribers {
    id
  }
  updated_at
  updates {
    id
  }
}
fragment ItemFields on Item {
  ..._ItemFields
  subitems {
    ..._ItemFields
  }
}
"""

ITEMS = f"""
query GetBoardItems($boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 20) {{
  boards(limit: $boardsLimit, page: $boardsPage) {{
    id
    items_page(limit: $itemsLimit) {{
      cursor
      items {{
        ...ItemFields
      }}
    }}
  }}
}}
{_ITEM_FIELDS}
"""

NEXT_ITEMS = f"""
query GetNextItems($cursor: String!, $limit: Int = 20) {{
  next_items_page(limit: $limit, cursor: $cursor) {{
    cursor
    items {{
      ...ItemFields
    }}
  }}
}}
{_ITEM_FIELDS}
"""

ITEMS_BY_IDS = f"""
query GetItemsByIds($ids: [ID!]!, $page: Int = 1, $limit: Int = 20) {{
  items(ids: $ids, page: $page, limit: $limit, newest_first: true) {{
    ...ItemFields
  }}
}}
{_ITEM_FIELDS}
"""
