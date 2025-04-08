import json
from logging import Logger
from typing import Any, AsyncGenerator, Dict, Literal

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
        raise GraphQLQueryError([GraphQLError.model_validate(e) for e in res["errors"]])

    response = GraphQLResponse[cls].model_validate(res)

    if response.errors:
        raise GraphQLQueryError(response.errors)

    return response


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
    data = await execute_query(
        ActivityLogsResponse,
        http,
        log,
        ACTIVITY_LOGS,
        {"start": start},
    )

    if not data.data or not data.data.boards:
        return

    ids: set[str] = set()

    for board in data.data.boards:
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


async def fetch_boards(
    http: HTTPSession,
    log: Logger,
    page: int | None = None,
    limit: int | None = None,
    ids: list[str] | None = None,
) -> AsyncGenerator[Board, None]:
    """
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

    current_page = 1 if not page else page
    current_limit = 10 if not limit else limit

    while True:
        try:
            variables = {
                "limit": current_limit,
                "page": current_page,
                "ids": ids,
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


async def _filter_parent_items(
    items: list[Item],
    processed_parent_items: set[str] | None = None,
) -> AsyncGenerator[Item, None]:
    for item in items:
        item_id = str(item.id)
        if processed_parent_items is not None:
            if not item.parent_item and item_id not in processed_parent_items:
                processed_parent_items.add(item_id)
                yield item
        else:
            yield item


async def fetch_items_by_ids(
    http: HTTPSession,
    log: Logger,
    item_ids: list[str],
    limit: int | None = None,
) -> AsyncGenerator[Item, None]:
    if not item_ids:
        raise ValueError("No item IDs provided.")

    page = 1
    while True:
        response = await execute_query(
            ItemsByIdResponse,
            http,
            log,
            ITEMS_BY_IDS,
            {
                "limit": limit,
                "ids": item_ids,
                "page": page,
            },
        )

        if not response.data or not response.data.items:
            return

        async for item in _filter_parent_items(response.data.items):
            yield item

        if limit is not None and len(response.data.items) < limit:
            break

        page += 1


async def fetch_items_by_boards(
    http: HTTPSession,
    log: Logger,
    limit: int | None = None,
    board_ids: list[str] | None = None,
) -> AsyncGenerator[Item, None]:
    """
    Note: If `board_ids` is not provided, items from all boards will be fetched.
    """
    response = await execute_query(
        ItemsByBoardResponse,
        http,
        log,
        ITEMS,
        {
            "limit": limit,
            "boardIds": board_ids,
        },
    )
    if not response.data or not response.data.boards:
        return

    processed_parent_items: set[str] = set()
    board_cursors: dict[str, str] = {}

    for board in response.data.boards:
        items_page = board.items_page
        if items_page.cursor:
            board_cursors[board.id] = items_page.cursor

        async for item in _filter_parent_items(
            items_page.items, processed_parent_items
        ):
            yield item

    # Monday returns a cursor for each board's items_page. We use this cursor to fetch next items.
    # Note that the cursor will expire in 60 minutes.
    while board_cursors:
        for b_id, cur in list(board_cursors.items()):
            variables = {
                "limit": limit,
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
                return

            async for item in _filter_parent_items(
                response.data.next_items_page.items,
                processed_parent_items,
            ):
                yield item

            next_cursor = response.data.next_items_page.cursor
            if not next_cursor:
                board_cursors.pop(b_id)
            else:
                board_cursors[b_id] = next_cursor


ACTIVITY_LOGS = """
query GetActivityLogs($start: ISO8601DateTime!) {
  boards {
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
query GetBoardItems($boardIds: [ID!], $limit: Int = 20) {{
  boards(ids: $boardIds) {{
    id
    items_page(limit: $limit) {{
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
