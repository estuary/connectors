import asyncio
import json
from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator, Dict, Literal

from estuary_cdk.http import HTTPSession

from source_monday.models import GraphQLResponse

API = "https://api.monday.com/v2"


class GraphQLError(RuntimeError):
    """Raised when a GraphQL query returns an error."""

    def __init__(self, errors: list[dict]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


async def execute_query(
    http: HTTPSession, log: Logger, query: str, variables: Dict[str, Any] = None
) -> GraphQLResponse:
    """Execute a GraphQL query and return the response."""

    response = GraphQLResponse.model_validate_json(
        await http.request(
            log,
            API,
            method="POST",
            json={"query": query, "variables": variables}
            if variables
            else {"query": query},
        )
    )

    if response.errors:
        raise GraphQLError(response.errors)

    return response


async def fetch_recently_updated(
    resource: Literal["board", "item"],
    http: HTTPSession,
    log: Logger,
    start: datetime,
) -> list[int]:
    """
    Fetch IDs of recently updated resources.
    Returns parent item IDs even when only subitems were updated.
    """
    data = await execute_query(http, log, ACTIVITY_LOGS, {"start": start})

    boards = data.data.get("boards", [])
    if not boards:
        return datetime.fromisoformat(start), []

    ids = set()

    for board in boards:
        activity_logs_obj = board.get("activity_logs", {})

        if isinstance(activity_logs_obj, dict):
            logs_str = activity_logs_obj.get("activity_logs", [])
            try:
                activity_logs = json.loads(logs_str)
            except json.JSONDecodeError as e:
                log.error(f"Error decoding activity logs: {e}")
                raise
        elif isinstance(activity_logs_obj, list):
            activity_logs = activity_logs_obj
        else:
            activity_logs = []

        for event in activity_logs:
            event_data_str = event.get("data", {})
            entity = event.get("entity")
            try:
                event_data = json.loads(event_data_str)
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse event data: {event_data_str}, error: {e}")
                raise

            id = None
            if resource == "board" and entity == "board":
                id = event_data.get("board_id")
            elif resource == "item" and entity == "pulse":
                id = event_data.get("pulse_id")

            if id is not None:
                try:
                    ids.add(int(id))
                except (ValueError, TypeError) as e:
                    log.error(f"Failed to convert ID to integer: {id}, error: {e}")
                    raise

    return list(ids)


async def fetch_boards(
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


async def fetch_items_by_ids(
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


async def fetch_items_by_boards(
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


async def check_complexity(http: HTTPSession, log: Logger, threshold: int) -> None:
    """Check API complexity and wait if necessary."""
    data = await execute_query(http, log, COMPLEXITY)
    complexity = data["data"]["complexity"]

    if complexity["after"] < threshold:
        wait_time = complexity["reset_in_x_seconds"]
        log.warning(f"Complexity limit. Waiting for reset in {wait_time} seconds.")
        await asyncio.sleep(wait_time)


COMPLEXITY = """
query GetComplexity {
  complexity {
    before
    query
    after
    reset_in_x_seconds
  }
}
"""

# Query for getting activity logs.
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

# Query for getting boards.
BOARDS = """
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10) {
  boards(order_by: $order_by, page: $page, limit: $limit) {
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
      kind
      description
    }
  }
}
"""

# Query for getting teams.
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

# Query for getting users.
USERS = """
query {
  users {
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

# Query for getting tags.
TAGS = """
query {
  tags {
    id
    name
    color
  }
}
"""


# Item fields fragment.
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

# Query for getting items for a list of boards.
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

# Query for getting next page of items for a board.
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

# Query for getting items by their IDs.
ITEMS_BY_IDS = f"""
query GetItemsByIds($ids: [ID!]!, $page: Int = 1, $limit: Int = 20) {{
  items(ids: $ids, page: $page, limit: $limit, newest_first: true) {{
    ...ItemFields
  }}
}}
{_ITEM_FIELDS}
"""
