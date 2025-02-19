import asyncio
import json
from datetime import datetime
from typing import Dict, Any, Literal
from logging import Logger

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
                continue
        elif isinstance(activity_logs_obj, list):
            activity_logs = activity_logs_obj
        else:
            activity_logs = []

        for event in activity_logs:
            event_data_str = event.get("data", {})
            try:
                event_data = json.loads(event_data_str)
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse event data: {event_data_str}, error: {e}")
                continue

            if resource == "board":
                value = event_data.get("board_id")
            elif resource == "item":
                # Get both the pulse_id (item_id) and parent_item_id
                value = event_data.get("pulse_id")
                parent_id = event_data.get("parent_item_id")
                # If this is a subitem (has parent_id), use the parent_id instead
                if parent_id is not None:
                    value = parent_id

            if value is not None:
                try:
                    ids.add(int(value))
                except (ValueError, TypeError) as e:
                    log.error(f"Failed to convert ID to integer: {value}, error: {e}")
                    continue

    return list(ids)


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
# This query retrieves activity logs for boards.
ACTIVITY_LOGS = """
query GetActivityLogs($start: ISO8601DateTime!) {
  boards {
    activity_logs(from: $start) {
      id
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
# This query retrieves all fields that the Monday API returns for teams.
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
# This query returns a comprehensive set of user fields.
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
# This query returns all available tag fields from Monday.
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

# Query for getting items for a board.
# This query returns all items for a given board.
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
