import json
import asyncio
import time
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


async def fetch_activity_log_ids(
    resource: Literal["board", "pulse"],
    http: HTTPSession,
    log: Logger,
    start: str,
) -> list[str] | None:
    """
    Fetch IDs of recently updated resources from activity logs.

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
    total_logs_processed = 0
    max_logs_limit = 10000  # Monday.com API total log limit

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

        logs_in_page = 0
        for board in response.data.boards:
            if not board.activity_logs:
                continue
            for activity_log in board.activity_logs:
                logs_in_page += 1
                total_logs_processed += 1
                
                id = activity_log.data.get(f"{resource}_id")

                if id is not None and not activity_log.event.startswith("delete_"):
                    try:
                        ids.add(str(id))
                    except (ValueError, TypeError) as e:
                        log.error(f"Failed to convert ID to string: {id}, error: {e}")
                        raise
        
        if logs_in_page == 0:
            break

    # If we processed 10,000 logs, there might be more that we can't fetch.
    # Monday.com will only return the first 10,000 activity logs total with or without pagination.
    # If we reach this limit, we raise an error to prevent data loss. The incremental sync
    # interval should be reduced to potentially avoid this in the future.
    if total_logs_processed >= max_logs_limit:
        log.error(
            f"Retrieved {total_logs_processed} activity logs, which meets or exceeds the Monday.com API limit of {max_logs_limit}. "
            f"There may be additional changes that cannot be fetched."
        )
        raise RuntimeError(
            f"Activity log limit of {max_logs_limit} reached. This indicates there are too many changes "
            f"in the current sync interval. To avoid missing data, please reduce the sync interval "
            f"(e.g., from 5 minutes to 1 minute) to ensure fewer than {max_logs_limit} changes occur between syncs."
        )

    log.debug(f"Processed {total_logs_processed} activity logs, found {len(ids)} unique {resource} IDs")
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
        if board.state != "deleted":
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
            except GraphQLQueryError as e:
                log.info(
                    f"Trying to fetch board {board_id} without workspace 'kind' field"
                )
                try:
                    async for board in _fetch_boards(
                        http, log, BOARDS_WITHOUT_KIND, variables
                    ):
                        yield board
                except GraphQLQueryError as e2:
                    log.error(
                        f"Failed to fetch board {board_id} with or without workspace 'kind' field. "
                        f"With kind error: {e.errors}, Without kind error: {e2.errors}. "
                        f"This board will be skipped, which may cause data loss."
                    )
                    # Re-raise to ensure the error is not silently ignored
                    raise GraphQLQueryError(
                        [
                            GraphQLError(
                                message=f"Failed to fetch board {board_id}. Template boards may need special handling. "
                                f"Original errors: {e.errors}, {e2.errors}"
                            )
                        ]
                    )


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
    board_cursors: dict[str, tuple[str, float]] = {}  # board_id -> (cursor, creation_time)

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
        if board.state != "deleted":
            board_cursors[board.id] = (items_page.cursor, time.time())
    else:
        log.debug(f"Board {board.id} has no cursor.")

    for item in items_page.items:
        log.debug(f"Yielding item {item.id} from board {board.id}")
        yield item

    # Monday returns a cursor for each board's items_page. We use this cursor to fetch next items.
    # Note that the cursor will expire in 60 minutes.
    while board_cursors:
        log.debug(f"Fetching next items for boards: {board_cursors.keys()}")
        current_time = time.time()
        
        # Warn about cursors approaching expiration
        for b_id in list(board_cursors.keys()):
            _, creation_time = board_cursors[b_id]
            elapsed_minutes = (current_time - creation_time) / 60
            if elapsed_minutes > 50:
                log.warning(
                    f"Cursor for board {b_id} has been active for {elapsed_minutes:.1f} minutes. "
                    f"Monday's item cursors expire at 60 minutes. Consider reducing batch sizes if processing is slow."
                )
        
        if not board_cursors:
            break
            
        for b_id, (cur, creation_time) in list(board_cursors.items()):
            log.debug(f"Board {b_id} has cursor: {cur}")
            variables = {
                "limit": itemsLimit,
                "cursor": cur,
                "boardId": b_id,
            }

            try:
                response = await execute_query(
                    ItemsByBoardPageResponse,
                    http,
                    log,
                    NEXT_ITEMS,
                    variables,
                )
            except GraphQLQueryError as e:
                # Check if this is a cursor expiration error
                if any("cursor" in str(err.message).lower() for err in e.errors):
                    raise RuntimeError(
                        f"Cursor for board {b_id} has expired. This would result in missing items. "
                        f"The connector must be restarted to avoid data loss. "
                        f"Consider reducing batch sizes or increasing processing speed. Error: {e.errors}"
                    )
                else:
                    # Re-raise any other GraphQL errors
                    raise
                    
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
                board_cursors[b_id] = (next_cursor, creation_time)

    log.debug(
        f"Finished fetching items for board {board_id}. Remaining cursors: {board_cursors.keys()}"
    )
    # Yield the board ID in case we did not have items to yield for this board.
    # This ensures that the caller continues to the next page of boards.
    yield board_id


ACTIVITY_LOGS = """
query GetActivityLogs($start: ISO8601DateTime!, $limit: Int = 5, $page: Int = 1, $state: State = all) {
  boards(limit: $limit, page: $page, state: $state) {
    id
    state
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
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10, $ids: [ID!], $state: State = all) {
  boards(order_by: $order_by, page: $page, limit: $limit, ids: $ids, state: $state) {
    id
    state
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
query GetBoardItems($boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {{
  boards(limit: $boardsLimit, page: $boardsPage, state: $state) {{
    id
    state
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
query GetNextItems($cursor: String!, $limit: Int = 500) {{
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
query GetItemsByIds($ids: [ID!]!, $page: Int = 1, $limit: Int = 100) {{
  items(ids: $ids, page: $page, limit: $limit, newest_first: true) {{
    ...ItemFields
  }}
}}
{_ITEM_FIELDS}
"""
