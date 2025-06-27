import json
import asyncio
import time
from logging import Logger
from typing import Any, AsyncGenerator, Dict, Type

from estuary_cdk.http import HTTPSession
from source_monday.models import (
    ActivityLogsResponse,
    ActivityLog,
    GraphQLResponse,
    GraphQLError,
    ResponseObject,
    BoardsResponse,
    Board,
    ItemsByIdResponse,
    ItemsByBoardPageResponse,
    ItemsByBoardResponse,
    Item,
)

API = "https://api.monday.com/v2"


class CursorManager:
    """Manage cursor-based pagination with expiration tracking."""

    def __init__(self):
        self.cursors: Dict[
            str, tuple[str, float]
        ] = {}  # board_id -> (cursor, creation_time)

    def add_cursor(self, board_id: str, cursor: str) -> None:
        """Add a cursor for a board."""
        self.cursors[board_id] = (cursor, time.time())

    def get_cursor(self, board_id: str) -> str | None:
        """Get the current cursor for a board."""
        if board_id in self.cursors:
            return self.cursors[board_id][0]
        return None

    def update_cursor(self, board_id: str, new_cursor: str) -> None:
        """Update cursor while preserving creation time."""
        if board_id in self.cursors:
            _, creation_time = self.cursors[board_id]
            self.cursors[board_id] = (new_cursor, creation_time)

    def remove_cursor(self, board_id: str) -> None:
        """Remove a cursor for a board."""
        self.cursors.pop(board_id, None)

    def get_all_board_ids(self) -> list[str]:
        """Get all board IDs with active cursors."""
        return list(self.cursors.keys())

    def check_expiration_warnings(self, log: Logger) -> None:
        """Check for cursors approaching expiration and log warnings."""
        current_time = time.time()
        for board_id, (cursor, creation_time) in self.cursors.items():
            elapsed_minutes = (current_time - creation_time) / 60
            if elapsed_minutes >= 55:
                log.warning(
                    f"Cursor for board {board_id} approaching expiration",
                    {
                        "board_id": board_id,
                        "elapsed_minutes": round(elapsed_minutes, 1),
                        "remaining_minutes": round(60 - elapsed_minutes, 1),
                        "cursor_creation_time": creation_time,
                    },
                )

    def is_empty(self) -> bool:
        """Check if there are any active cursors."""
        return len(self.cursors) == 0


async def _execute_board_query_with_fallback(
    http: HTTPSession,
    log: Logger,
    variables: Dict[str, Any],
    board_id: str | None = None,
) -> AsyncGenerator[Board, None]:
    """
    Execute board query with fallback from 'kind' field to without 'kind' field.
    Consolidates the fallback logic used in multiple places.
    """
    context = (
        f"board {board_id}" if board_id else f"page {variables.get('page', 'unknown')}"
    )

    try:
        log.debug(f"Trying to fetch {context} with workspace 'kind' field")
        async for board in _fetch_boards(http, log, BOARDS_WITH_KIND, variables):
            yield board
    except GraphQLQueryError as e:
        log.debug(f"Trying to fetch {context} without workspace 'kind' field")
        try:
            async for board in _fetch_boards(http, log, BOARDS_WITHOUT_KIND, variables):
                yield board
        except GraphQLQueryError as e2:
            if board_id:
                log.error(
                    f"Failed to fetch board {board_id} with or without workspace 'kind' field. "
                    f"With kind error: {e.errors}, Without kind error: {e2.errors}. "
                    f"This board will be skipped, which may cause data loss."
                )
                raise GraphQLQueryError(
                    [
                        GraphQLError(
                            message=f"Failed to fetch board {board_id}. Template boards may need special handling. "
                            f"Original errors: {e.errors}, {e2.errors}"
                        )
                    ]
                )
            else:
                # Re-raise the original error for page-level failures
                raise e


async def _process_cursor_pagination(
    cursor_manager: CursorManager,
    http: HTTPSession,
    log: Logger,
    items_limit: int,
    query: str,
) -> AsyncGenerator[Item, None]:
    """
    Handle cursor-based pagination for items with expiration management.
    """
    cursor_loop_iteration = 0

    while not cursor_manager.is_empty():
        cursor_loop_iteration += 1

        log.info(
            f"Starting cursor processing iteration {cursor_loop_iteration}",
            {
                "iteration": cursor_loop_iteration,
                "active_boards": cursor_manager.get_all_board_ids(),
                "active_cursor_count": len(cursor_manager.cursors),
            },
        )

        # Check for cursor expiration warnings
        cursor_manager.check_expiration_warnings(log)

        if cursor_manager.is_empty():
            log.info("No more active cursors, exiting cursor processing loop")
            break

        for board_id in list(cursor_manager.get_all_board_ids()):
            cursor = cursor_manager.get_cursor(board_id)
            if not cursor:
                continue

            variables = {"cursor": cursor, "limit": items_limit}

            cursor_retry_count = 0
            max_cursor_retries = 2
            response = None
            cursor_processing_start = time.time()

            while cursor_retry_count <= max_cursor_retries:
                try:
                    response = await execute_query(
                        ItemsByBoardPageResponse,
                        http,
                        log,
                        query,
                        variables,
                    )
                    cursor_processing_time = time.time() - cursor_processing_start
                    log.debug(
                        f"Successfully fetched next items for board {board_id}",
                        {
                            "board_id": board_id,
                            "cursor_retry_count": cursor_retry_count,
                            "processing_time_ms": round(
                                cursor_processing_time * 1000, 2
                            ),
                        },
                    )
                    break  # Success, exit retry loop
                except GraphQLQueryError as e:
                    cursor_retry_count += 1
                    processing_time = time.time() - cursor_processing_start

                    # Check if this is a cursor or server error
                    is_cursor_error = any(
                        "cursor" in str(err.message).lower() for err in e.errors
                    )
                    is_server_error = any(
                        "internal server error" in str(err.message).lower()
                        for err in e.errors
                    )

                    if is_cursor_error:
                        log.warning(
                            f"Cursor expired/invalid for board {board_id}, removing from processing",
                            {
                                "board_id": board_id,
                                "cursor": cursor,
                                "error_messages": [
                                    str(err.message) for err in e.errors
                                ],
                                "processing_time_ms": round(processing_time * 1000, 2),
                            },
                        )
                        cursor_manager.remove_cursor(board_id)
                        break  # Don't retry cursor errors
                    elif is_server_error and cursor_retry_count <= max_cursor_retries:
                        retry_delay = 5 + (2**cursor_retry_count)
                        log.warning(
                            f"Server error for board {board_id} cursor query, will retry",
                            {
                                "board_id": board_id,
                                "cursor": cursor,
                                "attempt": cursor_retry_count,
                                "max_retries": max_cursor_retries,
                                "retry_delay_s": retry_delay,
                                "error_messages": [
                                    str(err.message) for err in e.errors
                                ],
                                "processing_time_ms": round(processing_time * 1000, 2),
                            },
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        # Re-raise any other GraphQL errors or after max retries
                        log.error(
                            f"Unrecoverable error for board {board_id} cursor query",
                            {
                                "board_id": board_id,
                                "cursor": cursor,
                                "total_attempts": cursor_retry_count,
                                "error_messages": [
                                    str(err.message) for err in e.errors
                                ],
                                "processing_time_ms": round(processing_time * 1000, 2),
                            },
                        )
                        raise

            if response is None:
                log.error(
                    f"Failed to fetch next items for board {board_id} after {max_cursor_retries} retries."
                )
                raise GraphQLQueryError(
                    [
                        GraphQLError(
                            message=f"Failed to fetch next items for board {board_id} after {max_cursor_retries} retries."
                        )
                    ]
                )

            if not response.data or not response.data.next_items_page:
                log.debug(f"Received empty response for board {board_id}")
                cursor_manager.remove_cursor(board_id)
                continue

            items_page = response.data.next_items_page
            if not items_page.items:
                log.info(
                    f"Board {board_id} cursor returned no items, removing from processing",
                    {
                        "board_id": board_id,
                        "cursor": cursor,
                        "reason": "no_items_in_page",
                    },
                )
                cursor_manager.remove_cursor(board_id)
                continue

            items_yielded = 0
            for item in items_page.items:
                yield item
                items_yielded += 1

            next_cursor = response.data.next_items_page.cursor
            if not next_cursor:
                log.info(
                    f"Board {board_id} cursor processing completed - no next cursor",
                    {
                        "board_id": board_id,
                        "items_yielded": items_yielded,
                        "reason": "no_next_cursor",
                    },
                )
                cursor_manager.remove_cursor(board_id)
            else:
                log.debug(
                    f"Board {board_id} cursor updated for next iteration",
                    {
                        "board_id": board_id,
                        "items_yielded": items_yielded,
                        "cursor_age_preserved": True,
                    },
                )
                cursor_manager.update_cursor(board_id, next_cursor)


class GraphQLQueryError(RuntimeError):
    def __init__(self, errors: list[GraphQLError]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


async def execute_query(
    cls: Type[ResponseObject],
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: Dict[str, Any] | None = None,
) -> GraphQLResponse[ResponseObject]:
    attempt = 1

    while True:
        try:
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
                error_details = [GraphQLError.model_validate(e) for e in res["errors"]]
                raise GraphQLQueryError(error_details)

            response = GraphQLResponse[cls].model_validate(res)

            if response.errors:
                raise GraphQLQueryError(response.errors)

            return response

        except Exception as e:
            if attempt == 5:
                log.error(
                    "GraphQL query failed permanently",
                    {
                        "total_attempts": attempt,
                        "final_error": str(e),
                        "variables": variables,
                    },
                )
                raise

            log.warning(
                "GraphQL query failed, will retry",
                {
                    "error": str(e),
                    "attempt": attempt,
                    "query": query,
                    "variables": variables,
                    "next_retry_delay_s": attempt * 2,
                },
            )
            await asyncio.sleep(attempt * 2)
            attempt += 1


async def fetch_activity_logs(
    http: HTTPSession,
    log: Logger,
    start: str,
) -> AsyncGenerator[ActivityLog, None]:
    """
    Fetch IDs of recently updated resources from activity logs.

    Note:
    - Monday.com calls items a "pulse" in the Activity Logs API.
    - For `pulse` resource, this function will return IDs of parent items affected by updates.
    So, if a subitem is updated, the parent item ID will be returned. If a parent item is updated,
    the parent item ID will be returned.
    """
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
                yield activity_log
                logs_in_page += 1
                total_logs_processed += 1

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


async def _fetch_single_board(
    http: HTTPSession,
    log: Logger,
    page: int,
) -> AsyncGenerator[Board, None]:
    variables: Dict[str, Any] = {"limit": 1, "page": page}
    async for board in _execute_board_query_with_fallback(
        http, log, variables, str(page)
    ):
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
    if not response.data:
        return

    boards = getattr(response.data, "boards", None)
    if not boards:
        return

    for board in boards:
        yield board


async def _fetch_items_chunk(
    http: HTTPSession,
    log: Logger,
    chunk: list[str],
) -> AsyncGenerator[Item, None]:
    page = 1
    page_limit = 100

    while True:
        variables = {"ids": chunk, "page": page, "limit": page_limit}
        response = await execute_query(
            ItemsByIdResponse, http, log, ITEMS_BY_IDS, variables
        )

        if not response.data or not response.data.items:
            break

        items_in_page = 0
        for item in response.data.items:
            yield item
            items_in_page += 1

        if items_in_page < page_limit:
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
            async for board in _execute_board_query_with_fallback(
                http, log, variables, board_id
            ):
                yield board


async def _fetch_boards_page_with_fallback(
    http: HTTPSession,
    log: Logger,
    current_page: int,
    current_limit: int,
) -> AsyncGenerator[Board, None]:
    """Fetch a page of boards, with fallback to individual board queries if needed."""
    variables = {
        "limit": current_limit,
        "page": current_page,
    }

    try:
        async for board in _fetch_boards(http, log, BOARDS_WITH_KIND, variables):
            yield board
    except GraphQLQueryError:
        log.info(
            f"Boards query with workspace 'kind' field failed for page {current_page}, "
            f"switching to querying one board at a time."
        )

        # Calculate the starting item index based on the current page and limit
        # For example, if we're on page 139 with limit 5, we start from item 691 (138*5 + 1)
        start_item = ((current_page - 1) * current_limit) + 1

        for i in range(int(current_limit)):
            item_page = start_item + i
            try:
                async for board in _fetch_single_board(http, log, item_page):
                    yield board
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


async def fetch_boards(
    http: HTTPSession,
    log: Logger,
    page: int | None = None,
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
    limit = 10

    if ids:
        # Process IDs in chunks of 100 (API limit)
        for i in range(0, len(ids), 100):
            chunk = ids[i : i + 100]
            async for board in _fetch_boards_chunk(http, log, chunk, limit):
                yield board
        return

    current_page = 1 if not page else page
    current_limit = limit

    while True:
        boards_in_page = 0
        async for board in _fetch_boards_page_with_fallback(
            http, log, current_page, current_limit
        ):
            yield board
            boards_in_page += 1

        if page is not None or boards_in_page < current_limit:
            break

        current_page += 1


async def fetch_boards_minimal(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Board, None]:
    limit = 10000
    query = """
    query ($limit: Int = 10000, $page: Int = 1, $state: State = all) {
        boards(limit: $limit, page: $page, state: $state) {
            id
            name
            state
            updated_at
        }
    }
    """

    page = 1

    while True:
        variables = {"state": "all", "page": page, "limit": limit}
        response = await execute_query(BoardsResponse, http, log, query, variables)

        if not response.data or not response.data.boards:
            break

        boards_in_page = 0
        for board in response.data.boards:
            yield board
            boards_in_page += 1

        if boards_in_page < limit:
            break

        page += 1


async def fetch_items_minimal(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Item, None]:
    items_limit = 500  # Maximum allowed by Monday.com API
    board_page = 1
    cursor_manager = CursorManager()

    while True:
        # Get one board per page to iterate through all boards
        response = await execute_query(
            ItemsByBoardResponse,
            http,
            log,
            """
            query GetMinimalItems($boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {
                boards(limit: $boardsLimit, page: $boardsPage, state: $state) {
                    id
                    state
                    items_page(limit: $itemsLimit, query_params: {
                        order_by: [{
                            column_id: "__last_updated__",
                            direction: desc
                        }]
                    }) {
                        cursor
                        items {
                            id
                            state
                            updated_at
                        }
                    }
                }
            }
            """,
            {
                "boardsLimit": 1,
                "boardsPage": board_page,
                "itemsLimit": items_limit,
            },
        )

        if not response.data or not response.data.boards:
            break

        board = response.data.boards[0]
        if board.state == "deleted":
            board_page += 1
            continue

        items_page = board.items_page
        if not items_page or not items_page.items:
            board_page += 1
            continue

        # Yield items from this board's first page
        for item in items_page.items:
            yield item

        # Add cursor to manager if available
        if items_page.cursor:
            cursor_manager.add_cursor(board.id, items_page.cursor)

        board_page += 1

    # Process all cursors using the helper function
    minimal_cursor_query = """
    query GetNextMinimalItems($cursor: String!, $limit: Int = 500) {
        next_items_page(limit: $limit, cursor: $cursor) {
            cursor
            items {
                id
                state
                updated_at
            }
        }
    }
    """

    async for item in _process_cursor_pagination(
        cursor_manager,
        http,
        log,
        items_limit,
        minimal_cursor_query,
    ):
        yield item


async def fetch_items_by_ids(
    http: HTTPSession,
    log: Logger,
    item_ids: list[str],
) -> AsyncGenerator[Item, None]:
    # Process IDs in chunks of 100 (API limit)
    for i in range(0, len(item_ids), 100):
        chunk = item_ids[i : i + 100]
        async for item in _fetch_items_chunk(http, log, chunk):
            yield item


async def fetch_items_by_boards(
    http: HTTPSession,
    log: Logger,
    page: int,
) -> AsyncGenerator[Item | str, None]:
    cursor_manager = CursorManager()
    itemsLimit = 100

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

    if not response.data:
        log.debug(
            f"Received empty response for page {page} with itemsLimit {itemsLimit}"
        )
        return

    boards = getattr(response.data, "boards", None)
    if not boards:
        log.debug(
            f"Received response with no boards for page {page} with itemsLimit {itemsLimit}"
        )
        return

    board = boards[0]
    board_id = board.id

    if board.state == "deleted":
        log.debug(f"Skipping querying items for deleted board {board.id}")
        yield board_id
        return

    items_page = board.items_page

    if items_page.cursor:
        cursor_manager.add_cursor(board.id, items_page.cursor)
        log.info(
            f"Initialized cursor for board {board.id}",
            {
                "board_id": board.id,
                "cursor": items_page.cursor,
                "initial_items_count": len(items_page.items),
                "cursor_creation_time": time.time(),
            },
        )
    else:
        log.info(
            f"Board {board.id} has no cursor - no further items to fetch",
            {
                "board_id": board.id,
                "initial_items_count": len(items_page.items),
            },
        )

    for item in items_page.items:
        log.debug(f"Yielding item {item.id} from board {board.id}")
        yield item

    # Use the helper function for cursor pagination
    async for item in _process_cursor_pagination(
        cursor_manager,
        http,
        log,
        itemsLimit,
        NEXT_ITEMS,
    ):
        yield item

    log.info(
        f"Finished fetching items for board {board_id}",
        {
            "board_id": board_id,
            "remaining_active_cursors": cursor_manager.get_all_board_ids(),
        },
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
query GetBoardItems($boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 50, $state: State = all) {{
  boards(limit: $boardsLimit, page: $boardsPage, state: $state) {{
    id
    state
    items_page(limit: $itemsLimit, query_params: {{
      order_by: [{{
        column_id: "__last_updated__",
        direction: desc
      }}]
    }}) {{
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
query GetNextItems($cursor: String!, $limit: Int = 200) {{
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
