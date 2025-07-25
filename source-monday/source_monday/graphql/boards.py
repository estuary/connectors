from logging import Logger
from typing import Any, AsyncGenerator
import itertools

from estuary_cdk.http import HTTPSession

from source_monday.graphql.query_executor import GraphQLQueryError, execute_query, BoardNullTracker
from source_monday.models import Board

# Process boards in batches of 100 to balance GraphQL complexity with throughput.
# Monday.com's single query complexity limit is 5M points, and this batch size
# ensures we stay well under that limit while fetching comprehensive board data.
BOARDS_BATCH_SIZE = 100


async def fetch_boards_by_ids(
    http: HTTPSession,
    log: Logger,
    ids: list[str],
) -> AsyncGenerator[Board, None]:
    limit = 100

    for i in range(0, len(ids), limit):
        chunk = ids[i : i + limit]
        variables = {
            "limit": limit,
            "page": 1,
            "ids": chunk,
        }

        try:
            async for board in execute_query(
                Board,
                http,
                log,
                "data.boards.item",
                BOARDS_WITH_KIND,
                variables,
            ):
                yield board
        except GraphQLQueryError as original_error:
            try:
                async for board in fetch_boards_with_retry(http, log, chunk):
                    yield board
            except Exception as e:
                raise e from original_error


async def fetch_boards_minimal(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Board, None]:
    query = """
    query ($limit: Int = 10000, $page: Int = 1, $state: State = all) {
        boards(limit: $limit, page: $page, state: $state, order_by: created_at) {
            id
            name
            state
            updated_at
        }
    }
    """

    page = 1
    limit = 10000

    while True:
        variables = {
            "state": "all",
            "page": page,
            "limit": limit,
        }
        boards_in_page = 0

        async for board in execute_query(
            Board,
            http,
            log,
            "data.boards.item",
            query,
            variables,
        ):
            yield board
            boards_in_page += 1

        if boards_in_page == 0:
            break

        page += 1


async def fetch_boards_paginated(
    http: HTTPSession,
    log: Logger,
    page: int = 1,
    limit: int = 100,
) -> AsyncGenerator[Board, None]:
    variables = {
        "limit": limit,
        "page": page,
        "state": "all",
    }

    log.debug(f"Fetching boards for page {page} with limit {limit}")

    boards_in_page = 0
    try:
        async for board in _try_fetch_boards(http, log, variables):
            boards_in_page += 1
            yield board
    except GraphQLQueryError as e:
        log.error(f"Failed to fetch boards for page {page}: {e}")
        raise

    log.debug(f"Page {page} returned {boards_in_page} boards")


async def fetch_boards_with_retry(
    http: HTTPSession,
    log: Logger,
    ids: list[str],
    page: int = 1,
    limit: int = 100,
) -> AsyncGenerator[Board, None]:
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    page_ids = ids[start_idx:end_idx]

    if not page_ids:
        return

    log.debug(
        f"Fetching boards for page {page}: {len(page_ids)} boards (indices {start_idx}-{end_idx - 1})"
    )

    for batch in itertools.batched(page_ids, BOARDS_BATCH_SIZE):
        batch_list = list(batch)
        chunks_to_process = [batch_list]

        while chunks_to_process:
            current_chunk = chunks_to_process.pop(0)

            try:
                variables = {
                    "limit": len(current_chunk),
                    "page": 1,  # Always 1 since we're using IDs
                    "ids": current_chunk,
                }

                async for board in _try_fetch_boards(http, log, variables):
                    yield board

            except GraphQLQueryError as e:
                if len(current_chunk) == 1:
                    log.error(
                        "Single board cannot be fetched despite retry attempts",
                        {
                            "board_id": current_chunk[0],
                            "error_details": str(e),
                            "attempted_queries": [
                                "with_workspace_kind",
                                "without_workspace_kind",
                            ],
                            "suggestion": "Board may be corrupted or have API access restrictions",
                        },
                    )
                    raise

                mid = len(current_chunk) // 2
                left_chunk = current_chunk[:mid]
                right_chunk = current_chunk[mid:]

                log.debug(
                    "Splitting board fetch chunk to isolate problematic boards",
                    {
                        "original_chunk_size": len(current_chunk),
                        "left_chunk_size": len(left_chunk),
                        "right_chunk_size": len(right_chunk),
                        "error_summary": str(e)[:100] + "..."
                        if len(str(e)) > 100
                        else str(e),
                    },
                )

                chunks_to_process.extend([left_chunk, right_chunk])


async def _try_fetch_boards(
    http: HTTPSession,
    log: Logger,
    variables: dict[str, Any],
) -> AsyncGenerator[Board, None]:
    null_tracker = BoardNullTracker()
    
    try:
        async for board in execute_query(
            Board,
            http,
            log,
            "data.boards.item",
            BOARDS_WITH_KIND,
            variables,
            null_tracker=null_tracker,
        ):
            if board.columns is None:
                null_tracker.track_board_with_null_field(board.id, "columns")
            if board.groups is None:
                null_tracker.track_board_with_null_field(board.id, "groups")
            if board.owners is None:
                null_tracker.track_board_with_null_field(board.id, "owners")
            if board.subscribers is None:
                null_tracker.track_board_with_null_field(board.id, "subscribers")
            if board.views is None:
                null_tracker.track_board_with_null_field(board.id, "views")
            
            yield board
    except GraphQLQueryError as original_error:
        try:
            null_tracker = BoardNullTracker()
            
            async for board in execute_query(
                Board,
                http,
                log,
                "data.boards.item",
                BOARDS_WITHOUT_KIND,
                variables,
                null_tracker=null_tracker,
            ):
                if board.columns is None:
                    null_tracker.track_board_with_null_field(board.id, "columns")
                if board.groups is None:
                    null_tracker.track_board_with_null_field(board.id, "groups")
                if board.owners is None:
                    null_tracker.track_board_with_null_field(board.id, "owners")
                if board.subscribers is None:
                    null_tracker.track_board_with_null_field(board.id, "subscribers")
                if board.views is None:
                    null_tracker.track_board_with_null_field(board.id, "views")
                
                yield board
        except Exception as e:
            log.error(
                f"Failed to fetch boards with IDs {variables.get('ids', [])} using both queries with null detection. Original error: {original_error}"
            )
            raise e from original_error


_BOARD_CORE_FIELDS = """
fragment BoardCoreFields on Board {
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
}
"""

_BOARD_WITH_KIND_FIELDS = (
    """
fragment BoardWithKindFields on Board {
    ...BoardCoreFields
    workspace {
        id
        name
        description
        kind
    }
}
"""
    + _BOARD_CORE_FIELDS
)

BOARDS_WITH_KIND = (
    """
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10, $ids: [ID!], $state: State = all) {
    boards(order_by: $order_by, page: $page, limit: $limit, ids: $ids, state: $state) {
        ...BoardWithKindFields
    }
}
"""
    + _BOARD_WITH_KIND_FIELDS
)

BOARDS_WITHOUT_KIND = (
    """
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10, $ids: [ID!], $state: State = all) {
    boards(order_by: $order_by, page: $page, limit: $limit, ids: $ids, state: $state) {
        ...BoardCoreFields
    }
}
"""
    + _BOARD_CORE_FIELDS
)
