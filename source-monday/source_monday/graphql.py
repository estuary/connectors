import asyncio
import re
from datetime import datetime, UTC
from dataclasses import dataclass, field
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Type,
    TypeVar,
)


from pydantic import BaseModel

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from source_monday.models import (
    ActivityLog,
    Board,
    BoardItems,
    Item,
    ItemsPage,
    GraphQLResponseRemainder,
    GraphQLError,
)
from source_monday.utils import merge_async_generators, parse_monday_timestamp

API = "https://api.monday.com/v2"


TGraphqlData = TypeVar("TGraphqlData", bound=BaseModel)


@dataclass
class QueryComplexityCache:
    max_complexity_used = 0
    next_query_delay = 0
    query_complexity_map: Dict[str, int] = field(default_factory=dict)

    def set_complexity(self, query_name: str, complexity: int) -> None:
        if complexity < 0:
            raise ValueError("Complexity must be a non-negative integer.")
        self.query_complexity_map[query_name] = max(
            self.query_complexity_map.get(query_name, 0), complexity
        )
        self.max_complexity_used = max(
            self.max_complexity_used, self.query_complexity_map[query_name]
        )

    def get_complexity(self, query_name: str) -> int:
        return self.query_complexity_map.get(query_name, 0)


class GraphQLQueryError(RuntimeError):
    def __init__(self, errors: list[GraphQLError]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


complexity_cache = QueryComplexityCache()


async def execute_query(
    cls: Type[TGraphqlData],
    http: HTTPSession,
    log: Logger,
    json_path: str,
    query: str,
    variables: Dict[str, Any] | None = None,
) -> AsyncGenerator[TGraphqlData, None]:
    attempt = 1

    modified_query = query

    normalized_query = " ".join(query.split())

    # Pattern to match the opening brace of the query body
    # This handles: query { ... } or query Name { ... } or query Name($var: Type) { ... }
    pattern = r"(query(?:\s+\w+)?(?:\s*\([^)]*\))?\s*\{\s*)"
    match = re.search(pattern, normalized_query, re.IGNORECASE)

    if match:
        insert_pos = match.end()
        complexity_field = "complexity { query after reset_in_x_seconds } "
        modified_query = (
            normalized_query[:insert_pos]
            + complexity_field
            + normalized_query[insert_pos:]
        )
    else:
        log.error(
            "Failed to modify query to include complexity field",
            {"original_query": normalized_query},
        )
        raise ValueError("Query modification failed. Cannot find query body.")

    if complexity_cache.next_query_delay > 0:
        log.info(
            "Delaying query execution to respect Monday's complexity budget.",
            {
                "next_query_delay_seconds": complexity_cache.next_query_delay,
                "max_complexity_used": complexity_cache.max_complexity_used,
            },
        )
        await asyncio.sleep(complexity_cache.next_query_delay)
        complexity_cache.next_query_delay = 0

    while True:
        try:
            _, body = await http.request_stream(
                log,
                API,
                method="POST",
                json=(
                    {"query": modified_query, "variables": variables}
                    if variables
                    else {"query": modified_query}
                ),
            )

            processor = IncrementalJsonProcessor(
                body(),
                json_path,
                cls,
                GraphQLResponseRemainder,
            )

            async for item in processor:
                yield item

            # Process the remainder to check for errors and extract complexity
            remainder = processor.get_remainder()

            if remainder.has_errors():
                raise GraphQLQueryError(remainder.get_errors())

            if remainder.data and remainder.data.complexity:
                # Using the json_path as the query name is a simplification
                # and safer assumption for uniquely tracking queries complexity
                # since the data structure of the query response is the same
                complexity_cache.set_complexity(
                    json_path, remainder.data.complexity.query
                )

                log.info(
                    "GraphQL query executed successfully",
                    {
                        "complexity": remainder.data.complexity.query,
                        "reset_in_seconds": remainder.data.complexity.reset_in_x_seconds,
                        "after": remainder.data.complexity.after,
                    },
                )

                # Update the next query delay based on whether the remaining complexity
                # budget is within the maximum used complexity.
                # TODO(justin): Assess whether this needs to be more sophisticated and distinquish between backfill and incremental queries.
                if (
                    remainder.data.complexity.after
                    <= complexity_cache.max_complexity_used
                ):
                    log.debug(
                        f"Remaining complexity budget is not sufficient for next query. Delaying next query by {remainder.data.complexity.reset_in_x_seconds} seconds to allow the budget to reset."
                    )
                    complexity_cache.next_query_delay = (
                        remainder.data.complexity.reset_in_x_seconds
                    )

            return

        except GraphQLQueryError as e:
            # Check if this is a complexity-related error and handle it specially
            # TODO: This is likely a query that is too complex for Monday's API and will never succeed.
            complexity_error = False
            for error in e.errors:
                if (
                    "ComplexityException" in str(error)
                    or "complexity" in str(error).lower()
                ):
                    complexity_error = True
                    log.warning(
                        f"Hit complexity limit during query execution (attempt {attempt})",
                        {
                            "error": str(error),
                            "will_retry_in_seconds": 60,
                        },
                    )

                    # Delaying for 60 seconds to fully reset the complexity budget
                    await asyncio.sleep(60)
                    break

            if not complexity_error and attempt == 5:
                log.error(
                    "GraphQL streaming query failed permanently",
                    {
                        "total_attempts": attempt,
                        "final_error": str(e),
                        "variables": variables,
                        "json_path": json_path,
                        "response_model": cls.__name__,
                    },
                )
                raise

            if not complexity_error:
                log.warning(
                    "GraphQL streaming query failed, will retry",
                    {
                        "error": str(e),
                        "attempt": attempt,
                        "query": modified_query,
                        "variables": variables,
                        "json_path": json_path,
                        "response_model": cls.__name__,
                        "next_retry_delay_s": attempt * 2,
                    },
                )
                await asyncio.sleep(attempt * 2)

            attempt += 1

        except Exception as e:
            if attempt == 5:
                log.error(
                    "GraphQL streaming query failed permanently",
                    {
                        "total_attempts": attempt,
                        "final_error": str(e),
                        "variables": variables,
                        "json_path": json_path,
                        "response_model": cls.__name__,
                    },
                )
                raise

            log.warning(
                "GraphQL streaming query failed, will retry",
                {
                    "error": str(e),
                    "attempt": attempt,
                    "query": modified_query,
                    "variables": variables,
                    "json_path": json_path,
                    "response_model": cls.__name__,
                    "next_retry_delay_s": attempt * 2,
                },
            )

            await asyncio.sleep(attempt * 2)
            attempt += 1


async def fetch_activity_logs(
    http: HTTPSession,
    log: Logger,
    start: str,
    end: str | None = None,
) -> AsyncGenerator[ActivityLog, None]:
    assert start != "", "Start must not be empty"

    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    start = start_dt.isoformat().replace("+00:00", "Z")

    if end is None:
        end_dt = datetime.now(tz=UTC)
        end = end_dt.isoformat().replace("+00:00", "Z")
    else:
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))

    log.debug("Pre-fetching board IDs for activity log queries")
    board_ids = [board.id async for board in fetch_boards_minimal(http, log)]
    log.debug(f"Found {len(board_ids)} total boards to query for activity logs")

    # Process boards in batches with high per-board activity log limits
    # Monday.com's 10k limit log limit is a PER BOARD storage cap, not per request
    # We can potentially get: boards_per_batch × activity_logs_per_board total logs
    boards_per_batch = 5000
    activity_logs_per_board = 100

    total_logs_processed = 0

    log.debug(
        f"Processing activity logs from {start} to {end} with "
        f"{boards_per_batch} boards per batch, {activity_logs_per_board} logs per board"
    )

    for i in range(0, len(board_ids), boards_per_batch):
        batch_board_ids = board_ids[i : i + boards_per_batch]
        batch_number = (i // boards_per_batch) + 1
        total_batches = (len(board_ids) + boards_per_batch - 1) // boards_per_batch

        log.debug(
            f"Processing board batch {batch_number}/{total_batches} with {len(batch_board_ids)} boards"
        )

        batch_logs_count = 0
        async for activity_log in _fetch_activity_logs_for_board_batch(
            http, log, batch_board_ids, start, end, activity_logs_per_board
        ):
            yield activity_log
            batch_logs_count += 1
            total_logs_processed += 1

        log.debug(
            f"Batch {batch_number}/{total_batches}: retrieved {batch_logs_count} activity logs"
        )

    log.info(f"Total activity logs processed: {total_logs_processed}")


async def _fetch_activity_logs_for_board_batch(
    http: HTTPSession,
    log: Logger,
    board_ids: list[str],
    start_time: str,
    end_time: str,
    activity_logs_per_board: int,
) -> AsyncGenerator[ActivityLog, None]:
    """
    Fetch activity logs for a batch of boards.

    The key insight: limit argument is PER BOARD, not total across request.
    So with 5000 boards and limit:100, we can potentially get 500,000 logs in one request.
    This is why we use the IncrementalJsonProcessor to handle large responses efficiently.
    """
    page = 1

    while True:
        variables: dict[str, Any] = {
            "start": start_time,
            "end": end_time,
            "ids": board_ids,
            "limit": len(board_ids),
            "activity_limit": activity_logs_per_board,
            "page": page,
        }
        logs_in_page = 0

        async for activity_log in execute_query(
            ActivityLog,
            http,
            log,
            "data.boards.activity_logs.item",
            ACTIVITY_LOGS,
            variables,
        ):
            try:
                log_time = parse_monday_timestamp(activity_log.created_at, log)
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

                # Double-check the log is within our time bounds
                if start_dt <= log_time <= end_dt:
                    yield activity_log
                    logs_in_page += 1

            except (ValueError, TypeError) as e:
                if activity_log.created_at is not None:
                    log.warning(
                        f"Failed to parse activity log timestamp {activity_log.created_at}: {e}"
                    )
                continue

        if logs_in_page == 0:
            log.debug(
                f"No activity logs found for page {page} with start {start_time} and end {end_time}"
            )
            break

        page += 1
        log.debug(f"Page {page - 1}: processed {logs_in_page} activity logs")


async def fetch_boards_minimal(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Board, None]:
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

        if boards_in_page < limit:
            break

        page += 1


async def fetch_boards_by_ids(
    http: HTTPSession,
    log: Logger,
    ids: list[str],
) -> AsyncGenerator[Board, None]:
    limit = 100

    for i in range(0, len(ids), limit):  # API limit is 100 IDs per request
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
        except GraphQLQueryError:
            async for board in fetch_boards_with_retry(http, log, chunk):
                yield board


async def _try_fetch_boards(
    http: HTTPSession,
    log: Logger,
    variables: dict[str, Any],
) -> AsyncGenerator[Board, None]:
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
    except GraphQLQueryError:
        async for board in execute_query(
            Board,
            http,
            log,
            "data.boards.item",
            BOARDS_WITHOUT_KIND,
            variables,
        ):
            yield board


async def fetch_boards_with_retry(
    http: HTTPSession,
    log: Logger,
    ids: list[str] | None = None,
    page: int = 1,
    limit: int = 1000,
) -> AsyncGenerator[Board, None]:
    if ids is None:
        variables = {
            "limit": limit,
            "page": page,
        }

        async for board in _try_fetch_boards(http, log, variables):
            yield board

        return

    # Maximum number of IDs Monday API allows in a single page request
    if len(ids) > 100:
        mid = len(ids) // 2
        left_chunk = ids[:mid]
        right_chunk = ids[mid:]

        log.debug(
            f"Dividing chunk of {len(ids)} boards into chunks of {len(left_chunk)} and {len(right_chunk)} due to API limits."
        )

        left_gen = fetch_boards_with_retry(http, log, left_chunk)
        right_gen = fetch_boards_with_retry(http, log, right_chunk)

        async for board in merge_async_generators(left_gen, right_gen):
            yield board
        return

    try:
        variables = {
            "limit": len(ids),
            "page": 1,
            "ids": ids,
        }
        async for board in _try_fetch_boards(http, log, variables):
            yield board

        return
    except GraphQLQueryError:
        if len(ids) == 1:
            log.error(
                f"Board {ids[0]} cannot be fetched with or without workspace kind field. This may be an issue with the board or Monday's API."
            )
            raise

        # Divide and conquer: continuously split the list of IDs in half
        mid = len(ids) // 2
        left_chunk = ids[:mid]
        right_chunk = ids[mid:]

        log.debug(
            f"Dividing chunk of {len(ids)} boards into chunks of {len(left_chunk)} and {len(right_chunk)}"
        )

        left_gen = fetch_boards_with_retry(http, log, left_chunk)
        right_gen = fetch_boards_with_retry(http, log, right_chunk)

        async for board in merge_async_generators(left_gen, right_gen):
            yield board


async def fetch_items_by_ids(
    http: HTTPSession,
    log: Logger,
    item_ids: list[str],
) -> AsyncGenerator[Item, None]:
    # API accepts maximum 100 IDs per request
    limit = 100
    chunk_count = (len(item_ids) + limit - 1) // limit
    log.debug(f"Fetching items by IDs in {chunk_count} chunks of {limit} each.")

    chunk_generators = []
    for i in range(0, len(item_ids), limit):
        chunk = item_ids[i : i + limit]
        chunk_number = (i // limit) + 1
        chunk_generators.append(
            _fetch_items_chunk(http, log, chunk, chunk_number, chunk_count)
        )

    async for item in merge_async_generators(*chunk_generators):
        yield item


async def _fetch_items_chunk(
    http: HTTPSession,
    log: Logger,
    chunk: list[str],
    chunk_number: int,
    total_chunks: int,
) -> AsyncGenerator[Item, None]:
    log.debug(f"Fetching items for IDs chunk {chunk_number}/{total_chunks}.")

    variables = {"ids": chunk, "page": 1, "limit": len(chunk)}

    async for item in execute_query(
        Item,
        http,
        log,
        "data.items.item",
        ITEMS_BY_IDS,
        variables,
    ):
        yield item


async def get_non_deleted_board_ids(
    http: HTTPSession,
    log: Logger,
) -> list[int]:
    board_ids = []

    async for board in fetch_boards_minimal(http, log):
        try:
            # Skip boards that are deleted
            # we cannot get items from them - API returns INTERNAL_SERVER_ERROR
            if board.state == "deleted":
                continue

            board_ids.append(int(board.id))
        except ValueError as e:
            log.error(f"Failed to convert board ID {board.id} to integer: {e}.")
            raise

    return board_ids


async def fetch_items_minimal(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Item, None]:
    board_ids = await get_non_deleted_board_ids(http, log)

    if not board_ids:
        log.debug("No board IDs found, returning early")
        return

    batch_size = 100  # Monday API limit for board IDs per request

    for i in range(0, len(board_ids), batch_size):
        batch_ids = board_ids[i : i + batch_size]
        log.debug(
            f"Processing board batch {i // batch_size + 1} with {len(batch_ids)} boards"
        )

        variables = {
            "boardIds": batch_ids,
            "itemsLimit": 500,
        }

        minimal_items_query = """
        query GetMinimalBoardItems($boardIds: [ID!]!, $itemsLimit: Int) {
            boards(ids: $boardIds) {
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
        """

        next_page_query = """
        query GetNextMinimalItemsInWindow($cursor: String!, $limit: Int = 500) {
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

        async for board in execute_query(
            BoardItems,
            http,
            log,
            "data.boards.item",
            minimal_items_query,
            variables,
        ):
            log.debug(
                f"Processing board {board.id} with state {board.state}",
                {"board": board},
            )
            # Skip deleted boards since we cannot fetch items from them
            if board.state == "deleted":
                log.debug(f"Skipping deleted board {board.id}")
                continue

            items_page = board.items_page
            if not items_page or not items_page.items:
                log.debug(f"No items found for board {board.id}.")
                continue

            for item in items_page.items:
                yield item

            cursor = items_page.cursor
            while cursor:
                cursor_variables = {"cursor": cursor, "limit": 500}

                async for next_items in execute_query(
                    ItemsPage,
                    http,
                    log,
                    "data.next_items_page.items_page",
                    next_page_query,
                    cursor_variables,
                ):
                    cursor = next_items.cursor

                    for item in next_items.items:
                        yield item


ACTIVITY_LOGS = """
query ($start: ISO8601DateTime!, $end: ISO8601DateTime!, $ids: [ID!]!, $limit: Int!, $activity_limit: Int!, $page: Int = 1) {
    boards(ids: $ids, limit: $limit, page: $page) {
        activity_logs(from: $start, to: $end, limit: $activity_limit) {
            account_id
            created_at
            data
            entity
            event
            id
            user_id
        }
    }
}
"""

BOARDS_WITH_KIND = """
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
            kind
        }
    }
}
"""

BOARDS_WITHOUT_KIND = """
query ($order_by: BoardsOrderBy = created_at, $page: Int = 1, $limit: Int = 10, $ids: [ID!], $state: State = all) {
    boards(order_by: $order_by, page: $page, limit: $limit, ids: $ids, state: $state) {
        id
        state
        name
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
        }
    }
}
"""

ITEMS_BY_IDS = """
query ($ids: [ID!]!, $limit: Int = 10, $page: Int = 1) {
    items(ids: $ids, limit: $limit, page: $page) {
        id
        name
        state
        created_at
        updated_at
        creator_id
        board {
            id
        }
        group {
            id
            title
            color
        }
        column_values {
            column {
                id
                title
                type
                settings_str
            }
            id
            text
            type
            value
        }
        subitems {
            id
            name
            state
            created_at
            updated_at
            creator_id
            board {
                id
            }
            group {
                id
                title
                color
            }
            column_values {
                column {
                    id
                    title
                    type
                    settings_str
                }
                id
                text
                type
                value
            }
        }
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
