import itertools
from logging import Logger
from typing import AsyncGenerator

from aiostream.stream import merge
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel

from source_monday.graphql.query_executor import execute_query
from source_monday.models import GraphQLResponseData, GraphQLResponseRemainder, Item


class ItemsPage(BaseModel, extra="allow"):
    cursor: str | None = None


class BoardItems(BaseModel, extra="allow"):
    updated_at: AwareDatetime
    items_page: ItemsPage | None = None


class ItemsPageRemainderData(GraphQLResponseData, extra="allow"):
    boards: list[BoardItems] | None = None
    next_items_page: ItemsPage | None = None


ItemsPageRemainder = GraphQLResponseRemainder[ItemsPageRemainderData]

# Fetching items by ID is restricted to 100 items per request when using the `items` query.
# This is a limitation of the Monday API, so we need to batch requests accordingly.
# This is only used in the incremental task, so we don't expect to have multiple concurrent fetches
# that often, but we still limit it to avoid overwhelming the API even though the maximum concurrent requests
# to Monday can go higher based on the account's plan.
MAX_CONCURRENT_ITEM_FETCHES = 5
# These limits combined use ~1.8 Million API complexity units per request.
# The max per request is 5 million and 10 million per minute.
BOARDS_PER_PAGE = 4
ITEMS_PER_BOARD = 20
ITEMS_LIMIT_BY_ID = 100


class CursorCollector:
    def __init__(self):
        self.cursors: list[str] = []

    def process(self, log: Logger, remainder: ItemsPageRemainder) -> None:
        log.debug("Processing ItemsPageRemainder for cursors")
        if not remainder.data:
            log.debug("No data in remainder, skipping cursor processing")
            return

        if remainder.data.boards is not None:
            log.debug("Processing boards structure for cursors")
            boards_data = remainder.data.boards
            
            for board_data in boards_data:
                if not board_data.items_page:
                    log.debug(
                        "No items_page in board_data, skipping",
                        {"board_data": board_data},
                    )
                    continue

                log.debug("Found items_page in board_data", {"board_data": board_data})
                cursor = board_data.items_page.cursor

                if cursor:
                    self.cursors.append(cursor)

        if remainder.data.next_items_page is not None:
            log.debug("Processing next_items_page structure for cursors")
            cursor = remainder.data.next_items_page.cursor
            
            if cursor:
                log.debug(f"Found cursor in next_items_page: {cursor}")
                self.cursors.append(cursor)

    def get_result(self, log: Logger) -> list[str]:
        log.debug(f"Returning collected cursors: {self.cursors}")
        return self.cursors


async def fetch_items_by_id(
    http: HTTPSession,
    log: Logger,
    item_ids: list[str],
) -> AsyncGenerator[Item, None]:
    limit = 100
    chunk_count = (len(item_ids) + limit - 1) // limit
    log.debug(f"Fetching items by IDs in {chunk_count} chunks of {limit} each")

    batch_generators: list[AsyncGenerator[Item, None]] = []
    for batch in itertools.batched(item_ids, limit):
        batch_list = list(batch)
        batch_generators.append(
            _fetch_items(http, log, batch_list, len(batch_generators) + 1, chunk_count)
        )

    if batch_generators:
        for i in range(0, len(batch_generators), MAX_CONCURRENT_ITEM_FETCHES):
            concurrent_batch = batch_generators[i : i + MAX_CONCURRENT_ITEM_FETCHES]
            async for item in merge(*concurrent_batch):
                yield item


async def _fetch_items(
    http: HTTPSession,
    log: Logger,
    chunk: list[str],
    chunk_number: int,
    total_chunks: int,
) -> AsyncGenerator[Item, None]:
    log.debug(f"Fetching items for IDs chunk {chunk_number}/{total_chunks}")

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


async def get_items_from_boards(
    http: HTTPSession,
    log: Logger,
    board_ids: list[str],
    items_limit: int = ITEMS_PER_BOARD,
) -> AsyncGenerator[Item, None]:
    if len(board_ids) == 0:
        log.error("get_items_from_boards requires a non-empty list of board IDs.")
        raise ValueError("get_items_from_boards requires a non-empty list of board IDs.")

    log.debug(f"Fetching items for boards {board_ids}.")

    if items_limit is None or items_limit <= 0:
        log.error("Invalid items_limit provided, must provide an integer greater than 0.")
        raise ValueError("Invalid items_limit provided, must provide an integer greater than 0.")

    elif items_limit > ITEMS_PER_BOARD:
        log.warning(
            f"items_limit {items_limit} exceeds maximum of {ITEMS_PER_BOARD}, using {ITEMS_PER_BOARD}."
        )
        items_limit = ITEMS_PER_BOARD

    for batch_ids in itertools.batched(board_ids, BOARDS_PER_PAGE):
        batch_ids_list = list(batch_ids)
        log.debug(f"Processing batch with board IDs: {batch_ids_list}")

        variables = {
            "boardIds": batch_ids_list,
            "boardsLimit": len(batch_ids_list),
            "boardsPage": 1,
            "itemsLimit": items_limit,
            "state": "all",
        }

        batch_cursor_collector = CursorCollector()

        async for item in _stream_all_items_from_page(
            http,
            log,
            ITEMS,
            variables,
            batch_cursor_collector,
            batch_ids_list,
        ):
            yield item

async def _stream_all_items_from_page(
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: dict,
    cursor_collector: CursorCollector,
    board_ids: list[str],
) -> AsyncGenerator[Item, None]:
    """
    Stream all items from boards on this page, handling cursor pagination internally.

    The internal cursor handling is needed because Monday's API uses a cursor-based pagination
    for items within each board, where the cursor is specific to the board's items_page and
    expires after 60-minutes.
    """

    items_yielded = 0
    async for item in execute_query(
        Item,
        http,
        log,
        "data.boards.item.items_page.items.item",
        query,
        variables,
        remainder_cls=ItemsPageRemainder,
        remainder_processor=cursor_collector,
    ):
        items_yielded += 1
        yield item

    cursors = cursor_collector.get_result(log)
    while cursors:
        cursor = cursors.pop(0)
        log.debug(
            f"Processing cursor pagination, remaining cursors: {len(cursors)}"
        )

        cursor_variables = {"cursor": cursor, "limit": ITEMS_LIMIT_BY_ID}
        cur_collector = CursorCollector()

        async for item in execute_query(
            Item,
            http,
            log,
            "data.next_items_page.items.item",
            NEXT_ITEMS,
            cursor_variables,
            remainder_cls=ItemsPageRemainder,
            remainder_processor=cur_collector,
        ):
            items_yielded += 1
            yield item

        next_cursors = cur_collector.get_result(log)
        if next_cursors:
            cursors.extend(next_cursors)

    log.debug(
        "Items fetched.",
        {
            "boards": board_ids,
            "items_yielded": items_yielded,
        },
    )


_ITEM_FIELDS = """
fragment _ItemFields on Item {
  id
  name
  created_at
  updated_at
  creator_id
  state
  assets {
    id
    name
    created_at
    file_extension
    file_size
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
  group {
    id
  }
  parent_item {
    id
  }
  subscribers {
    id
  }
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

ITEMS = (
    """
query GetBoardItems($boardIds: [ID!]!, $boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {
  boards(ids: $boardIds, limit: $boardsLimit, page: $boardsPage, state: $state, order_by: created_at) {
    id
    state
    updated_at
    items_page(limit: $itemsLimit) {
      cursor
      items {
        ...ItemFields
      }
    }
  }
}
"""
    + _ITEM_FIELDS
)

NEXT_ITEMS = (
    """
query GetNextItems($cursor: String!, $limit: Int = 200) {
  next_items_page(limit: $limit, cursor: $cursor) {
    cursor
    items {
      ...ItemFields
    }
  }
}
"""
    + _ITEM_FIELDS
)

ITEMS_BY_BOARD_PAGE = (
    """
query GetItemsByBoardPage($boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {
  boards(limit: $boardsLimit, page: $boardsPage, state: $state, order_by: created_at) {
    id
    state
    updated_at
    items_page(limit: $itemsLimit) {
      cursor
      items {
        ...ItemFields
      }
    }
  }
}
"""
    + _ITEM_FIELDS
)

ITEMS_BY_IDS = (
    """
query GetItemsByIds($ids: [ID!]!, $limit: Int = 10, $page: Int = 1) {
    items(ids: $ids, limit: $limit, page: $page) {
        ...ItemFields
    }
}
"""
    + _ITEM_FIELDS
)
