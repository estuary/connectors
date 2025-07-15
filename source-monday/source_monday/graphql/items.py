import itertools
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Callable

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
        self.oldest_board_updated_at: datetime | None = None

    def process(self, log: Logger, remainder: ItemsPageRemainder) -> None:
        log.debug("Processing ItemsPageRemainder for cursors")
        if remainder.data:
            if remainder.data.boards is None:
                log.debug(
                    "No boards found in remainder data, skipping cursor processing"
                )
                return
            else:
                self.oldest_board_updated_at = min(
                    (board.updated_at for board in remainder.data.boards),
                    default=None,
                )
                boards_data = remainder.data.boards

            log.debug(
                f"Oldest board updated at: {self.oldest_board_updated_at}",
                {"all_updated_at": [board.updated_at for board in boards_data]},
            )

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

    def get_result(self, log: Logger) -> list[str]:
        log.debug(f"Returning collected cursors: {self.cursors}")
        return self.cursors

    def get_cursors(self, log: Logger) -> list[str]:
        """Alias for get_result."""
        return self.get_result(log)

    def get_oldest_board_updated_at(self) -> datetime | None:
        return self.oldest_board_updated_at


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
        if not batch_list:
            continue
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


class BoardItemIterator:
    def __init__(
        self,
        http: HTTPSession,
        log: Logger,
    ):
        self.http = http
        self.log = log
        self._items_generator_started = False

    async def get_items_from_boards(
        self,
        board_ids: list[str],
        max_items_count: int | None = None,
    ) -> tuple[AsyncGenerator[Item, None], Callable[[], datetime | None]]:
        if not board_ids or len(board_ids) == 0:
            self.log.debug("No board IDs provided, returning empty generator")
            return self._empty_items_generator(), lambda: None

        self.log.debug(f"Fetching items for boards {board_ids}.")

        oldest_board_updated_at: datetime | None = None

        async def batched_items_generator() -> AsyncGenerator[Item, None]:
            nonlocal oldest_board_updated_at

            for batch_ids in itertools.batched(board_ids, BOARDS_PER_PAGE):
                batch_ids_list = list(batch_ids)
                self.log.debug(f"Processing batch with board IDs: {batch_ids_list}")

                variables = {
                    "boardIds": batch_ids_list,
                    "boardsLimit": len(batch_ids_list),
                    "boardsPage": 1,
                    "itemsLimit": min(max_items_count or ITEMS_PER_BOARD, ITEMS_PER_BOARD),
                    "state": "all",
                }

                batch_cursor_collector = CursorCollector()

                async for item in self._stream_all_items_from_page(
                    ITEMS,
                    variables,
                    batch_cursor_collector,
                    batch_ids_list,
                ):
                    yield item

                batch_oldest = batch_cursor_collector.get_oldest_board_updated_at()

                if batch_oldest is not None:
                    if (
                        oldest_board_updated_at is None
                        or batch_oldest < oldest_board_updated_at
                    ):
                        oldest_board_updated_at = batch_oldest

        return batched_items_generator(), lambda: oldest_board_updated_at

    async def _empty_items_generator(self) -> AsyncGenerator[Item, None]:
        """Return empty items generator when no boards to process."""
        return
        yield  # Unreachable, but needed for generator

    async def _stream_all_items_from_page(
        self,
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
            self.http,
            self.log,
            "data.boards.item.items_page.items.item",
            query,
            variables,
            remainder_cls=ItemsPageRemainder,
            remainder_processor=cursor_collector,
        ):
            items_yielded += 1
            yield item

        cursors = cursor_collector.get_result(self.log)
        while cursors:
            cursor = cursors.pop(0)
            self.log.debug(
                f"Processing cursor pagination, remaining cursors: {len(cursors)}"
            )

            cursor_variables = {"cursor": cursor, "limit": ITEMS_LIMIT_BY_ID}
            cur_collector = CursorCollector()

            async for item in execute_query(
                Item,
                self.http,
                self.log,
                "data.next_items_page.items.item",
                NEXT_ITEMS,
                cursor_variables,
                remainder_cls=ItemsPageRemainder,
                remainder_processor=cur_collector,
            ):
                items_yielded += 1
                yield item

            next_cursors = cur_collector.get_result(self.log)
            if next_cursors:
                cursors.extend(next_cursors)

        self.log.debug(
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
