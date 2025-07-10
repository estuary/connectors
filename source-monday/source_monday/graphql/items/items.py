import itertools
from logging import Logger
from typing import AsyncGenerator

from aiostream.stream import merge
from estuary_cdk.http import HTTPSession
from pydantic import BaseModel

from source_monday.graphql.query_executor import execute_query
from source_monday.models import GraphQLResponseData, GraphQLResponseRemainder, Item


class ItemsPage(BaseModel, extra="allow"):
    cursor: str | None = None


class BoardItems(BaseModel, extra="allow"):
    items_page: ItemsPage | None = None


class ItemsPageRemainderData(GraphQLResponseData, extra="allow"):
    boards: list[BoardItems] | None = None


ItemsPageRemainder = GraphQLResponseRemainder[ItemsPageRemainderData]


class CursorCollector:
    def __init__(self):
        self.cursors: list[str] = []

    def process(self, log: Logger, remainder: ItemsPageRemainder) -> None:
        log.debug("Processing ItemsPageRemainder for cursors")
        if remainder.data:
            boards_data = (
                remainder.data.boards if remainder.data.boards is not None else []
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


async def fetch_items_by_ids(
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
            _fetch_items_chunk(
                http, log, batch_list, len(batch_generators) + 1, chunk_count
            )
        )

    if batch_generators:
        async for item in merge(*batch_generators):
            yield item


async def _fetch_items_chunk(
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


async def fetch_items(
    http: HTTPSession,
    log: Logger,
    board_ids: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    for_cache: bool = False,
) -> AsyncGenerator[Item, None]:
    if board_ids is None:
        from source_monday.graphql import fetch_boards_minimal

        board_ids = []
        async for board in fetch_boards_minimal(http, log):
            if board.state != "deleted":
                board_ids.append(board.id)

    assert isinstance(board_ids, list), "board_ids must be a list of strings"

    batch_size = 25 if for_cache else 5

    for i in range(0, len(board_ids), batch_size):
        batch_ids = board_ids[i : i + batch_size]
        log.debug(
            f"Processing board batch {i // batch_size + 1} with {len(batch_ids)} boards"
        )

        async for item in _fetch_batch_items_depth_first(
            http,
            log,
            batch_ids,
            start,
            end,
            for_cache,
        ):
            yield item


async def _fetch_batch_items_depth_first(
    http: HTTPSession,
    log: Logger,
    batch_ids: list[str],
    start: str | None = None,
    end: str | None = None,
    for_cache: bool = False,
) -> AsyncGenerator[Item, None]:
    variables = {
        "boardIds": batch_ids,
        "boardsLimit": len(batch_ids),
        "boardsPage": 1,
        "itemsLimit": 100 if for_cache else 25
    }
    rules = []

    if start:
        rules.append(f"""{{
            column_id: "__last_updated__",
            compare_value: ["EXACT", "{start}"],
            operator: greater_than_or_equals,
            compare_attribute: "UPDATED_AT"
        }}""")
    if end:
        rules.append(f"""{{
            column_id: "__last_updated__",
            compare_value: ["EXACT", "{end}"],
            operator: lower_than_or_equal,
            compare_attribute: "UPDATED_AT"
        }}""")

    query_params_str = f"rules: [{', '.join(rules)}]" if rules else "rules: []"
    first_page_query = (
        ITEMS_FOR_CACHE.format(query_params_str=query_params_str)
        if for_cache
        else ITEMS.format(query_params_str=query_params_str)
    )

    cursor_collector = CursorCollector()

    # Fetch first page of items for this batch
    async for item in execute_query(
        Item,
        http,
        log,
        "data.boards.item.items_page.items.item",
        first_page_query,
        variables,
        ItemsPageRemainder,
        cursor_collector,
    ):
        yield item

    cursors = cursor_collector.get_result(log)

    while cursors:
        cursor = cursors.pop(0)
        log.debug(f"Processing cursor {cursor} for current batch")

        cursor_variables = {"cursor": cursor, "limit": 200}
        next_page_query = NEXT_ITEMS_FOR_CACHE if for_cache else NEXT_ITEMS
        cur_collector = CursorCollector()

        async for item in execute_query(
            Item,
            http,
            log,
            "data.next_items_page.items.item",
            next_page_query,
            cursor_variables,
            ItemsPageRemainder,
            cur_collector,
        ):
            yield item

        next_cursors = cur_collector.get_result(log)
        if next_cursors:
            log.debug(f"Found {len(next_cursors)} additional cursors for current batch")
            cursors.extend(next_cursors)

    log.debug(
        f"Completed depth-first processing for batch with {len(batch_ids)} boards"
    )


_ITEM_FIELDS = """
fragment _ItemFields on Item {{
  id
  board {{
    id
  }}
  updated_at
  state
}}
fragment ItemFields on Item {{
  ..._ItemFields
}}
"""

ITEMS_FOR_CACHE = """
query GetBoardItemsForCache($boardIds: [ID!]!, $boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {{
  boards(ids: $boardIds, limit: $boardsLimit, page: $boardsPage, state: $state, order_by: created_at) {{
    id
    state
    items_page(limit: $itemsLimit, query_params: {{
      order_by: [{{
        column_id: "__last_updated__",
        direction: desc
      }}],
      {query_params_str}
    }}) {{
      cursor
      items {{
        id
        state
        updated_at
        board {{
          id
        }}
      }}
    }}
  }}
}}
"""

ITEMS = (
    """
query GetBoardItems($boardIds: [ID!]!, $boardsLimit: Int = 25, $boardsPage: Int = 1, $itemsLimit: Int = 500, $state: State = all) {{
  boards(ids: $boardIds, limit: $boardsLimit, page: $boardsPage, state: $state, order_by: created_at) {{
    id
    state
    items_page(limit: $itemsLimit, query_params: {{
      order_by: [{{
        column_id: "__last_updated__",
        direction: desc
      }}],
      {query_params_str}
    }}) {{
      cursor
      items {{
        ...ItemFields
      }}
    }}
  }}
}}
"""
    + _ITEM_FIELDS
)

NEXT_ITEMS_FOR_CACHE = """
query GetNextItemsForCache($cursor: String!, $limit: Int = 500) {
  next_items_page(limit: $limit, cursor: $cursor) {
    cursor
    items {
        id
        state
        updated_at
        board {
            id
        }
    }
  }
}
"""

NEXT_ITEMS = """
query GetNextItems($cursor: String!, $limit: Int = 200) {
  next_items_page(limit: $limit, cursor: $cursor) {
    cursor
    items {
      ...ItemFields
    }
  }
}
""" + _ITEM_FIELDS.replace("{{", "{").replace("}}", "}")

ITEMS_BY_IDS = """
query GetItemsByIds($ids: [ID!]!, $limit: Int = 10, $page: Int = 1) {
    items(ids: $ids, limit: $limit, page: $page) {
        id
        name
        state
        updated_at
        board {
            id
        }
    }
}
"""
