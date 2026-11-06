import itertools
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.async_streams import merge
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel

from source_monday.graphql.query_executor import execute_query, BoardNullTracker
from source_monday.models import GraphQLResponseData, GraphQLResponseRemainder, Item


class ItemsPage(BaseModel, extra="allow"):
    cursor: str | None = None
    items: list[Item] | None = None


class BoardItems(BaseModel, extra="allow"):
    id: str
    state: str
    updated_at: AwareDatetime
    items_page: ItemsPage | None = None


class ItemsPageRemainderData(GraphQLResponseData, extra="allow"):
    boards: list[BoardItems | None] | None = None
    next_items_page: ItemsPage | None = None


ItemsPageRemainder = GraphQLResponseRemainder[ItemsPageRemainderData]


class SubitemState(BaseModel, extra="allow"):
    id: str
    state: str


# Fetching items by ID is restricted to 100 items per request when using the `items` query.
# This is a limitation of the Monday API, so we need to batch requests accordingly.
# This is only used in the incremental task, so we don't expect to have multiple concurrent fetches
# that often, but we still limit it to avoid overwhelming the API even though the maximum concurrent requests
# to Monday can go higher based on the account's plan.
MAX_CONCURRENT_ITEM_FETCHES = 5
# The max complexity budget per request is 5 million and 10 million total per minute.
BOARDS_PER_PAGE = 1
ITEMS_PER_BOARD = 5
ITEMS_LIMIT_BY_ID = 100
# Monday caps the `items(ids:)` query at 100 IDs per request, so subitem `state`
# backfills are chunked into batches of this size (see `_enrich_subitem_states`).
SUBITEM_STATE_BATCH_SIZE = 100


class CursorCollector:
    def __init__(self):
        self.cursors: list[str] = []
        self.null_board_count: int = 0

    def process(self, log: Logger, remainder: ItemsPageRemainder) -> None:
        log.debug("Processing ItemsPageRemainder for cursors")
        if not remainder.data:
            log.debug("No data in remainder, skipping cursor processing")
            return

        if remainder.data.boards is not None:
            log.debug("Processing boards structure for cursors")
            boards_data = remainder.data.boards

            for board_data in boards_data:
                if not board_data:
                    log.debug("Skipping empty board_data")
                    self.null_board_count += 1
                    continue

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

    def get_result(self, log: Logger) -> tuple[list[str], int]:
        log.debug(
            f"Returning collected cursors and null board count: {self.cursors}, {self.null_board_count}"
        )
        return self.cursors, self.null_board_count


async def _enrich_subitem_states(
    http: HTTPSession,
    log: Logger,
    items: list[Item],
) -> None:
    """
    Populate `state` onto each item's nested subitems.

    We don't request `state` inside the `subitems` selection of the items query.
    Monday returns an INTERNAL_SERVER_ERROR when we request `state` for certain
    subitems through that nested selection, which fails the whole request. The
    top-level `items(ids:)` query returns subitem `state` fine, so we fetch it
    there and write it back onto the subitems.
    """
    subitem_ids: list[str] = []
    for item in items:
        for subitem in getattr(item, "subitems", None) or []:
            if isinstance(subitem, dict) and subitem.get("id"):
                subitem_ids.append(subitem["id"])

    subitem_ids = list(dict.fromkeys(subitem_ids))
    if not subitem_ids:
        return

    subitem_state_by_id: dict[str, str] = {}
    for batch in itertools.batched(subitem_ids, SUBITEM_STATE_BATCH_SIZE):
        batch_list = list(batch)
        async for subitem in execute_query(
            SubitemState,
            http,
            log,
            "data.items.item",
            SUBITEM_STATES,
            {"ids": batch_list, "limit": len(batch_list), "page": 1},
        ):
            subitem_state_by_id[subitem.id] = subitem.state

    for item in items:
        for subitem in getattr(item, "subitems", None) or []:
            if isinstance(subitem, dict):
                subitem_state = subitem_state_by_id.get(subitem.get("id", ""))
                if subitem_state is not None:
                    subitem["state"] = subitem_state


async def _with_subitem_states(
    http: HTTPSession,
    log: Logger,
    items: AsyncGenerator[Item, None],
) -> AsyncGenerator[Item, None]:
    """
    Buffer items in batches, backfill subitem `state` for each batch via
    `_enrich_subitem_states`, then yield the enriched items.
    """
    buffer: list[Item] = []

    async for item in items:
        buffer.append(item)
        if len(buffer) >= SUBITEM_STATE_BATCH_SIZE:
            await _enrich_subitem_states(http, log, buffer)
            for enriched in buffer:
                yield enriched
            buffer = []

    if buffer:
        await _enrich_subitem_states(http, log, buffer)
        for enriched in buffer:
            yield enriched


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
            async for item in _with_subitem_states(http, log, merge(*concurrent_batch)):
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
        raise ValueError(
            "get_items_from_boards requires a non-empty list of board IDs."
        )

    log.debug(f"Fetching items for boards {board_ids}.")

    if items_limit is None or items_limit <= 0:
        log.error(
            "Invalid items_limit provided, must provide an integer greater than 0."
        )
        raise ValueError(
            "Invalid items_limit provided, must provide an integer greater than 0."
        )

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

        async for item in _with_subitem_states(
            http,
            log,
            _stream_all_items_from_page(
                http,
                log,
                ITEMS,
                variables,
                batch_cursor_collector,
                batch_ids_list,
            ),
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

    Uses "up a level" streaming approach:
    1. Stream boards to capture board-level information and detect null items_page
    2. Track boards with null items_page for authorization correlation
    3. Yield individual items from accessible boards

    The internal cursor handling is needed because Monday's API uses a cursor-based pagination
    for items within each board, where the cursor is specific to the board's items_page and
    expires after 60-minutes.
    """

    items_yielded = 0

    null_tracker = BoardNullTracker()

    async for item in execute_query(
        Item,
        http,
        log,
        "data.boards.item.items_page.items.item",
        query,
        variables,
        remainder_cls=ItemsPageRemainder,
        remainder_processor=cursor_collector,
        null_tracker=null_tracker,
    ):
        items_yielded += 1
        yield item

    # When a board's items or fields on the items are not accessible to the user's
    # API token, the response may contain a "null" entry in the `data.boards` list while other
    # boards are present. While the API usually returns all other boards, it is safer to not
    # try to assume or infer the board ID(s) from the response like we do elsewhere with the `BoardNullTracker`.
    # Since the API response doesn't have the board ID with some fields "null", we can't infer the board ID
    # reliably enough. Therefore, we just log a warning message with the list of the board IDs that were queried producing
    # one or many USER_UNAUTHORIZED errors/"null" boards.
    cursors, null_board_count = cursor_collector.get_result(log)

    if null_board_count:
        log.error(
            f"Monday's API returned {null_board_count} 'null' boards when trying to retrieve items. "
            f"However, there is not a reliable way to get the board IDs from the API response. The boards that were queried are: {variables['boardIds']}"
        )

    while cursors:
        cursor = cursors.pop(0)
        log.debug(f"Processing cursor pagination, remaining cursors: {len(cursors)}")

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

        # Same comment as above, but for subsequent pages. However, if there are permission
        # issues for a board, there won't be a second page to process for the board in question.
        # This will handle the scenario where permissions change between querying the first page
        # and the second page that could cause the board to not be accessible anymore.
        next_cursors, null_board_count = cur_collector.get_result(log)

        if null_board_count:
            log.error(
                f"Monday's API returned {null_board_count} 'null' boards when trying to retrieve the next page of items. "
                f"However, there is not a reliable way to get the board IDs from the API response. The boards that were queried are: {variables['boardIds']}"
            )
            raise RuntimeError(
                "Encountered 'null' boards while trying to fetch next items page. "
                "This usually indicates a permissions issue with the API token. "
                "Please check the API token's permissions and try again.",
                {
                    "query": NEXT_ITEMS,
                    "variables": cursor_variables,
                    "null_board_count": null_board_count,
                },
            )

        if next_cursors:
            cursors.extend(next_cursors)

    log.debug(
        "Items fetched.",
        {
            "boards": board_ids,
            "items_yielded": items_yielded,
        },
    )


# `state` lives only on the top-level `ItemFields` fragment, not on the shared
# `_ItemFields` that subitems reuse. Monday returns an INTERNAL_SERVER_ERROR when
# `state` is requested for certain subitems through the nested `subitems`
# selection, so subitem `state` is populated separately by `_enrich_subitem_states`
# via the top-level `items(ids:)` query.
_ITEM_FIELDS = """
fragment _ItemFields on Item {
  id
  name
  email
  created_at
  updated_at
  creator_id
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
    column {
        id
        title
        description
        type
        width
        archived
    }
    ... on StatusValue {
        id
        column {
            id
            title
            description
            type
            width
            archived
        }
        index
        is_done
        label
        label_style {
            border
            color
        }
        text
        type
        update_id
        updated_at
        value
    }
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
  state
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

# Populate `state` onto subitems. Fetching subitem `state` inline through the
# `subitems` selection triggers an INTERNAL_SERVER_ERROR for certain subitems,
# but subitems are themselves items, so we can read their `state` here.
SUBITEM_STATES = """
query GetSubitemStates($ids: [ID!]!, $limit: Int = 100, $page: Int = 1) {
    items(ids: $ids, limit: $limit, page: $page) {
        id
        state
    }
}
"""
