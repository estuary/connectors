import itertools
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Iterator

from estuary_cdk.http import HTTPSession

from source_monday.graphql import fetch_boards_minimal
from source_monday.models import Item
from .items import fetch_items, fetch_items_by_ids

DEFAULT_BOARDS_BATCH_SIZE = 1
DEFAULT_ITEMS_BATCH_SIZE = 500


@dataclass(frozen=True, slots=True)
class ItemCacheEntry:
    board_id: str
    item_id: str
    state: str
    updated_at: datetime


@dataclass
class ItemIdCache:
    _items: deque[ItemCacheEntry] = field(default_factory=deque)
    _current_item_index: int = 0
    _boards: list[tuple[str, datetime]] = field(default_factory=list)
    _board_iterator: Iterator[tuple[tuple[str, datetime], ...]] | None = field(
        init=False, default=None
    )
    _processed_boards: int = 0
    boards_batch_size: int = DEFAULT_BOARDS_BATCH_SIZE
    items_batch_size: int = DEFAULT_ITEMS_BATCH_SIZE

    async def populate_next_board_chunk(
        self,
        http: HTTPSession,
        log: Logger,
        start: datetime,
        cutoff: datetime,
    ) -> datetime | None:
        if not self._boards:
            await self._load_boards(http, log, start)
            log.debug(f"Cached {len(self._boards)} boards for processing")
            self._board_iterator = itertools.batched(
                self._boards, self.boards_batch_size
            )

        if self._board_iterator is None:
            log.debug("No more board batches to process")
            return None

        try:
            current_batch: tuple[tuple[str, datetime], ...] = next(self._board_iterator)
        except StopIteration:
            log.debug("No more board batches to process")
            return None

        board_ids = [board_id for board_id, _ in current_batch]
        oldest_timestamp = min(timestamp for _, timestamp in current_batch)

        self._log_batch_info(log, board_ids, oldest_timestamp)

        self._items.clear()
        self._current_item_index = 0

        async for item_entry in self._fetch_items_for_boards(
            http, log, board_ids, cutoff
        ):
            self._items.append(item_entry)

        items_list = list(self._items)
        items_list.sort(key=lambda x: (x.updated_at, x.item_id))
        self._items = deque(items_list)

        self._processed_boards += len(board_ids)
        log.info(f"Loaded {len(self._items)} items from {len(board_ids)} boards")

        return oldest_timestamp

    async def _fetch_items_for_boards(
        self,
        http: HTTPSession,
        log: Logger,
        board_ids: list[str],
        cutoff: datetime,
    ) -> AsyncGenerator[ItemCacheEntry, None]:
        end = (cutoff.date() + timedelta(days=1)).isoformat()

        async for item in fetch_items(
            http, log, end=end, for_cache=True, board_ids=board_ids
        ):
            if item.updated_at <= cutoff and item.state != "deleted":
                assert item.board is not None, "Item must have a board"
                yield ItemCacheEntry(
                    item.board.id,
                    item.id,
                    item.state,
                    item.updated_at,
                )

    def get_next_batch(self) -> list[str]:
        if not self._items:
            return []

        batch_size = min(self.items_batch_size, len(self._items))
        batch_entries = [self._items.popleft() for _ in range(batch_size)]
        return [entry.item_id for entry in batch_entries]

    def has_more_items(self) -> bool:
        return len(self._items) > 0

    def has_more_boards(self) -> bool:
        return self._processed_boards < len(self._boards)

    def get_cache_info(self) -> dict:
        return {
            "items_in_current_chunk": len(self._items),
            "remaining_items": len(self._items),
            "total_boards": len(self._boards),
            "processed_boards": self._processed_boards,
        }

    def reset_for_new_cutoff(self) -> None:
        self._items.clear()
        self._current_item_index = 0
        self._boards.clear()
        self._board_iterator = None
        self._processed_boards = 0

    async def _load_boards(
        self, http: HTTPSession, log: Logger, start: datetime
    ) -> None:
        async for board in fetch_boards_minimal(http, log):
            if board.updated_at <= start and board.state != "deleted":
                self._boards.append((board.id, board.updated_at))

        self._boards.sort(key=lambda x: x[1], reverse=True)

    def _log_batch_info(
        self, log: Logger, board_ids: list[str], oldest_timestamp: datetime
    ) -> None:
        batch_number = (self._processed_boards // self.boards_batch_size) + 1
        log.debug(
            f"Processing board batch {batch_number}",
            {
                "board_count": len(board_ids),
                "oldest_in_batch": oldest_timestamp,
                "total_boards": len(self._boards),
            },
        )


class ItemCacheSession:
    def __init__(
        self,
        http: HTTPSession,
        log: Logger,
        cutoff: datetime,
        boards_batch_size: int = DEFAULT_BOARDS_BATCH_SIZE,
        items_batch_size: int = DEFAULT_ITEMS_BATCH_SIZE,
    ):
        self.http = http
        self.log = log
        self.cutoff = cutoff
        self._cache = ItemIdCache(
            boards_batch_size=boards_batch_size,
            items_batch_size=items_batch_size,
        )
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return

        self.log.info(f"Initializing item cache session for cutoff: {self.cutoff}")
        self._initialized = True

    async def process_page(
        self,
        page_cursor: str | None,
    ) -> tuple[AsyncGenerator[Item, None], str | None]:
        if not self._initialized:
            await self.initialize()

        # Determine starting point for board processing
        start = datetime.fromisoformat(page_cursor) if page_cursor else self.cutoff

        # Get next batch of boards and their items
        oldest_board_timestamp = await self._cache.populate_next_board_chunk(
            self.http, self.log, start, self.cutoff
        )

        if not oldest_board_timestamp:
            self.log.debug("No more boards to process - backfill complete")
            return self._empty_generator(), None

        cache_info = self._cache.get_cache_info()
        self.log.debug(
            "Processing board chunk from cache session",
            {
                "chunk_info": cache_info,
                "cutoff": self.cutoff,
                "page_cursor": page_cursor,
            },
        )

        return (
            self._stream_items_from_cache(),
            oldest_board_timestamp.isoformat(),
        )

    async def _empty_generator(self) -> AsyncGenerator[Item, None]:
        return
        yield  # Unreachable, but needed for generator

    async def _stream_items_from_cache(self) -> AsyncGenerator[Item, None]:
        """Stream items from the current cache batch."""
        yielded_items = False
        while self._cache.has_more_items():
            item_ids = self._cache.get_next_batch()

            if item_ids:
                yielded_items = True
                async for item in fetch_items_by_ids(self.http, self.log, item_ids):
                    if item.updated_at <= self.cutoff:
                        yield item

        if not yielded_items:
            self.log.debug("All board chunks processed - backfill complete")

    def reset(self) -> None:
        self._cache.reset_for_new_cutoff()
        self._initialized = False
