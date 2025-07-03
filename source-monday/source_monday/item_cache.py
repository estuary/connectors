from dataclasses import dataclass, field
from datetime import datetime, UTC
from logging import Logger
from typing import AsyncGenerator, NamedTuple, Union

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import PageCursor
from source_monday.graphql import fetch_items_minimal


class ItemCacheEntry(NamedTuple):
    item_id: str
    state: str
    updated_at: datetime


@dataclass
class ItemIdCache:
    _items: list[ItemCacheEntry] = field(default_factory=list)
    _current_index: int = 0
    _cutoff: datetime | None = None
    _cache_populated_at: datetime | None = None

    async def populate_if_needed(
        self,
        http: HTTPSession,
        log: Logger,
        page_cursor: PageCursor,
        cutoff: datetime,
    ) -> None:
        now = datetime.now(tz=UTC)
        is_cutoff_in_future = cutoff > now

        needs_repopulation = (
            self._cutoff is None  # First population
            or self._cutoff != cutoff  # Cutoff has changed (backfill was triggered)
        )

        if not needs_repopulation:
            return

        log.info(
            "Populating item cache",
            {
                "cutoff": cutoff,
                "is_cutoff_in_future": is_cutoff_in_future,
                "page_cursor": page_cursor,
            },
        )

        self._items.clear()
        self._current_index = 0

        new_item_count = 0
        async for item_entry in self._fetch_all_qualifying_items(http, log, cutoff):
            self._items.append(item_entry)
            new_item_count += 1

        # Sort items by updated_at timestamp, then by item_id for deterministic ordering
        self._items.sort(key=lambda x: (x.updated_at, x.item_id))

        self._cutoff = cutoff
        self._cache_populated_at = now

        log.info(
            f"Item cache populated with {len(self._items)} qualifying items",
            {
                "new_items": new_item_count,
                "total_items": len(self._items),
            },
        )

    async def _fetch_all_qualifying_items(
        self,
        http: HTTPSession,
        log: Logger,
        cutoff: datetime,
    ) -> AsyncGenerator[ItemCacheEntry, None]:
        total_qualifying_items = 0
        boards_processed = 0

        log.debug(f"Starting to collect all qualifying items with cutoff: {cutoff}")

        async for item in fetch_items_minimal(http, log):
            # Check if item qualifies based on cutoff and state
            log.debug(
                f"Processing item {item.id} with state {item.state} and updated_at {item.updated_at}"
            )
            if item.updated_at <= cutoff and item.state != "deleted":
                yield ItemCacheEntry(item.id, item.state, item.updated_at)
                total_qualifying_items += 1

        log.debug(
            "Finished collecting qualifying items",
            {
                "total_qualifying_items": total_qualifying_items,
                "board_pages_processed": boards_processed,
            },
        )

    def get_next_batch(
        self, batch_size: int = 500
    ) -> Union[tuple[list[str], None], tuple[list[str], str]]:
        if self._current_index >= len(self._items):
            return [], None

        start_idx = self._current_index
        end_idx = min(start_idx + batch_size, len(self._items))
        batch_entries = self._items[start_idx:end_idx]

        item_ids = [entry.item_id for entry in batch_entries]

        if not item_ids:
            return [], None

        last_item = batch_entries[-1]
        cursor = last_item.updated_at.isoformat()

        self._current_index = end_idx
        return item_ids, cursor

    def has_more_items(self) -> bool:
        return self._current_index < len(self._items)

    def get_progress(self) -> tuple[int, int]:
        return self._current_index, len(self._items)

    def get_cache_info(self) -> dict:
        return {
            "total_items": len(self._items),
            "processed_items": self._current_index,
            "remaining_items": len(self._items) - self._current_index,
            "cutoff": self._cutoff,
            "cache_populated_at": self._cache_populated_at,
            "cache_age_minutes": (
                (datetime.now(UTC) - self._cache_populated_at).total_seconds() / 60
                if self._cache_populated_at
                else None
            ),
        }
