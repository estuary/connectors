from datetime import timedelta, datetime, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPMixin, HTTPSession

from .models import (
    EndpointConfig,
    ResourceState,
    Item,
    User,
    UpdatedItems,
    StoryRanking,
    ResourceConfig,
)
from .api import (
    fetch_updates,
    fetch_max_item_id,
    fetch_items_batch,
    fetch_users_batch,
    fetch_story_ids,
    BACKFILL_BATCH_SIZE,
    STORY_LIST_CATEGORIES,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        items_resource(http),
        users_resource(http),
        updates_resource(http),
        *[story_ranking_resource(http, category) for category in STORY_LIST_CATEGORIES],
    ]


def items_resource(http: HTTPSession) -> common.Resource:
    """
    HackerNews Items resource - stories, comments, jobs, polls, and poll options.

    Backfill walks item IDs forward from 1 up to the current maximum. Incremental
    replication polls the `/updates.json` feed, which lists items whose content
    changed (new comments, score, edits, deletions); every item the feed reports
    is emitted, since an item's `time` is its immutable creation time and cannot
    be used to detect those changes.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_changes(
            log: Logger, log_cursor: LogCursor
        ) -> AsyncGenerator[Item | LogCursor, None]:
            assert isinstance(log_cursor, datetime)

            updates = await fetch_updates(http, log)
            if updates and updates.items:
                log.info(f"Processing {len(updates.items)} updated items")
                async for item in fetch_items_batch(
                    http, log, updates.items, concurrency=15
                ):
                    yield item

            yield datetime.now(tz=UTC)

        async def fetch_backfill_page(
            log: Logger, page_cursor: PageCursor, cutoff: LogCursor
        ) -> AsyncGenerator[Item | PageCursor, None]:
            assert isinstance(cutoff, datetime)
            start_id = 1 if page_cursor is None else page_cursor
            assert isinstance(start_id, int)

            max_id = await fetch_max_item_id(http, log)
            if max_id == 0:
                log.warning("Failed to fetch max item ID")
                return
            if start_id > max_id:
                log.info(f"Backfill complete, reached max item ID {max_id}")
                return

            end_id = min(start_id + BACKFILL_BATCH_SIZE - 1, max_id)
            item_ids = list(range(start_id, end_id + 1))

            count = 0
            async for item in fetch_items_batch(http, log, item_ids, concurrency=20):
                if item.time < cutoff:
                    log.info(
                        f"Reached cutoff time, stopping backfill at item {item.id}"
                    )
                    return
                count += 1
                yield item

            if end_id < max_id:
                yield end_id

            log.info(f"Backfilled {count} items, from {start_id} to {end_id}")

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_changes,
            fetch_page=fetch_backfill_page,
        )

    return common.Resource(
        name="items",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC)),
            backfill=ResourceState.Backfill(
                next_page=None, cutoff=datetime.fromtimestamp(0, tz=UTC)
            ),
        ),
        initial_config=ResourceConfig(name="items", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def users_resource(http: HTTPSession) -> common.Resource:
    """
    HackerNews Users resource - user profiles and metadata.

    Driven entirely by the `/updates.json` feed's `profiles` list; every reported
    profile is re-fetched and emitted, since a user's `created` time cannot be
    used to detect karma/about changes.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_user_changes(
            log: Logger, log_cursor: LogCursor
        ) -> AsyncGenerator[User | LogCursor, None]:
            assert isinstance(log_cursor, datetime)

            updates = await fetch_updates(http, log)
            if updates and updates.profiles:
                log.info(f"Processing {len(updates.profiles)} updated users")
                async for user in fetch_users_batch(
                    http, log, updates.profiles, concurrency=10
                ):
                    yield user

            yield datetime.now(tz=UTC)

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_user_changes,
        )

    return common.Resource(
        name="users",
        key=["/id"],
        model=User,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
        ),
        initial_config=ResourceConfig(name="users", interval=timedelta(minutes=10)),
        schema_inference=True,
    )


def updates_resource(http: HTTPSession) -> common.Resource:
    """
    HackerNews Updates resource - the raw `/updates.json` change feed.

    Each poll appends one document recording the items and profiles HackerNews
    reported as changed, keyed by the framework-assigned row ID so the feed is
    captured as an append-only log.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_update_changes(
            log: Logger, log_cursor: LogCursor
        ) -> AsyncGenerator[UpdatedItems | LogCursor, None]:
            assert isinstance(log_cursor, datetime)

            updates = await fetch_updates(http, log)
            if updates:
                yield updates

            yield datetime.now(tz=UTC)

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_update_changes,
        )

    return common.Resource(
        name="updates",
        key=["/_meta/row_id"],
        model=UpdatedItems,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
        ),
        initial_config=ResourceConfig(name="updates", interval=timedelta(minutes=1)),
        schema_inference=True,
    )


def story_ranking_resource(http: HTTPSession, category: str) -> common.Resource:
    """
    A HackerNews ranking list: one of topstories, newstories, beststories,
    askstories, showstories, or jobstories.

    Each poll appends one snapshot of the current ordered story IDs, keyed by the
    framework-assigned row ID so the ranking is captured as an append-only log.
    A story's rank at a given time is its index within `story_ids`. Rankings
    change continuously and cannot be reconstructed from items, so they are
    captured as their own streams.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_changes(
            log: Logger, log_cursor: LogCursor
        ) -> AsyncGenerator[StoryRanking | LogCursor, None]:
            assert isinstance(log_cursor, datetime)

            now = datetime.now(tz=UTC)
            story_ids = await fetch_story_ids(http, log, category)
            if story_ids:
                yield StoryRanking(
                    category=category, retrieved_at=now, story_ids=story_ids
                )

            yield now

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_changes,
        )

    return common.Resource(
        name=category,
        key=["/_meta/row_id"],
        model=StoryRanking,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
        ),
        initial_config=ResourceConfig(name=category, interval=timedelta(minutes=5)),
        schema_inference=True,
    )
