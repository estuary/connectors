from datetime import timedelta, datetime, UTC
from logging import Logger
from typing import List, AsyncGenerator, Optional

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPSession, HTTPMixin

from .models import (
    EndpointConfig,
    ResourceState,
    Item,
    User,
    UpdatedItems,
    ResourceConfig,
)
from .api import (
    fetch_user,
    fetch_updates,
    fetch_max_item_id,
    fetch_items_batch,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> List[common.Resource]:
    """
    Return all available HackerNews resources following CLAUDE.md specification.
    """
    return [
        items_resource(http),
        users_resource(http),
        updates_resource(http),
    ]


def items_resource(http: HTTPSession) -> common.Resource:
    """
    HackerNews Items resource - stories, comments, jobs, polls, and poll options.
    Supports both incremental updates and backfill with parallel processing.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_changes(
            log: Logger, log_cursor: datetime
        ) -> AsyncGenerator[Item | datetime, None]:
            """
            Fetch incremental updates using HackerNews /updates.json endpoint.
            This provides real-time changes to items.
            """
            updates = await fetch_updates(http, log)
            if not updates or not updates.items:
                # Emit updated cursor even if no changes
                yield datetime.now(tz=UTC)
                return

            log.info(
                f"Processing {len(updates.items)} updated items",
                {"count": len(updates.items)},
            )

            # Fetch updated items in parallel with controlled concurrency
            async for item in fetch_items_batch(
                http, log, updates.items, concurrency=15
            ):
                # Only yield items that were updated after our cursor
                if item.time > log_cursor:
                    yield item

            # Update cursor to current time
            yield datetime.now(tz=UTC)

        async def fetch_backfill_page(
            log: Logger, page_cursor: Optional[int], cutoff: datetime
        ) -> AsyncGenerator[Item | int, None]:
            """
            Fetch items for backfill, starting from item ID 1 and working forward.
            Uses parallel processing for optimal performance.
            """
            if page_cursor is None:
                # Initialize with item ID 1 (HackerNews items start from 1, not 0)
                page_cursor = 1
                log.info("Starting backfill from item ID 1")

            # Get max item ID to know when to stop
            max_id = await fetch_max_item_id(http, log)
            if max_id == 0:
                log.warning("Failed to fetch max item ID")
                return

            # Fetch items in batches working forward from the cursor
            batch_size = 100
            start_id = page_cursor
            end_id = min(start_id + batch_size - 1, max_id)

            if start_id > max_id:
                log.info(f"Backfill complete, reached max item ID {max_id}")
                return

            item_ids = list(range(start_id, end_id + 1))

            items_fetched = 0
            try:
                async for item in fetch_items_batch(
                    http, log, item_ids, concurrency=20
                ):
                    # Stop if we've reached the cutoff time
                    if item.time < cutoff:
                        log.info(
                            f"Reached cutoff time, stopping backfill at item {item.id}"
                        )
                        return

                    items_fetched += 1
                    yield item
            except Exception as e:
                log.warning(f"Error during batch fetch: {e}")
                # Continue with next batch despite errors
                pass

            # Yield next page cursor (move forward)
            next_cursor = end_id
            if next_cursor < max_id:
                yield next_cursor

            if items_fetched > 0:
                log.info(
                    f"Backfilled {items_fetched} items, next cursor: {next_cursor}"
                )
            else:
                log.info(f"No items fetched in batch {start_id}-{end_id}")

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
    Updates are triggered by the updates endpoint.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_user_changes(
            log: Logger, log_cursor: datetime
        ) -> AsyncGenerator[User | datetime, None]:
            """
            Fetch user profile updates using the /updates.json endpoint.
            """
            updates = await fetch_updates(http, log)
            if not updates or not updates.profiles:
                # Emit updated cursor even if no changes
                yield datetime.now(tz=UTC)
                return

            log.info(
                f"Processing {len(updates.profiles)} updated users",
                {"count": len(updates.profiles)},
            )

            # Fetch users sequentially (user endpoint is lighter weight)
            for user_id in updates.profiles:
                user = await fetch_user(http, log, user_id)
                if user:
                    # Only yield users that were updated after our cursor
                    if user.created > log_cursor:
                        yield user

            # Update cursor to current time
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
    HackerNews Updates resource - provides change notifications for items and profiles.
    This is the real-time feed that drives incremental updates.
    """

    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_update_changes(
            log: Logger, log_cursor: datetime
        ) -> AsyncGenerator[UpdatedItems | datetime, None]:
            """
            Fetch the latest updates from HackerNews.
            """
            updates = await fetch_updates(http, log)
            if updates:
                # Add timestamp metadata for change tracking
                if not hasattr(updates, "_meta") or updates._meta is None:
                    updates._meta = UpdatedItems.Meta()
                updates._meta.op = "u"  # Update operation
                yield updates

            # Always emit updated cursor
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
        key=["/items", "/profiles"],  # Composite key based on the arrays
        model=UpdatedItems,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
        ),
        initial_config=ResourceConfig(name="updates", interval=timedelta(minutes=1)),
        schema_inference=True,
    )
