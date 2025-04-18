import functools
from datetime import timedelta, datetime, UTC
from logging import Logger
from typing import List, AsyncGenerator, Awaitable
import itertools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPSession, HTTPMixin

from .models import (
    EndpointConfig,
    ResourceState, Item, User, ResourceConfig
)
from .api import (
    fetch_page, fetch_user
)

from .buffer_ordered import buffer_ordered


async def all_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        items(http),
        users(http),
    ]


def items(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_items_batched(log, start_cursor, log_cutoff) -> AsyncGenerator[Item | int, None]:
            async def _do_batch_fetch(batch: List[int]) -> List[Item]:
                results = []
                for item_id in batch:
                    async for response in fetch_page(http, log, item_id, log_cutoff):
                        if response is not None and isinstance(response, Item):
                            results.append(response)
                return results

            async def _batches_gen() -> AsyncGenerator[Awaitable[List[Item]], None]:
                for batch_it in itertools.batched(range(start_cursor, start_cursor + 100), 10):
                    yield _do_batch_fetch(list(batch_it))

            total = 100
            count = 0
            async for res in buffer_ordered(_batches_gen(), 5):
                for item in res:
                    count += 1
                    if count > 0 and count % 10_000 == 0:
                        log.info("fetching items", {"count": count, "total": total})
                    yield item
                yield start_cursor + count  # Yield the next cursor position

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_items_batched,
        )

    return common.Resource(
        name="items",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="items", interval=timedelta(hours=1)),
        schema_inference=False,
    )


def users(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_user_filtered(log, start_cursor, log_cutoff):
            user = await fetch_user(http, log, start_cursor)
            if user is not None:  # Only yield if we got a valid user
                yield user
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_user_filtered,
        )

    return common.Resource(
        name="users",
        key=["/id"],
        model=User,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="users", interval=timedelta(minutes=5)),
        schema_inference=False,
    )