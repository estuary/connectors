from datetime import timedelta, datetime, UTC
from logging import Logger
from typing import List, AsyncGenerator, Awaitable, Optional
import itertools
import asyncio

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin

from .models import EndpointConfig, ResourceState, Item, ResourceConfig
from .api import fetch_page

from .buffer_ordered import buffer_ordered


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        items(http),
    ]


def items(http: HTTPMixin):
    def open(
        binding: CaptureBinding,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        async def fetch_items_batched(
            log, start_cursor, log_cutoff
        ) -> AsyncGenerator[Item | int, None]:
            async def fetch_with_retry(
                item_id: int, max_retries: int = 3
            ) -> Optional[Item]:
                for attempt in range(max_retries):
                    try:
                        async for response in fetch_page(
                            http, log, item_id, log_cutoff
                        ):
                            if response is not None and isinstance(response, Item):
                                return response
                    except Exception as e:
                        if attempt == max_retries - 1:
                            log.error(
                                f"Failed to fetch item {item_id} after {max_retries} attempts: {e}"
                            )
                            return None
                        await asyncio.sleep(2**attempt)
                return None

            async def _do_batch_fetch(batch: List[int]) -> List[Item]:

                tasks = [fetch_with_retry(item_id) for item_id in batch]
                results = await asyncio.gather(*tasks)
                return [r for r in results if r is not None]

            async def _batches_gen() -> AsyncGenerator[Awaitable[List[Item]], None]:
                total_items = 100
                batch_size = min(20, max(5, int(total_items / 10)))
                for batch_it in itertools.batched(
                    range(start_cursor, start_cursor + total_items), batch_size
                ):
                    yield _do_batch_fetch(list(batch_it))

            total = 100
            count = 0
            concurrency = min(10, max(3, int(total / 20)))
            async for res in buffer_ordered(_batches_gen(), concurrency):
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
        initial_state=ResourceState(
            backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))
        ),
        initial_config=ResourceConfig(name="items", interval=timedelta(hours=1)),
        schema_inference=False,
    )
