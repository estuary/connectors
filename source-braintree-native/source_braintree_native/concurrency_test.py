import asyncio
import braintree
from braintree import ResourceCollection
import concurrent.futures
from datetime import datetime, UTC
import itertools
import threading

from source_braintree_native.api import _async_iterator_wrapper

PAGE_SIZE = 50

gateway = braintree.BraintreeGateway(
    braintree.Configuration(
        environment=braintree.Environment.Production,
        merchant_id="",
        public_key="",
        private_key="",
    )
)

collection: ResourceCollection = gateway.transaction.search(
    braintree.TransactionSearch.created_at.between(datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 4, 3, tzinfo=UTC))
)

print(f"Found {collection.maximum_size} transactions.")


async def fetch_transaction_with_ids(
    chunk: dict,
):
    ids: list[str] = list(chunk["ids"])
    chunk_id: int = chunk["chunk_id"]

    start = datetime.now(tz=UTC)
    print(f"Starting chunk {chunk_id} at {start}.")

    collection: ResourceCollection = await asyncio.to_thread(
        gateway.transaction.search,
        braintree.TransactionSearch.ids.in_list(ids)
    )

    objs = []
    async for item in _async_iterator_wrapper(collection):
        # objs.append(item)
        pass

    end = datetime.now(tz=UTC)
    print(f"Finished chunk {chunk_id} at {end}. Took {end - start}. There are {threading.active_count()} active threads.")

    return objs


async def fetch_in_batches(
    collection: ResourceCollection
):
    chunks = []
    for num, ids_chunk in enumerate(itertools.batched(collection.ids, PAGE_SIZE)):
        chunks.append({
            "ids": ids_chunk,
            "chunk_id": num,
        })

    start = datetime.now(UTC)
    print(f"Starting concurrent fetching at {start}.")
    for coro in asyncio.as_completed(
        [
            fetch_transaction_with_ids(chunk)
            for chunk in chunks
        ]
    ):
        objs = await coro
        # for _ in objs:
        #     pass

    end = datetime.now(UTC)
    print(f"Finished concurrent fetching at {end}. Took {end - start}.")


async def fetch_sequentially(
    collection: ResourceCollection
):
    start = datetime.now(tz=UTC)
    print(f"Started sequential fetch at {start}.")

    count = 0
    async for _ in _async_iterator_wrapper(collection):
        if count % PAGE_SIZE == 0:
            if count > 0:
                page_end = datetime.now(tz=UTC)
                print(f"Finished page {count // PAGE_SIZE} at {page_end}. Took {page_end - page_start}")
            page_start = datetime.now(tz=UTC)
            print(f"Starting page {count // PAGE_SIZE + 1} at {page_start}.")
        count += 1
        pass

    end = datetime.now(tz=UTC)
    print(f"Finished sequential fetch at {end}. Took {end - start}")


# Try to have as many threads as possible.
custom_executor = concurrent.futures.ThreadPoolExecutor(max_workers=collection.maximum_size // PAGE_SIZE)

async def main():
    loop = asyncio.get_running_loop()
    loop.set_default_executor(custom_executor)

    # Concurrent test
    await fetch_in_batches(collection)

    # Sequential test
    await fetch_sequentially(collection)


asyncio.run(main())

custom_executor.shutdown(wait=True)
