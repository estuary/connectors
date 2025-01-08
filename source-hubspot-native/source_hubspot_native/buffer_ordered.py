import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, TypeVar

T = TypeVar("T")


@dataclass
class BufferWork[T]:
    aw: Awaitable[T]
    result: asyncio.Queue[T | None]

    next: asyncio.Future["BufferWork[T] | None"]


async def buffer_ordered(
    aws: AsyncGenerator[Awaitable[T], None],
    concurrency: int,
) -> AsyncGenerator[T, None]:
    """
    Run the stream of awaitables 'aws' concurrently and return the results of
    each in order. There may be up to 'concurrency' results from the awaitables
    held in memory at a time, so a lower concurrency may improve memory usage at
    the expense of throughput.

    Args:
        aws: A stream of awaitables to run concurrently. concurrency: The
        maximum number of concurrent awaitables to run.

    Returns:
        A stream of results from the awaitables, with the order matching the
        order yielded by 'aws'.
    """

    work: asyncio.Queue[BufferWork[T] | None] = asyncio.Queue(1)
    next: asyncio.Future[BufferWork[T] | None] = asyncio.Future()

    async def _producer():
        current = next
        try:
            async for aw in aws:
                this_next: asyncio.Future[BufferWork[T] | None] = asyncio.Future()
                this_work = BufferWork(
                    aw=aw,
                    result=asyncio.Queue(1),
                    next=this_next,
                )
                current.set_result(this_work)
                current = this_next
                await work.put(this_work)

                # Wait until the last item has been removed from the queue by a
                # worker before requesting anything else from the awaitables
                # generator. This prevents an awaitable from being held in limbo
                # in this loop without being awaited if the producer or worker
                # raises an exception and exits early.
                await work.join()

            # Send stopping signals to the output loop and workers.
            current.set_result(None)
            for _ in range(concurrency):
                await work.put(None)
        except Exception as e:
            # Signal the output loop to stop.
            current.set_result(None)
            raise

    async def _worker():
        while True:
            this_work = await work.get()
            work.task_done()  # Signal removal from the queue, per the note above in _producer.
            if this_work is None:
                break

            try:
                this_work.result.put_nowait(await this_work.aw)
            except Exception as e:
                # Signal the output loop to stop.
                this_work.result.put_nowait(None)
                raise
            # Do not get another awaitable until this one has been fully handled
            # by the output loop. This limits the number of pending work items
            # to output, which is important their result may not necessarily be
            # small and will be held in memory.
            await this_work.result.join()

    try:
        async with asyncio.TaskGroup() as tg:
            for coro in [_producer(), *[_worker() for _ in range(concurrency)]]:
                tg.create_task(coro)

            # Output loop.
            while True:
                finished_work = await next
                if finished_work is None:
                    break

                this_result = await finished_work.result.get()
                if this_result is None:
                    break

                yield this_result

                # Signal the worker so it can start on another awaitable.
                finished_work.result.task_done()
                # Output the next result per the original ordering of input awaitables.
                next = finished_work.next
    except ExceptionGroup as eg:
        # Raise the first error from the producer or any of the workers.
        for e in eg.exceptions:
            raise e
    except Exception as e:
        raise
    finally:
        # Await any queued awaitables, discarding further errors.
        while not work.empty():
            remaining_work = work.get_nowait()
            if remaining_work is not None:
                try:
                    await remaining_work.aw
                except Exception as e:
                    pass
