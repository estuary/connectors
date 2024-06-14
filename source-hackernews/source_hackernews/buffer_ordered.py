import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, TypeVar, ContextManager
from contextlib import asynccontextmanager

T = TypeVar("T")


@dataclass
class BufferWork[T]:
    aw: Awaitable[T]
    result: asyncio.Queue[T | None]
    next: asyncio.Future["BufferWork[T] | None"]


@asynccontextmanager
async def managed_buffer_ordered(
    aws: AsyncGenerator[Awaitable[T], None],
    concurrency: int,
) -> AsyncGenerator[AsyncGenerator[T, None], None]:
    """
    A context manager that ensures proper cleanup of resources when using buffer_ordered.
    
    Args:
        aws: A stream of awaitables to run concurrently.
        concurrency: The maximum number of concurrent awaitables to run.
        
    Yields:
        An async generator that yields results from the awaitables in order.
    """
    work: asyncio.Queue[BufferWork[T] | None] = asyncio.Queue(1)
    tasks = []
    producer_task = None
    worker_tasks = []

    async def _producer(next_future: asyncio.Future[BufferWork[T] | None]):
        current = next_future
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
                # generator.
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
            work.task_done()
            if this_work is None:
                break

            try:
                this_work.result.put_nowait(await this_work.aw)
            except Exception as e:
                # Signal the output loop to stop.
                this_work.result.put_nowait(None)
                raise
            await this_work.result.join()

    async def _run():
        next_future = asyncio.Future[BufferWork[T] | None]()
        try:
            async with asyncio.TaskGroup() as tg:
                # Create producer task
                producer_task = tg.create_task(_producer(next_future))
                tasks.append(producer_task)
                
                # Create worker tasks
                for _ in range(concurrency):
                    worker_task = tg.create_task(_worker())
                    worker_tasks.append(worker_task)
                    tasks.append(worker_task)

                # Output loop.
                while True:
                    finished_work = await next_future
                    if finished_work is None:
                        break

                    this_result = await finished_work.result.get()
                    if this_result is None:
                        break

                    yield this_result

                    finished_work.result.task_done()
                    next_future = finished_work.next
        except ExceptionGroup as eg:
            # Raise the first error from the producer or any of the workers.
            for e in eg.exceptions:
                raise e
        except Exception as e:
            raise
        finally:
            # Cancel all tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for all tasks to complete
            if tasks:
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                except Exception as e:
                    pass  # Ignore exceptions during cleanup
            
            # Ensure all worker tasks are done
            for task in worker_tasks:
                if not task.done():
                    try:
                        await task
                    except Exception as e:
                        pass  # Ignore exceptions during cleanup
            
            # Ensure producer task is done
            if producer_task and not producer_task.done():
                try:
                    await producer_task
                except Exception as e:
                    pass  # Ignore exceptions during cleanup
            
            # Await any queued awaitables, discarding further errors.
            while not work.empty():
                remaining_work = work.get_nowait()
                if remaining_work is not None:
                    try:
                        await remaining_work.aw
                    except Exception as e:
                        pass

    async def _generator():
        async for result in _run():
            yield result

    try:
        yield _generator()
    finally:
        # Ensure all tasks are cancelled and cleaned up
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                pass  # Ignore exceptions during cleanup
        
        # Ensure all worker tasks are done
        for task in worker_tasks:
            if not task.done():
                try:
                    await task
                except Exception as e:
                    pass  # Ignore exceptions during cleanup
        
        # Ensure producer task is done
        if producer_task and not producer_task.done():
            try:
                await producer_task
            except Exception as e:
                pass  # Ignore exceptions during cleanup


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
        aws: A stream of awaitables to run concurrently.
        concurrency: The maximum number of concurrent awaitables to run.

    Returns:
        A stream of results from the awaitables, with the order matching the
        order yielded by 'aws'.
    """
    async with managed_buffer_ordered(aws, concurrency) as gen:
        async for result in gen:
            yield result 