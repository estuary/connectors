import asyncio
from typing import AsyncIterator, TypeVar

T = TypeVar("T")


async def merge(*iterators: AsyncIterator[T]) -> AsyncIterator[T]:
    """
    Merge multiple async iterators into a single stream in completion order.

    Items are yielded as they become available from any iterator, without
    preserving the original ordering. All iterators run concurrently.

    If any iterator raises an exception, all other iterators are cancelled
    and the exception is propagated after cleanup. Similarly, if the consumer
    stops iterating early (e.g., using break), all iterators are cancelled.

    Args:
        *iterators: Variable number of async iterators to merge. Can be
            async generators or any object implementing AsyncIterator protocol.

    Yields:
        Items from any of the input iterators as they become available.

    Raises:
        Exception: Any exception raised by the input iterators is propagated
            to the caller after all iterators are properly cleaned up.

    Example:
        >>> async def gen1():
        ...     for i in [1, 2, 3]:
        ...         yield i
        ...
        >>> async def gen2():
        ...     for i in [4, 5, 6]:
        ...         yield i
        ...
        >>> async for item in merge(gen1(), gen2()):
        ...     print(item)  # Prints 1-6 in arbitrary order

    Note:
        The bounded queue (size 16) provides backpressure to prevent fast
        iterators from consuming excessive memory while waiting for slow
        consumers. This makes it safe to use with high-throughput streams.
    """
    if not iterators:
        return

    _DONE = object()
    queue: asyncio.Queue[T | BaseException | object] = asyncio.Queue(16)
    active_count = len(iterators)
    cancelling = False

    async def _drain(iterator: AsyncIterator[T]) -> None:
        nonlocal cancelling
        try:
            async for item in iterator:
                await queue.put(item)
        except Exception as e:
            if not cancelling:
                cancelling = True
                await queue.put(e)
        finally:
            await queue.put(_DONE)

    tasks = [asyncio.create_task(_drain(aiter)) for aiter in iterators]

    def _cancel_all_tasks() -> None:
        nonlocal cancelling
        cancelling = True
        for task in tasks:
            if not task.done():
                task.cancel()

    try:
        while active_count > 0:
            item = await queue.get()

            if isinstance(item, BaseException):
                raise item

            if item is _DONE:
                active_count -= 1
            else:
                yield item

    finally:
        _cancel_all_tasks()
        await asyncio.gather(*tasks, return_exceptions=True)

        for iterator in iterators:
            if hasattr(iterator, "aclose"):
                try:
                    await iterator.aclose()
                except Exception:
                    pass
