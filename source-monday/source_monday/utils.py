import asyncio
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncIterator, AsyncGenerator, TypeVar

TGeneratorItem = TypeVar("TGeneratorItem")


async def merge_async_generators(
    *gens: AsyncGenerator[TGeneratorItem, None],
) -> AsyncIterator[TGeneratorItem]:
    queue: asyncio.Queue[TGeneratorItem | None] = asyncio.Queue()

    async def pump(gen: AsyncGenerator[TGeneratorItem, None]):
        try:
            async for item in gen:
                await queue.put(item)
        finally:
            await queue.put(None)

    async with asyncio.TaskGroup() as tg:
        for g in gens:
            tg.create_task(pump(g))

        finished = 0
        while finished < len(gens):
            item = await queue.get()
            if item is None:
                finished += 1
            else:
                yield item


def parse_monday_timestamp(timestamp_str: str, log: Logger) -> datetime:
    """Parse Monday.com 17-digit UNIX timestamp with proper error handling."""
    if not timestamp_str:
        raise ValueError("Empty timestamp string for activity log. Cannot parse.")

    try:
        timestamp_17_digit = int(timestamp_str)

        if timestamp_17_digit <= 0:
            raise ValueError(
                f"Invalid timestamp value: {timestamp_17_digit}. Must be positive."
            )

        timestamp_seconds = timestamp_17_digit / 10_000_000
        result = datetime.fromtimestamp(timestamp_seconds, tz=UTC)

        now = datetime.now(UTC)
        if not (
            datetime(1970, 1, 1, tzinfo=UTC) <= result <= now + timedelta(days=365)
        ):
            raise ValueError(
                f"Parsed timestamp {result} is out of valid range: "
                f"1970-01-01 to {now + timedelta(days=365)}"
            )

        return result

    except (ValueError, TypeError, OverflowError) as e:
        log.warning(f"Unable to parse timestamp '{timestamp_str}': {e}")
        raise
    except Exception as e:
        log.error(f"Unexpected error parsing timestamp '{timestamp_str}': {e}")
        raise
