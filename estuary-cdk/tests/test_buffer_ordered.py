import asyncio
import random
from typing import AsyncGenerator, Awaitable

import pytest
import estuary_cdk.buffer_ordered


@pytest.mark.asyncio
async def test_buffer_ordered():
    fixture = [i for i in range(1007)]

    async def _input() -> AsyncGenerator[Awaitable[int], None]:
        for i in fixture:
            # Include a short random delay to keep things interesting.
            yield asyncio.sleep(random.randint(1, 10) / 1000, result=i)

    output = []
    async for result in estuary_cdk.buffer_ordered.buffer_ordered(
        _input(),
        20,
    ):
        output.append(result)

    assert fixture == output
