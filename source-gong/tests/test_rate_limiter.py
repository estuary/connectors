import asyncio
import time

import pytest

from source_gong.api import GongRateLimiter


@pytest.mark.asyncio
async def test_acquire_does_not_deadlock():
    """Rate limiter should not deadlock when limit is hit."""
    limiter = GongRateLimiter(max_requests=3, window_seconds=0.5)

    # Acquire all 3 slots
    for _ in range(3):
        await limiter.acquire()

    # 4th acquire should block until window expires, not deadlock
    start = time.monotonic()
    await asyncio.wait_for(limiter.acquire(), timeout=2.0)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.3, "Should have waited for window to expire"


@pytest.mark.asyncio
async def test_acquire_within_limit():
    """Acquires within limit should not block."""
    limiter = GongRateLimiter(max_requests=5, window_seconds=60.0)

    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    elapsed = time.monotonic() - start

    assert elapsed < 0.1, "Should not block when under limit"


@pytest.mark.asyncio
async def test_concurrent_callers():
    """Multiple concurrent callers should all eventually acquire."""
    limiter = GongRateLimiter(max_requests=2, window_seconds=0.3)

    results: list[float] = []

    async def worker():
        await limiter.acquire()
        results.append(time.monotonic())

    tasks = [asyncio.create_task(worker()) for _ in range(4)]
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=3.0)

    assert len(results) == 4, "All workers should complete"
