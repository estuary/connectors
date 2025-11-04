import asyncio
from typing import AsyncGenerator

import pytest

from estuary_cdk.async_streams import merge


@pytest.mark.asyncio
async def test_merge_basic():
    """Test basic merging of multiple generators."""

    async def gen1() -> AsyncGenerator[int, None]:
        for i in [1, 2, 3]:
            await asyncio.sleep(0.01)
            yield i

    async def gen2() -> AsyncGenerator[int, None]:
        for i in [4, 5, 6]:
            await asyncio.sleep(0.01)
            yield i

    async def gen3() -> AsyncGenerator[int, None]:
        for i in [7, 8, 9]:
            await asyncio.sleep(0.01)
            yield i

    results = []
    async for item in merge(gen1(), gen2(), gen3()):
        results.append(item)

    # All items should be present (order may vary due to concurrency)
    assert sorted(results) == list(range(1, 10))


@pytest.mark.asyncio
async def test_merge_empty_input():
    """Test merge with no generators."""
    results = []
    async for item in merge():
        results.append(item)

    assert results == []


@pytest.mark.asyncio
async def test_merge_single_generator():
    """Test merge with a single generator."""

    async def gen() -> AsyncGenerator[int, None]:
        for i in range(5):
            yield i

    results = []
    async for item in merge(gen()):
        results.append(item)

    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_merge_exception_propagation():
    """Test that exceptions from generators are propagated."""

    async def gen1() -> AsyncGenerator[int, None]:
        yield 1
        await asyncio.sleep(0.01)
        yield 2

    async def gen2() -> AsyncGenerator[int, None]:
        yield 3
        await asyncio.sleep(0.005)
        raise ValueError("Test exception")

    async def gen3() -> AsyncGenerator[int, None]:
        for i in [4, 5, 6]:
            await asyncio.sleep(0.02)
            yield i

    results = []
    with pytest.raises(ValueError, match="Test exception"):
        async for item in merge(gen1(), gen2(), gen3()):
            results.append(item)

    # Should have received at least some items before the exception
    assert len(results) > 0


@pytest.mark.asyncio
async def test_merge_early_termination():
    """Test that breaking out of the merge loop properly cleans up."""

    cleanup_flags = {"gen1": False, "gen2": False, "gen3": False}

    async def gen1() -> AsyncGenerator[int, None]:
        try:
            for i in range(100):
                yield i
                await asyncio.sleep(0.001)
        finally:
            cleanup_flags["gen1"] = True

    async def gen2() -> AsyncGenerator[int, None]:
        try:
            for i in range(100, 200):
                yield i
                await asyncio.sleep(0.001)
        finally:
            cleanup_flags["gen2"] = True

    async def gen3() -> AsyncGenerator[int, None]:
        try:
            for i in range(200, 300):
                yield i
                await asyncio.sleep(0.001)
        finally:
            cleanup_flags["gen3"] = True

    results = []
    async for item in merge(gen1(), gen2(), gen3()):
        results.append(item)
        if len(results) >= 10:
            break

    # Give cleanup a moment to complete
    await asyncio.sleep(0.05)

    # All generators should have been cleaned up
    assert cleanup_flags["gen1"]
    assert cleanup_flags["gen2"]
    assert cleanup_flags["gen3"]
    assert len(results) == 10


@pytest.mark.asyncio
async def test_merge_generator_cleanup_on_exception():
    """Test that all generators are properly closed when one raises an exception."""

    cleanup_flags = {"gen1": False, "gen2": False, "gen3": False}

    async def gen1() -> AsyncGenerator[int, None]:
        try:
            for i in range(10):
                yield i
                await asyncio.sleep(0.01)
        finally:
            cleanup_flags["gen1"] = True

    async def gen2() -> AsyncGenerator[int, None]:
        try:
            yield 100
            await asyncio.sleep(0.005)
            raise RuntimeError("Intentional error")
        finally:
            cleanup_flags["gen2"] = True

    async def gen3() -> AsyncGenerator[int, None]:
        try:
            for i in range(200, 210):
                yield i
                await asyncio.sleep(0.01)
        finally:
            cleanup_flags["gen3"] = True

    with pytest.raises(RuntimeError, match="Intentional error"):
        async for item in merge(gen1(), gen2(), gen3()):
            pass

    # Give cleanup a moment to complete
    await asyncio.sleep(0.05)

    # All generators should have been cleaned up
    assert cleanup_flags["gen1"]
    assert cleanup_flags["gen2"]
    assert cleanup_flags["gen3"]


@pytest.mark.asyncio
async def test_merge_different_speeds():
    """Test merging generators with different speeds."""

    async def fast_gen() -> AsyncGenerator[str, None]:
        for i in range(10):
            yield f"fast-{i}"
            await asyncio.sleep(0.001)

    async def slow_gen() -> AsyncGenerator[str, None]:
        for i in range(5):
            yield f"slow-{i}"
            await asyncio.sleep(0.01)

    results = []
    async for item in merge(fast_gen(), slow_gen()):
        results.append(item)

    # Check all items are present
    fast_items = [r for r in results if r.startswith("fast-")]
    slow_items = [r for r in results if r.startswith("slow-")]

    assert len(fast_items) == 10
    assert len(slow_items) == 5
    assert len(results) == 15


@pytest.mark.asyncio
async def test_merge_backpressure():
    """Test that bounded queue provides backpressure."""

    produced_count = {"count": 0}
    consumed_count = {"count": 0}

    async def fast_producer() -> AsyncGenerator[int, None]:
        for i in range(100):
            produced_count["count"] += 1
            yield i
            # No delay - produces as fast as possible

    results = []
    async for item in merge(fast_producer()):
        consumed_count["count"] += 1
        results.append(item)
        await asyncio.sleep(0.001)  # Slow consumer

        # Check that producer hasn't run too far ahead
        # The queue is bounded (size 16), so producer should be blocked
        if consumed_count["count"] == 20:
            QUEUE_SIZE = 16
            BUFFER_MARGIN = 14
            assert produced_count["count"] < consumed_count["count"] + QUEUE_SIZE + BUFFER_MARGIN
            break

    assert len(results) == 20


@pytest.mark.asyncio
async def test_merge_cancellation():
    """Test that merge handles task cancellation properly."""

    cleanup_flag = {"cleaned": False}

    async def gen() -> AsyncGenerator[int, None]:
        try:
            for i in range(100):
                yield i
                await asyncio.sleep(0.01)
        finally:
            cleanup_flag["cleaned"] = True

    async def run_merge():
        async for item in merge(gen()):
            if item >= 5:
                raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await run_merge()

    # Give cleanup a moment to complete
    await asyncio.sleep(0.05)

    # Generator should have been cleaned up
    assert cleanup_flag["cleaned"]


@pytest.mark.asyncio
async def test_merge_many_generators():
    """Test merging many generators at once."""

    async def numbered_gen(n: int) -> AsyncGenerator[tuple[int, int], None]:
        for i in range(10):
            yield (n, i)
            await asyncio.sleep(0.001)

    # Merge 20 generators
    generators = [numbered_gen(i) for i in range(20)]

    results = []
    async for item in merge(*generators):
        results.append(item)

    # Should receive all items
    assert len(results) == 200

    # Check all generator IDs are present
    gen_ids = set(item[0] for item in results)
    assert gen_ids == set(range(20))


@pytest.mark.asyncio
async def test_merge_preserves_item_types():
    """Test that merge preserves different item types correctly."""

    async def int_gen() -> AsyncGenerator[int, None]:
        for i in [1, 2, 3]:
            yield i

    async def str_gen() -> AsyncGenerator[str, None]:
        for s in ["a", "b", "c"]:
            yield s

    async def dict_gen() -> AsyncGenerator[dict, None]:
        for d in [{"x": 1}, {"y": 2}]:
            yield d

    results = []
    async for item in merge(int_gen(), str_gen(), dict_gen()):
        results.append(item)

    # Check types are preserved
    ints = [r for r in results if isinstance(r, int)]
    strs = [r for r in results if isinstance(r, str)]
    dicts = [r for r in results if isinstance(r, dict)]

    assert len(ints) == 3
    assert len(strs) == 3
    assert len(dicts) == 2


@pytest.mark.asyncio
async def test_merge_empty_generator():
    """Test merging where one generator is empty."""

    async def empty_gen() -> AsyncGenerator[int, None]:
        empty_list: list[int] = []
        for i in empty_list:
            yield i

    async def normal_gen() -> AsyncGenerator[int, None]:
        for i in [1, 2, 3]:
            yield i

    results = []
    async for item in merge(empty_gen(), normal_gen()):
        results.append(item)

    assert results == [1, 2, 3]


@pytest.mark.asyncio
async def test_merge_all_empty_generators():
    """Test merging multiple empty generators."""

    async def empty_gen() -> AsyncGenerator[int, None]:
        empty_list: list[int] = []
        for i in empty_list:
            yield i

    results = []
    async for item in merge(empty_gen(), empty_gen(), empty_gen()):
        results.append(item)

    assert results == []
