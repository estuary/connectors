import asyncio

import pytest
import xxhash

from estuary_cdk.capture.buffer import ConcurrentBuffer


EMPTY_DIGEST = xxhash.xxh3_128().digest().hex()


class TestConcurrentBufferBasics:
    @pytest.mark.asyncio
    async def test_append_then_drain_emits_bytes_in_order(self):
        buf = ConcurrentBuffer()
        await buf.append(b"a\n")
        await buf.append(b"b\n")

        async with buf.drain(suffix=b"ckpt\n") as file:
            contents = file.read()

        assert contents == b"a\nb\nckpt\n"

    @pytest.mark.asyncio
    async def test_drain_truncates_buffer_and_resets_digest(self):
        buf = ConcurrentBuffer()
        await buf.append(b"x\n")
        assert await buf.digest_hex() != EMPTY_DIGEST

        async with buf.drain() as file:
            _ = file.read()

        async with buf.drain() as file:
            assert file.read() == b""
        assert await buf.digest_hex() == EMPTY_DIGEST

    @pytest.mark.asyncio
    async def test_reset_clears_buffer_and_digest(self):
        buf = ConcurrentBuffer()
        await buf.append(b"x\n")
        await buf.reset()

        async with buf.drain() as file:
            assert file.read() == b""
        assert await buf.digest_hex() == EMPTY_DIGEST


class TestConcurrentBufferConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_appends_preserve_all_bytes(self):
        """Many coroutines appending in parallel: every line lands intact
        and the total byte count matches the sum of appends."""
        buf = ConcurrentBuffer()
        n = 100

        async def appender(i: int):
            await buf.append(f"line-{i}\n".encode())

        await asyncio.gather(*(appender(i) for i in range(n)))

        async with buf.drain() as file:
            lines = file.read().splitlines()

        assert sorted(lines) == sorted(f"line-{i}".encode() for i in range(n))

    @pytest.mark.asyncio
    async def test_appends_blocked_during_drain(self):
        """An append issued mid-drain must wait until the drain releases
        the lock — its bytes must NOT appear in the drained file."""
        buf = ConcurrentBuffer()
        await buf.append(b"pre\n")

        appended_event = asyncio.Event()

        async def late_appender():
            await buf.append(b"late\n")
            appended_event.set()

        async with buf.drain() as file:
            late_task = asyncio.create_task(late_appender())
            # Yield repeatedly; the late append should remain blocked because
            # we hold the drain lock.
            for _ in range(10):
                await asyncio.sleep(0)
            assert not appended_event.is_set(), (
                "append should be blocked while drain holds the lock"
            )
            drained = file.read()

        await late_task
        assert drained == b"pre\n"

        async with buf.drain() as file:
            assert file.read() == b"late\n"
