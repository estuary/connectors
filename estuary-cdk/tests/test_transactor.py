import asyncio
import io

import pytest

from estuary_cdk.capture.transactor import Transactor


def _buffer(payload: bytes = b"hello\n") -> io.BytesIO:
    """Build a buffer positioned at 0 for emit_from_buffer to drain."""
    buf = io.BytesIO()
    _ = buf.write(payload)
    _ = buf.seek(0)
    return buf


async def _yield_until_pending(transactor: Transactor, expected: int) -> None:
    """Spin the event loop until `expected` slots are pending, or fail."""

    async def wait() -> None:
        while (
            len(transactor._pending) < expected  # pyright: ignore[reportPrivateUsage]
        ):
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=1.0)


async def _commit_then_wait(transactor: Transactor, buf: io.BytesIO) -> None:
    """Drive a full waiting commit: emit, then block on the ACK future."""
    completion = await transactor.commit(buf, wait_for_ack=True)
    assert completion is not None
    await completion


class TestTransactor:
    @pytest.mark.asyncio
    async def test_commit_without_ack_tracking_emits_and_returns_no_future(self):
        output = io.BytesIO()
        transactor = Transactor(output)

        completion = await transactor.commit(_buffer(b"line\n"), wait_for_ack=False)

        assert completion is None
        assert output.getvalue() == b"line\n"
        assert len(transactor._pending) == 0  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_wait_for_ack_on_untracked_transactor_raises(self):
        output = io.BytesIO()
        transactor = Transactor(output)

        with pytest.raises(RuntimeError, match="requires_explicit_acks=True"):
            await transactor.commit(_buffer(b"line\n"), wait_for_ack=True)

    @pytest.mark.asyncio
    async def test_commit_blocks_until_ack(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        commit_task = asyncio.create_task(
            _commit_then_wait(transactor, _buffer(b"line\n"))
        )
        await _yield_until_pending(transactor, 1)
        assert not commit_task.done()
        assert (
            output.getvalue() == b"line\n"
        ), "buffer is emitted before the ACK arrives"

        transactor.deliver_ack(1)
        await asyncio.wait_for(commit_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_fire_and_forget_returns_before_ack(self):
        """When ACK tracking is on but the caller doesn't need to wait (a
        pull-API checkpoint in a mixed connector), commit() returns as soon
        as the buffer is on stdout. The slot still sits in the FIFO until
        Flow's ACK drains it."""
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        await transactor.commit(_buffer(b"line\n"), wait_for_ack=False)

        assert output.getvalue() == b"line\n"
        assert len(transactor._pending) == 1  # pyright: ignore[reportPrivateUsage]

        transactor.deliver_ack(1)
        assert len(transactor._pending) == 0  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_mixed_waiting_and_fire_and_forget_drain_in_order(self):
        """A pull-API commit's slot drains correctly even when interleaved
        with a webhook commit that is awaiting its own ACK."""
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        await transactor.commit(_buffer(b"a\n"), wait_for_ack=False)
        webhook = asyncio.create_task(
            _commit_then_wait(transactor, _buffer(b"b\n"))
        )
        await _yield_until_pending(transactor, 2)

        transactor.deliver_ack(1)  # drains the fire-and-forget slot
        assert not webhook.done(), "webhook commit must wait for its own ACK"

        transactor.deliver_ack(1)
        await asyncio.wait_for(webhook, timeout=1.0)
        assert output.getvalue() == b"a\nb\n"

    @pytest.mark.asyncio
    async def test_concurrent_waiting_commits_resolve_in_arrival_order(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        first = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"a\n")))
        second = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"b\n")))
        await _yield_until_pending(transactor, 2)

        assert not first.done() and not second.done()

        transactor.deliver_ack(1)
        await asyncio.wait_for(first, timeout=1.0)
        assert not second.done(), "second commit must wait for its own ACK"

        transactor.deliver_ack(1)
        await asyncio.wait_for(second, timeout=1.0)
        assert output.getvalue() == b"a\nb\n"

    @pytest.mark.asyncio
    async def test_batched_ack_resolves_multiple_commits(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        first = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"a\n")))
        second = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"b\n")))
        await _yield_until_pending(transactor, 2)

        transactor.deliver_ack(2)
        await asyncio.wait_for(first, timeout=1.0)
        await asyncio.wait_for(second, timeout=1.0)

    @pytest.mark.asyncio
    async def test_ack_without_pending_checkpoint_raises(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        with pytest.raises(RuntimeError, match="only 0 are pending"):
            transactor.deliver_ack(1)

    @pytest.mark.asyncio
    async def test_ack_exceeding_pending_count_raises(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        commit_task = asyncio.create_task(_commit_then_wait(transactor, _buffer()))
        await _yield_until_pending(transactor, 1)

        with pytest.raises(RuntimeError, match="only 1 are pending"):
            transactor.deliver_ack(2)

        transactor.deliver_ack(1)
        await asyncio.wait_for(commit_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_deliver_zero_is_noop(self):
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        commit_task = asyncio.create_task(_commit_then_wait(transactor, _buffer()))
        await _yield_until_pending(transactor, 1)

        transactor.deliver_ack(0)
        assert not commit_task.done()

        transactor.deliver_ack(1)
        await asyncio.wait_for(commit_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_untracked_acks_are_ignored(self):
        """When ACK tracking is disabled, deliver_ack is a no-op even with a
        non-zero count, so pull-only connectors can ignore stray ACKs."""
        output = io.BytesIO()
        transactor = Transactor(output)

        transactor.deliver_ack(5)

    @pytest.mark.asyncio
    async def test_reusable_after_full_drain(self):
        """After a full commit/ack/drain cycle, the Transactor accepts new
        commits and drains them correctly."""
        output = io.BytesIO()
        transactor = Transactor(output, requires_explicit_acks=True)

        first = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"a\n")))
        await _yield_until_pending(transactor, 1)
        transactor.deliver_ack(1)
        await asyncio.wait_for(first, timeout=1.0)

        second = asyncio.create_task(_commit_then_wait(transactor, _buffer(b"b\n")))
        await _yield_until_pending(transactor, 1)
        transactor.deliver_ack(1)
        await asyncio.wait_for(second, timeout=1.0)
