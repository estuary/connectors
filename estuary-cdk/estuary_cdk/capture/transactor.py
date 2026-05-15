import asyncio
from collections import deque
from typing import BinaryIO

from ._emit import emit_from_buffer


class Transactor:
    """
    Coordinates stdout writes and pairs each emitted checkpoint with the
    Acknowledge message that Flow returns once the checkpoint is durably
    persisted.

    When `requires_explicit_acks` is True, every commit registers a slot in
    the pending FIFO so its ordering matches the ACK stream. A commit only
    blocks on the slot when its caller passes `wait_for_ack=True`; otherwise
    the slot is drained on ACK without anyone awaiting it. When False, no
    FIFO bookkeeping happens and stray ACKs are ignored.
    """

    def __init__(self, output: BinaryIO, requires_explicit_acks: bool = False):
        self._output: BinaryIO = output
        self._emit_lock: asyncio.Lock = asyncio.Lock()
        self._pending: deque[asyncio.Future[None] | None] = deque()
        self._tracks_acks: bool = requires_explicit_acks

    async def commit(
        self,
        buffer: BinaryIO,
        wait_for_ack: bool = False,
    ) -> asyncio.Future[None] | None:
        """Emit `buffer` (already containing captured + checkpoint bytes) to
        stdout in commit order. Returns a future the caller awaits to block
        until Flow ACKs this checkpoint when `wait_for_ack` is True; otherwise
        returns None."""
        _ = buffer.seek(0)

        if not self._tracks_acks:
            if wait_for_ack:
                raise RuntimeError(
                    (
                        "wait_for_ack=True requires a Transactor constructed with "
                        "requires_explicit_acks=True"
                    )
                )
            await emit_from_buffer(buffer, self._output)
            return None

        async with self._emit_lock:
            completion = (
                asyncio.get_running_loop().create_future() if wait_for_ack else None
            )
            self._pending.append(completion)

            await emit_from_buffer(buffer, self._output)

        return completion

    def deliver_ack(self, count: int) -> None:
        """Forward an Acknowledge.checkpoints count from Flow, draining the
        first `count` pending slots and resolving their completion futures."""
        if not self._tracks_acks:
            return
        if count == 0:
            return

        if count > len(self._pending):
            raise RuntimeError(
                f"received ACK for {count} checkpoint(s) but only {len(self._pending)} are pending"
            )

        for _ in range(count):
            slot = self._pending.popleft()
            if slot is None:
                continue
            if not slot.done():
                slot.set_result(None)
