import asyncio
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import BinaryIO

import xxhash


class ConcurrentBuffer:
    """Append-only spool with atomic drain, safe under concurrent producers.

    All buffer-mutating operations are serialized by an internal lock.
    drain() yields the underlying file under the same lock, so writes
    cannot race against the read+truncate cycle that emits buffered bytes
    to the connector's output (which happens on a worker thread via
    asyncio.to_thread — a real cross-thread race that the lock prevents).

    Owns a content digest (xxh3_128) covering everything appended since
    the last drain or reset.
    """

    DEFAULT_MAX_BUFFER_MEM: int = 1_000_000

    def __init__(self, max_size: int = DEFAULT_MAX_BUFFER_MEM):
        self._buf: tempfile.SpooledTemporaryFile = tempfile.SpooledTemporaryFile(
            max_size=max_size
        )
        self._hasher: xxhash.xxh3_128 = xxhash.xxh3_128()
        self._lock: asyncio.Lock = asyncio.Lock()

    async def append(self, data: bytes) -> None:
        async with self._lock:
            self._buf.write(data)
            self._hasher.update(data)

    async def digest_hex(self) -> str:
        async with self._lock:
            return self._hasher.digest().hex()

    async def reset(self) -> None:
        async with self._lock:
            self._buf.truncate(0)
            self._buf.seek(0)
            self._hasher.reset()

    @asynccontextmanager
    async def drain(self, suffix: bytes = b"") -> AsyncIterator[BinaryIO]:
        """Hold the buffer exclusively for emission. `suffix` (e.g. a
        checkpoint marker) is written at the end of the existing bytes
        before the file is seeked to 0 and yielded for reading. Doing
        this inside the drain lock is what keeps the suffix terminal:
        if the caller did append(marker) and then drain() in two steps,
        a concurrent append() could land between them and place bytes
        after the marker. The suffix bypasses the digest because the
        digest is reset on exit anyway. On exit (including exceptional
        exit), the buffer is truncated and the digest is reset."""
        async with self._lock:
            self._buf.write(suffix)
            self._buf.seek(0)
            try:
                yield self._buf
            finally:
                self._buf.truncate(0)
                self._buf.seek(0)
                self._hasher.reset()
