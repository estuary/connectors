import asyncio
import shutil
from typing import BinaryIO

# Global lock for serializing all emissions to stdout and prevent interleaving output.
_emit_lock = asyncio.Lock()


async def emit_bytes(data: bytes, output: BinaryIO) -> None:
    async with _emit_lock:
        await asyncio.to_thread(_write_bytes, data, output)


async def emit_from_buffer(buffer: BinaryIO, output: BinaryIO) -> None:
    async with _emit_lock:
        await asyncio.to_thread(_copy_buffer, buffer, output)


def _write_bytes(data: bytes, output: BinaryIO) -> None:
    output.write(data)
    output.flush()


def _copy_buffer(buffer: BinaryIO, output: BinaryIO) -> None:
    shutil.copyfileobj(buffer, output)
    output.flush()
