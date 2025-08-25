import zlib
from typing import AsyncGenerator

class GunzipStream:
    """
    Incrementally decompress a stream of Gzip-compressed bytes from an async generator.
    Usage:
      async for chunk in GunzipStream(async_bytes):
          ... # process decompressed bytes
    """
    def __init__(self, input: AsyncGenerator[bytes, None]):
        self.input = input
        self.decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
        self.done = False
        self._input_iter = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._input_iter is None:
            self._input_iter = self._stream()
        return await anext(self._input_iter)

    async def _stream(self):
        async for chunk in self.input:
            if not chunk:
                continue
            data = self.decompressor.decompress(chunk)
            if data:
                yield data
        # Flush any buffered data at the end of the stream
        leftover = self.decompressor.flush()
        if leftover:
            yield leftover
