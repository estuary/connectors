from typing import AsyncGenerator

from stream_unzip import async_stream_unzip


class UnzipStream:
    """
    Incrementally decompresses a stream of ZIP file bytes,  yielding uncompressed file contents
    as bytes in an asynchronous generator.

    Example:
    ```python
    async for chunk in UnzipStream(input_bytes, flatten=True):
        ... # process chunk
    ```

    Example usage with IncrementalJsonProcessor:
    ```python
    async for chunk in UnzipStream(input_bytes):
        async for obj in IncrementalJsonProcessor(chunk, "optional_prefix", streamed_item_cls=MyModel):
            ... # process each parsed object
    ```

    Notes:
    - If the ZIP archive is empty, yields nothing.
    - If a file in the archive is empty, yields an empty generator for that file.
    - Filenames are yielded as bytes (per stream_unzip); decode as needed.
    - This is a thin wrapper around stream_unzip.async_stream_unzip and inherits its limitations.
    """

    def __init__(self, input: AsyncGenerator[bytes, None]):
        self.input = input
        self.done = False
        self._stream_iterator = None

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self._stream_iterator is None:
            self._stream_iterator = self._stream()

        try:
            return await anext(self._stream_iterator)
        except StopAsyncIteration:
            self.done = True
            raise

    async def _stream(self) -> bytes:
        async for _, _, uncompressed_chunks in async_stream_unzip(self.input):
            async for chunk in uncompressed_chunks:
                yield chunk
