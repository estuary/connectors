from typing import AsyncGenerator, Tuple, Union

from stream_unzip import async_stream_unzip


class UnzipStream:
    """
    Incrementally decompresses a stream of ZIP file bytes, yielding either (filename, AsyncGenerator[bytes, None])
    for each file in the archive, or just the uncompressed bytes if flatten=True. Each yielded async generator produces
    the uncompressed bytes of the file, chunked as provided by the underlying stream_unzip library.

    Example usage (default, per-file):
    ```python
    async for filename, file_bytes in UnzipStream(input_bytes):
        async for chunk in file_bytes:
            ... # process chunk
    ```

    Example usage (flattened):
    ```python
    async for chunk in UnzipStream(input_bytes, flatten=True):
        ... # process chunk from all files in order
    ```

    Example usage with NDJSON:
    ```python
    async for filename, file_bytes in UnzipStream(input_bytes):
        async for obj in IncrementalJsonProcessor(file_bytes, ndjson=True, streamed_item_cls=MyModel):
            ... # process each parsed object
    ```

    Edge Cases & Notes:
    - If the ZIP archive is empty, yields nothing.
    - If a file in the archive is empty, yields an empty generator for that file.
    - If flatten=True, all file contents are yielded in archive order, with no file boundaries.
    - Filenames are yielded as bytes (per stream_unzip), decode as needed.
    - This is a thin wrapper around stream_unzip.async_stream_unzip and inherits its limitations.
    """

    def __init__(self, input: AsyncGenerator[bytes, None], flatten: bool = False):
        self.input = input
        self.flatten = flatten
        self.done = False
        self._stream_iterator = None

    def __aiter__(self):
        return self

    async def __anext__(
        self,
    ) -> Union[Tuple[bytes, AsyncGenerator[bytes, None]], bytes]:
        if self._stream_iterator is None:
            self._stream_iterator = self._stream()

        try:
            return await anext(self._stream_iterator)
        except StopAsyncIteration:
            self.done = True
            raise

    async def _stream(self):
        async for filename, _, uncompressed_chunks in async_stream_unzip(self.input):
            if self.flatten:
                async for chunk in uncompressed_chunks:
                    yield chunk
            else:
                yield filename, uncompressed_chunks
