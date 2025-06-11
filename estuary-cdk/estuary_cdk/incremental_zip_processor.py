from typing import AsyncGenerator, Optional
from logging import Logger
from stream_unzip import async_stream_unzip


class IncrementalZipProcessor:
    """
    Processes a stream of ZIP file bytes incrementally, yielding lines from the
    compressed files within.

    Example usage:
    ```python
    async for line in IncrementalZipProcessor(input_bytes):
        yield MyModel.model_validate_json(line)
    ```
    """

    def __init__(
        self,
        input: AsyncGenerator[bytes, None],
        chunk_size: int = 128 * 1024,
        log: Optional[Logger] = None,
    ):
        self.input = input
        self.chunk_size = chunk_size
        self.log = log
        self._done = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if not hasattr(self, "_processor"):
            self._processor = self._process()

        try:
            return await anext(self._processor)
        except StopAsyncIteration:
            self._done = True
            raise

    async def _process(self) -> AsyncGenerator[bytes, None]:
        files_iterator = async_stream_unzip(self.input)

        async for file_name, _, uncompressed_chunks in files_iterator:
            line_buffer = bytearray()
            file_has_content = False

            async for chunk in uncompressed_chunks:
                file_has_content = True

                last_newline = chunk.rfind(b"\n")

                if last_newline == -1:
                    line_buffer.extend(chunk)
                    continue

                if line_buffer:
                    complete_data = line_buffer + chunk[: last_newline + 1]
                    line_buffer = bytearray(chunk[last_newline + 1 :])
                else:
                    complete_data = chunk[: last_newline + 1]
                    line_buffer = bytearray(chunk[last_newline + 1 :])

                start = 0
                while True:
                    end = complete_data.find(b"\n", start)
                    if end == -1:
                        break

                    line = complete_data[start:end]
                    yield line

                    start = end + 1

            if line_buffer:
                yield bytes(line_buffer)

            if not file_has_content and self.log:
                self.log.debug(f"File {file_name.decode()} appears to be empty")
