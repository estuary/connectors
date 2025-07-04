import aiocsv
import aiocsv.protocols
import codecs
import csv
import sys
from typing import AsyncIterator, Optional, Any
from dataclasses import dataclass


# Python's csv module has a default field size limit of 131,072 bytes,
# and it will raise an _csv.Error exception if a field value is larger
# than that limit. Some users have CSVs with fields larger than 131,072
# bytes, so we max out the limit.
csv.field_size_limit(sys.maxsize)


@dataclass
class CSVConfig:
    """Configuration for CSV processing."""
    delimiter: str = ','
    quotechar: str = '"'
    escapechar: Optional[str] = None
    doublequote: bool = True
    skipinitialspace: bool = False
    lineterminator: str = '\r\n'
    quoting: int = 0 
    strict: bool = True
    encoding: str = 'utf-8'


class CSVProcessingError(Exception):
    """Exception raised when CSV processing fails."""

    def __init__(self, message: str, config: Optional[dict] = None):
        self.config = config
        if config:
            config_str = ", ".join([f"{k}={repr(v)}" for k, v in config.items()])
            message = f"{message}. CSV reader configuration: {config_str}"
        super().__init__(message)


class IncrementalCSVProcessor:
    """
    Process a stream of CSV bytes incrementally, yielding rows as dictionaries.

    This processor handles CSV data that arrives in chunks and uses incremental
    decoding to handle multi-byte characters that may be split across
    chunk boundaries.

    Example usage with default configuration:
    ```python
    async for row in IncrementalCSVProcessor(byte_iterator):
        do_something_with(row)
    ```

    Example usage with custom configuration:
    ```python
    # For tab-separated values with different encoding
    config = CSVConfig(
        delimiter='\t',
        encoding='utf-8-sig',
        lineterminator='\n'
    )

    async for row in IncrementalCSVProcessor(byte_iterator, config):
        do_something_with(row)
    ```
    """

    def __init__(self, byte_iterator: AsyncIterator[bytes], config: Optional[CSVConfig] = None):
        """
        Initialize the processor with byte iterator and optional CSV configuration.

        Args:
            byte_iterator: Async iterator of CSV byte chunks
            config: CSV configuration options
        """
        self.byte_iterator = byte_iterator
        self.config = config or CSVConfig()
        self._row_iterator: Optional[AsyncIterator[dict[str, Any]]] = None

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._row_iterator is None:
            self._row_iterator = self._process_stream()

        try:
            return await self._row_iterator.__anext__()
        except StopAsyncIteration:
            raise

    async def _process_stream(self) -> AsyncIterator[dict[str, Any]]:
        """
        Internal method to process the byte stream and yield CSV records.

        Yields:
            dict[str, Any]: Complete CSV records as dictionaries

        Raises:
            CSVProcessingError: When CSV data is malformed or cannot be parsed
        """

        class AsyncByteReader(aiocsv.protocols.WithAsyncRead):
            """Internal class that handles incremental decoding for aiocsv."""

            def __init__(self, byte_gen: AsyncIterator[bytes], encoding: str):
                self.byte_gen = byte_gen
                self.decoder = codecs.getincrementaldecoder(encoding)(errors='strict')
                self._exhausted = False

            async def read(self, size: int = -1) -> str:
                """Read and incrementally decode data from the byte stream."""
                if self._exhausted:
                    return ""

                try:
                    chunk = await self.byte_gen.__anext__()
                    # Use incremental decoder to handle multi-byte characters
                    # that may be split across chunk boundaries.
                    return self.decoder.decode(chunk, final=False)

                except StopAsyncIteration:
                    self._exhausted = True

                    # Finalize the decoder to get any remaining characters.
                    # This will raise UnicodeDecodeError if there are incomplete characters.
                    final_chunk = self.decoder.decode(b'', final=True)

                    return final_chunk if final_chunk else ""

        async_reader = AsyncByteReader(self.byte_iterator, self.config.encoding)

        reader_kwargs = {
            'delimiter': self.config.delimiter,
            'quotechar': self.config.quotechar,
            'doublequote': self.config.doublequote,
            'skipinitialspace': self.config.skipinitialspace,
            'lineterminator': self.config.lineterminator,
            'quoting': self.config.quoting,
            'strict': self.config.strict,
        }

        if self.config.escapechar is not None:
            reader_kwargs['escapechar'] = self.config.escapechar

        try:
            async for row in aiocsv.AsyncDictReader(async_reader, **reader_kwargs):
                yield row
        except csv.Error as e:
            raise CSVProcessingError(f"Failed to parse CSV data: {str(e)}", config=reader_kwargs) from e
