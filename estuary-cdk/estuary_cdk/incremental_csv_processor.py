import aiocsv
import aiocsv.protocols
import codecs
import csv
import sys
from typing import Any, AsyncGenerator, ClassVar, Generic, Optional, TypeVar
from dataclasses import dataclass
from pydantic import BaseModel, model_validator
from estuary_cdk.capture.common import BaseDocument


StreamedItem = TypeVar("StreamedItem", bound=BaseModel)

# Python's csv module has a default field size limit of 131,072 bytes,
# and it will raise an _csv.Error exception if a field value is larger
# than that limit. Some users have CSVs with fields larger than 131,072
# bytes, so we max out the limit.
csv.field_size_limit(sys.maxsize)


# BaseCSVRow is a convenience class that automatically handles
# replacing null representations in CSVs with None.
class BaseCSVRow(BaseDocument):
    # NULL_VALUES contains the representation of null values
    # by the source system. These values are replaced with None
    # during validation. If a source system represent nulls
    # differently than the default value, individual connectors
    # can subclass and override NULL_VALUES as needed.
    NULL_VALUES: ClassVar[set[str]] = {""}

    class Config:
        extra = "allow"

    @model_validator(mode='before')
    @classmethod
    def handle_null_values(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {
                key: None if isinstance(value, str) and value in cls.NULL_VALUES else value
                for key, value in data.items()
            }
        return data


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


class IncrementalCSVProcessor(Generic[StreamedItem]):
    """
    Process a stream of CSV bytes incrementally, yielding rows as dictionaries.

    This processor handles CSV data that arrives in chunks and uses incremental
    decoding to handle multi-byte characters that may be split across
    chunk boundaries.

    Example usage with default configuration:
    ```python
    async for row in IncrementalCSVProcessor(byte_iterator, model):
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

    async for row in IncrementalCSVProcessor(byte_iterator, model, config):
        do_something_with(row)
    ```

    Example usage with validation context:
    ```python
    # Pass context to the pydantic model validation
    context = {'data_source': 'bulk_api'}
    
    async for row in IncrementalCSVProcessor(byte_iterator, model, validation_context=context):
        do_something_with(row)
    ```

    Example usage with explicit field names (for CSV without headers):
    ```python
    # For CSV files without header rows, provide explicit field names
    fieldnames = ['name', 'age', 'city']
    
    async for row in IncrementalCSVProcessor(byte_iterator, model, fieldnames=fieldnames):
        do_something_with(row)
    ```
    """

    def __init__(
            self,
            byte_iterator: AsyncGenerator[bytes, None],
            streamed_item_cls: type[StreamedItem],
            config: Optional[CSVConfig] = None,
            validation_context: Optional[object] = None,
            fieldnames: Optional[list[str]] = None,
        ):
        """
        Initialize the processor with byte iterator and optional CSV configuration or validation context.

        Args:
            byte_iterator: Async generator of CSV byte chunks
            streamed_item_cls: Pydantic model class for validation
            config: Optional CSV configuration options
            validation_context: Optional validation context object passed to pydantic model_validate
            fieldnames: Optional list of field names to use for CSV columns. If None, uses first row as headers.
        """
        self.byte_iterator = byte_iterator
        self.config = config or CSVConfig()
        self.streamed_item_cls = streamed_item_cls
        self.validation_context = validation_context
        self.fieldnames = fieldnames
        self._row_iterator: Optional[AsyncGenerator[dict[str, Any]]] = None

    def __aiter__(self):
        return self

    async def __anext__(self) -> StreamedItem:
        if self._row_iterator is None:
            self._row_iterator = self._process_stream()

        try:
            row_data = await self._row_iterator.__anext__()
            if self.validation_context:
                return self.streamed_item_cls.model_validate(row_data, context=self.validation_context)
            else:
                return self.streamed_item_cls.model_validate(row_data)
        except StopAsyncIteration:
            raise

    async def _process_stream(self) -> AsyncGenerator[dict[str, Any], None]:
        """
        Internal method to process the byte stream and yield CSV records.

        Yields:
            dict[str, Any]: Complete CSV records as dictionaries

        Raises:
            CSVProcessingError: When CSV data is malformed or cannot be parsed
        """

        class AsyncByteReader(aiocsv.protocols.WithAsyncRead):
            """Internal class that handles incremental decoding for aiocsv."""

            def __init__(self, byte_gen: AsyncGenerator[bytes, None], encoding: str):
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

        if self.fieldnames is not None:
            reader_kwargs['fieldnames'] = self.fieldnames

        try:
            async for row in aiocsv.AsyncDictReader(async_reader, **reader_kwargs):
                yield row
        except csv.Error as e:
            raise CSVProcessingError(f"Failed to parse CSV data: {str(e)}", config=reader_kwargs) from e
