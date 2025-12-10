from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Generic, List, Optional, TypeVar

import ijson
from pydantic import BaseModel

StreamedItem = TypeVar("StreamedItem", bound=BaseModel)

Remainder = TypeVar("Remainder", bound=BaseModel)


class _NoopRemainder(BaseModel):
    pass


class IncrementalJsonProcessor(Generic[StreamedItem, Remainder]):
    """
    Processes a stream of JSON or NDJSON bytes incrementally, yielding objects of
    type `streamed_item_cls` along the way. Once iteration is complete, the
    "remainder" of the input that was not present under the specific prefix can
    be obtained using the `get_remainder` method.

    The prefix is a path within the JSON document where objects to parse reside.
    Usually it will end with the ".item" suffix, which allows iteration through
    objects in an array. Prefixes can be used with NDJSON to extract specific
    nested objects from each newline-delimited JSON object. More nuanced
    scenarios are also possible.

    Example usage if you only want the items and don't care about the remainder:
    ```python
    async for item in IncrementalJsonProcessor(
        input,
        prefix="data.items",
        streamed_item_cls=MyItem,
    ):
        do_something_with(item)
    ```

    Example usage for NDJSON with no prefix:
    ```python
    async for item in IncrementalJsonProcessor(
        input,
        prefix="",  # Use "" to yield the whole object per line
        streamed_item_cls=MyItem,
    ):
        do_something_with(item)
    ```

    For using the remainder after iteration is complete, keep a reference to the
    processor:
    ```python
    processor = IncrementalJsonProcessor(
        input,
        prefix="data.items",
        streamed_item_cls=MyItem,
        remainder_cls=MyRemainder,
    )

    async for item in processor:
        do_something_with(item)

    do_something_with(processor.get_remainder())
    ```

    See the tests file `test_incremental_json_processor.py` for more examples,
    including various formulations of the prefix to support more complex data
    structures.
    """

    def __init__(
        self,
        input: AsyncGenerator[bytes, None],
        prefix: str,
        streamed_item_cls: type[StreamedItem],
        remainder_cls: type[Remainder] = _NoopRemainder,
        validation_context: Optional[object] = None,
    ):
        self.input = input
        self.prefix = prefix
        self.streamed_item_cls = streamed_item_cls
        self.remainder_cls = remainder_cls
        self.validation_context = validation_context
        self.parser = ijson.parse_async(
            _AsyncStreamWrapper(input),
            multiple_values=True,
        )
        self.remainder_builder = _ObjectBuilder()
        self.done = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> StreamedItem:
        while True:
            try:
                current, event, value = await anext(self.parser)
            except StopAsyncIteration:
                self.done = True
                raise

            if current == self.prefix:
                break

            self.remainder_builder.event(event, value)

        if event not in ("start_map", "start_array"):
            return self.streamed_item_cls.model_validate(value, context=self.validation_context)

        depth = 1
        obj = _ObjectBuilder()
        while depth:
            obj.event(event, value)
            try:
                current, event, value = await anext(self.parser)
            except StopAsyncIteration:
                raise ValueError("Incomplete JSON structure")

            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1

        return self.streamed_item_cls.model_validate(obj.get_value(), context=self.validation_context)

    def get_remainder(self) -> Remainder:
        if not self.done:
            raise Exception(
                "attempted to access remainder before fully processing input"
            )

        return self.remainder_cls.model_validate(self.remainder_builder.get_value())


class _AsyncStreamWrapper:
    def __init__(self, gen: AsyncGenerator[bytes, None]):
        self.gen = gen
        self.buf = b""
        self.had_data = False
        self.eof = False

    async def read(self, size: int = -1) -> bytes:
        if size == -1:
            data = self.buf + b"".join([chunk async for chunk in self.gen])
            self.eof = True
        else:
            while len(self.buf) < size:
                try:
                    self.buf += await anext(self.gen)
                except StopAsyncIteration:
                    self.eof = True
                    break

        data, self.buf = self.buf[:size], self.buf[size:]
        if not self.had_data:
            data = data.lstrip(b" \t\n\r")

        if self.eof and not data and not self.had_data:
            raise StopAsyncIteration

        if not self.had_data and data:
            self.had_data = True

        return data


class _ObjectBuilder:
    def __init__(self) -> None:
        self.value: Any = None
        self.key: str | None = None

        def initial_set(value: Any) -> None:
            self.value = value

        self.stack: List[Callable[[Any], None]] = [initial_set]

    def event(self, event: str, value: Any) -> None:
        match event:
            case "map_key":
                self.key = value
            case "start_map":
                mappable = dict()
                self.stack[-1](mappable)

                def setter(value: Any) -> None:
                    assert self.key is not None
                    mappable[self.key] = value

                self.stack.append(setter)
            case "start_array":
                array = []
                self.stack[-1](array)

                self.stack.append(array.append)
            case "end_array" | "end_map":
                self.stack.pop()
            case _:
                self.stack[-1](value)

    def get_value(self) -> Any:
        del self.stack[:]  # This seems to improve memory usage significantly.
        return self.value
