from typing import Iterable, List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    PropertiesList,
    Property,
    StringType,
)

class TapNetSuiteStream(Stream):
    @property
    def partitions(self) -> List[dict]:
        return self.config.get(self.name, [])


class FooStream(TapNetSuiteStream):
    name = "foo"

    schema = PropertiesList(
        Property("my_key", StringType, required=True),
        Property("value", StringType),
    ).to_dict()
    primary_keys = ["my_key"]

    def __init__(self, tap: Tap):
        super().__init__(tap=tap, name=None, schema=None)

    def get_records(self, partition: dict) -> Iterable[dict]:
        yield {"my_key": "one", "value": "hello", "other": True}
        yield {"my_key": "one", "value": "world", "other": 42}
