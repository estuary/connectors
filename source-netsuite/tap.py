import typing as t
from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    PropertiesList,
    Property,
    StringType,
)

from .streams import (
    FooStream,
)

PLUGIN_NAME = "tap-netsuite"

STREAM_TYPES = [
    FooStream,
]


class TapNetSuite(Tap):
    """NetSuite tap class."""

    name = "tap-netsuite"
    config_jsonschema = PropertiesList(
        Property(
            "secret",
            StringType,
            required=True,
            secret=True,
            description="Example secret",
        ),
        Property(
            "not-secret",
            StringType,
            required=True,
            description="Example non-secret",
        ),
    ).to_dict()

    def discover_streams(self) -> t.List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


cli = TapNetSuite.cli
