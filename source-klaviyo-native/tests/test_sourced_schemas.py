import json
from pathlib import Path

from source_klaviyo_native.sourced_schemas import SOURCED_SCHEMAS

DISCOVER_SNAPSHOT = Path(__file__).parent / "snapshots" / "snapshots__discover__capture.stdout.json"


def test_every_discovered_stream_has_a_sourced_schema():
    # A binding without a schema fails at open.
    discovered = {b["recommendedName"] for b in json.loads(DISCOVER_SNAPSHOT.read_text())}

    assert discovered == set(SOURCED_SCHEMAS)
