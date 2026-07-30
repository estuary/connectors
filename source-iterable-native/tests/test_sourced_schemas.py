"""Checks on the sourced schema each stream declares.

Only what this connector decides is checked here. Everything uniform across
connectors — the `_meta` block, closure, `required`, and default bounds — is filled
in by `with_sourced_schema_defaults()` and covered by
`estuary-cdk/tests/test_sourced_schema_defaults.py`. The schema under test is
composed through that same function, so these assertions hold against what the
runtime actually receives.
"""

from typing import Any

import pytest
from estuary_cdk.capture.task import with_sourced_schema_defaults

from source_iterable_native.models import (
    CampaignMetrics,
    Campaigns,
    Channels,
    Events,
    ListUsers,
    Lists,
    MessageTypes,
    MetadataTables,
    Templates,
    UsersWithEmails,
    UsersWithIds,
    build_sourced_schema,
)


# Every stream paired with the collection key declared for it in resources.py.
# The pointers are duplicated here on purpose: a stream whose key fields aren't
# named in its sourced schema can have those fields squashed out of the inferred
# schema, silently dropping the key columns from downstream materializations.
STREAM_KEYS: list[tuple[type, list[str]]] = [
    (Channels, ["/_meta/row_id"]),
    (MessageTypes, ["/_meta/row_id"]),
    (MetadataTables, ["/_meta/row_id"]),
    (Templates, ["/_meta/row_id"]),
    (Lists, ["/_meta/row_id"]),
    (ListUsers, ["/list_id", "/user_id"]),
    (UsersWithEmails, ["/email"]),
    (UsersWithIds, ["/itblUserId"]),
    (Events, ["/_estuary_id", "/eventType"]),
    (Campaigns, ["/id"]),
    (CampaignMetrics, ["/id"]),
]

STREAM_IDS = [stream.__name__ for stream, _ in STREAM_KEYS]


def sourced_schema(stream: type) -> dict[str, Any]:
    """The schema the runtime receives for a stream, as resources.py emits it."""
    return with_sourced_schema_defaults(build_sourced_schema(stream.KEY_PROPERTIES))


def resolve_pointer(schema: dict[str, Any], pointer: str) -> dict[str, Any] | None:
    """Resolve a JSON pointer through a schema's `properties`, or None if absent."""
    node = schema
    for token in pointer.lstrip("/").split("/"):
        properties = node.get("properties", {})
        if token not in properties:
            return None
        node = properties[token]
    return node


@pytest.mark.parametrize("stream,keys", STREAM_KEYS, ids=STREAM_IDS)
def test_names_every_key_field(stream: type, keys: list[str]):
    schema = sourced_schema(stream)

    for pointer in keys:
        node = resolve_pointer(schema, pointer)
        assert node is not None, f"{stream.__name__} key {pointer} is not named"
        assert "type" in node, f"{stream.__name__} key {pointer} declares no type"

    for pointer in keys:
        root_property = pointer.lstrip("/").split("/")[0]
        assert root_property in schema["required"], (
            f"{stream.__name__} does not require {root_property}"
        )
