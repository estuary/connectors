from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.capture.task import (
    DEFAULT_NUMERIC_BOUNDS,
    DEFAULT_STRING_BOUNDS,
    with_sourced_schema_defaults,
)

STRING_MIN = DEFAULT_STRING_BOUNDS["minLength"]
STRING_MAX = DEFAULT_STRING_BOUNDS["maxLength"]
NUMERIC_MIN = DEFAULT_NUMERIC_BOUNDS["minimum"]
NUMERIC_MAX = DEFAULT_NUMERIC_BOUNDS["maximum"]


def test_a_bare_schema_gets_every_default():
    out = with_sourced_schema_defaults({"properties": {"s": {"type": "string"}}})

    assert out["type"] == "object"
    assert out["additionalProperties"] is False
    assert out["properties"]["_meta"] == BaseDocument.Meta.sourced_schema()
    assert sorted(out["required"]) == ["_meta", "s"]
    assert out["properties"]["s"] == {
        "type": "string",
        "minLength": STRING_MIN,
        "maxLength": STRING_MAX,
    }


def test_the_root_type_is_forced_to_object():
    # A captured document is always an object, so unlike a nested location the
    # root's declared type is overridden rather than defaulted.
    out = with_sourced_schema_defaults({"type": ["object", "null"], "properties": {}})

    assert out["type"] == "object"
    assert out["additionalProperties"] is False


def test_numeric_and_boolean_locations():
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "i": {"type": "integer"},
                "n": {"type": "number"},
                "b": {"type": "boolean"},
            }
        }
    )

    assert out["properties"]["i"] == {
        "type": "integer",
        "minimum": NUMERIC_MIN,
        "maximum": NUMERIC_MAX,
    }
    assert out["properties"]["n"]["maximum"] == NUMERIC_MAX
    # Booleans have nothing to bound.
    assert out["properties"]["b"] == {"type": "boolean"}


def test_what_the_connector_declared_wins():
    own_meta = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"source_file": {"type": "string"}},
    }
    out = with_sourced_schema_defaults(
        {
            "type": "object",
            "properties": {
                "_meta": own_meta,
                "id": {"type": "string", "minLength": 15, "maxLength": 18},
                "lat": {"type": "number", "minimum": -90, "maximum": 90},
            },
        }
    )

    # A connector's own `_meta` field survives, alongside the standard subfields.
    assert sorted(out["properties"]["_meta"]["properties"]) == [
        "op",
        "row_id",
        "source_file",
        "uuid",
    ]
    assert out["properties"]["id"]["minLength"] == 15
    assert out["properties"]["id"]["maxLength"] == 18
    assert out["properties"]["lat"]["minimum"] == -90
    assert out["properties"]["lat"]["maximum"] == 90


def test_a_declared_meta_still_gets_the_runtime_owned_uuid():
    # `uuid` is the one `_meta` subfield a connector never controls: the runtime
    # stamps it onto every captured document. So a connector that declared `_meta`
    # for its own reasons must still come out asserting `uuid`.
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "id": {"type": "string"},
                "_meta": {
                    "type": "object",
                    "properties": {"row_id": {"type": "integer"}},
                    "required": ["row_id"],
                },
            }
        }
    )
    meta = out["properties"]["_meta"]
    default_uuid = BaseDocument.Meta.sourced_schema()["properties"]["uuid"]

    assert meta["additionalProperties"] is False
    assert "row_id" in meta["properties"]
    assert meta["properties"]["uuid"] == default_uuid
    assert "uuid" in meta["required"]


def test_a_declared_meta_gains_the_standard_subfields_alongside_its_own():
    # source-dynamics-365-finance-and-operations declares `_meta` to describe a
    # `source_file` field of its own. Merging is per subfield, so its declaration
    # wins for the subfield it names and the ones it leaves out are filled in.
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "_meta": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "source_file": {"type": "string", "maxLength": 1},
                    },
                    "required": ["source_file"],
                },
            }
        }
    )
    meta = out["properties"]["_meta"]
    default_meta = BaseDocument.Meta.sourced_schema()

    assert sorted(meta["properties"]) == ["op", "row_id", "source_file", "uuid"]
    assert sorted(meta["required"]) == ["op", "row_id", "source_file", "uuid"]

    # The subfield it declared wins wholesale, and is still completed in place.
    assert meta["properties"]["source_file"] == {
        "type": "string",
        "minLength": STRING_MIN,
        "maxLength": 1,
    }

    # The ones it didn't name come from the CDK untouched.
    assert meta["properties"]["op"] == default_meta["properties"]["op"]
    assert meta["properties"]["row_id"] == default_meta["properties"]["row_id"]
    assert meta["properties"]["uuid"] == default_meta["properties"]["uuid"]


def test_partially_declared_bounds_are_completed():
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "s": {"type": "string", "maxLength": 64},
                "i": {"type": "integer", "minimum": -1},
            }
        }
    )

    assert out["properties"]["s"] == {
        "type": "string",
        "minLength": STRING_MIN,
        "maxLength": 64,
    }
    assert out["properties"]["i"] == {
        "type": "integer",
        "minimum": -1,
        "maximum": NUMERIC_MAX,
    }


def test_union_typed_locations_get_every_applicable_bound():
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "nullable": {"type": ["string", "null"]},
                "any": {"type": ["string", "number", "boolean"]},
            }
        }
    )

    nullable = out["properties"]["nullable"]
    assert nullable["minLength"] == STRING_MIN and "minimum" not in nullable

    # Length bounds constrain only the string values and range bounds only the
    # numeric ones, so a union of both carries both.
    any_typed = out["properties"]["any"]
    assert any_typed["minLength"] == STRING_MIN
    assert any_typed["maximum"] == NUMERIC_MAX

    # A declared union is never narrowed to a bare object.
    assert nullable["type"] == ["string", "null"]


def test_every_nested_location_is_reached():
    out = with_sourced_schema_defaults(
        {
            "properties": {
                "nested": {"properties": {"deep": {"type": "string"}}},
                "list": {"type": "array", "items": {"type": "string"}},
                "either": {"oneOf": [{"type": "string"}, {"type": "integer"}]},
            },
            "patternProperties": {"^x-": {"type": "string"}},
        }
    )

    nested = out["properties"]["nested"]
    assert nested["type"] == "object"
    assert nested["additionalProperties"] is False
    assert nested["required"] == ["deep"]
    assert nested["properties"]["deep"]["maxLength"] == STRING_MAX

    assert out["properties"]["list"]["items"]["maxLength"] == STRING_MAX
    assert out["properties"]["either"]["oneOf"][0]["maxLength"] == STRING_MAX
    assert out["properties"]["either"]["oneOf"][1]["maximum"] == NUMERIC_MAX
    assert out["patternProperties"]["^x-"]["maxLength"] == STRING_MAX


def test_declared_required_entries_are_kept_and_extended():
    out = with_sourced_schema_defaults(
        {
            "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
            "required": ["b"],
        }
    )

    assert out["required"] == ["b", "a", "_meta"]


def test_input_is_not_mutated():
    # Connectors keep sourced schemas in module-level constants, so the input
    # must survive being emitted more than once.
    original = {
        "type": "object",
        "properties": {"nested": {"properties": {"s": {"type": "string"}}}},
    }
    with_sourced_schema_defaults(original)

    assert original == {
        "type": "object",
        "properties": {"nested": {"properties": {"s": {"type": "string"}}}},
    }
