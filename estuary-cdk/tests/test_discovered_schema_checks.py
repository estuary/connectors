import pytest

from estuary_cdk.capture._discovered_schema_checks import check_schema_regexes
from estuary_cdk.capture.common import Resource, ResourceConfig, ResourceState, discovered

# The regex pydantic 2.12 and 2.13 add to a `Decimal` field's schema.
DECIMAL_REGEX = r"^(?!^[-+.]*$)[+-]?0*\d*\.?\d*$"


def test_a_pattern_fails():
    schema = {
        "type": "object",
        "properties": {"amount": {"type": "string", "pattern": DECIMAL_REGEX}},
    }

    with pytest.raises(RuntimeError) as exc:
        check_schema_regexes([("payments", schema)])

    message = str(exc.value)
    assert f"`{DECIMAL_REGEX}` at payments /properties/amount/pattern" in message
    assert "look-around, including look-ahead and look-behind, is not supported" in message


def test_a_pattern_properties_key_fails():
    schema = {
        "type": "object",
        "properties": {
            "rates": {"type": "object", "patternProperties": {DECIMAL_REGEX: {"type": "integer"}}},
        },
    }

    with pytest.raises(RuntimeError) as exc:
        check_schema_regexes([("exchange_rates", schema)])

    assert f"exchange_rates /properties/rates/patternProperties/{DECIMAL_REGEX}" in str(exc.value)


def test_a_nested_pattern_fails():
    # The shape pydantic gives a nested model with an optional `Decimal` field.
    schema = {
        "$defs": {
            "Line": {
                "type": "object",
                "properties": {
                    "amount": {
                        "anyOf": [{"type": "string", "pattern": DECIMAL_REGEX}, {"type": "null"}],
                    },
                },
            },
        },
        "type": "object",
        "properties": {
            "lines": {"type": "array", "items": {"$ref": "#/$defs/Line"}},
            "tags": {"type": "array", "items": {"type": "string", "pattern": r"(a)\1"}},
        },
    }

    with pytest.raises(RuntimeError) as exc:
        check_schema_regexes([("invoices", schema)])

    message = str(exc.value)
    assert "invoices /$defs/Line/properties/amount/anyOf/0/pattern" in message
    assert "invoices /properties/tags/items/pattern" in message


def test_valid_regexes_pass():
    schema = {
        "type": "object",
        "properties": {
            # These look like look-arounds, but their `(` is escaped or inside a
            # character class.
            "escaped": {"type": "string", "pattern": r"\(?=x"},
            "in_class": {"type": "string", "pattern": r"[(?!]"},
        },
        "patternProperties": {"^[a-z]+$": {"type": "string"}},
    }

    check_schema_regexes([("things", schema)])


def test_regexes_flow_never_compiles_pass():
    schema = {
        "type": "object",
        "properties": {
            # Flow doesn't build `default` or `x-` values as schemas, so it never
            # compiles these.
            "noted": {
                "type": "object",
                "default": {"pattern": "(?=a)"},
                "x-notes": {"pattern": "(?=a)"},
            },
            # Flow ignores `additionalItems` because `items` is a single schema.
            "extra_items": {
                "type": "array",
                "items": {"type": "string"},
                "additionalItems": {"type": "string", "pattern": "(?=a)"},
            },
        },
    }

    check_schema_regexes([("things", schema)])


def test_a_repeated_regex_is_reported_once():
    properties = {}
    for i in range(7):
        properties[f"amount_{i}"] = {"type": "string", "pattern": DECIMAL_REGEX}
    schema = {"type": "object", "properties": properties}

    with pytest.raises(RuntimeError) as exc:
        check_schema_regexes([("ledger", schema)])

    message = str(exc.value)
    assert message.count(f"`{DECIMAL_REGEX}`") == 1
    assert "ledger /properties/amount_4/pattern, and 2 more:" in message


def test_every_failing_regex_is_reported():
    payments = {
        "type": "object",
        "properties": {
            "amount": {"type": "string", "pattern": DECIMAL_REGEX},
            "ref": {"type": "string", "pattern": r"(a)\1"},
        },
    }
    refunds = {
        "type": "object",
        "properties": {
            "amount": {"type": "string", "pattern": DECIMAL_REGEX},
            "code": {"type": "string", "pattern": r"abc\Z"},
            "status": {"type": "string", "pattern": r"^[A-Z]+$"},
        },
    }

    with pytest.raises(RuntimeError) as exc:
        check_schema_regexes([("payments", payments), ("refunds", refunds)])

    message = str(exc.value)
    # The heading, then one line per regex that doesn't compile.
    assert message.startswith("Implementation error: ")
    assert len(message.splitlines()) == 4
    assert (
        f"`{DECIMAL_REGEX}` at payments /properties/amount/pattern, "
        "refunds /properties/amount/pattern:"
    ) in message
    assert r"`(a)\1` at payments /properties/ref/pattern:" in message
    assert r"`abc\Z` at refunds /properties/code/pattern:" in message
    assert "status" not in message


def test_discovery_checks_regexes():
    schema = {
        "type": "object",
        "properties": {"amount": {"type": "string", "pattern": DECIMAL_REGEX}},
    }
    resource = Resource(
        name="payments",
        key=["/id"],
        model=Resource.FixedSchema(schema),
        open=lambda *args: None,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="payments"),
        schema_inference=True,
    )

    with pytest.raises(RuntimeError, match="payments /properties/amount/pattern"):
        discovered([resource])
