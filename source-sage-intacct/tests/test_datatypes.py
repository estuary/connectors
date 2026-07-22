from datetime import UTC, datetime
from typing import Optional

import pytest
from pydantic import create_model

from source_sage_intacct.sage import DATATYPE_MAP, SageRecord


def build_model(fields: list[tuple[str, str]]) -> type[SageRecord]:
    """Build a SageRecord subclass the same way `Sage._build_model` does, so we
    can exercise value normalization and schema generation for specific
    datatypes without talking to the Sage Intacct API."""
    base_fields = set(SageRecord.model_fields.keys())
    field_defs = {
        name: (Optional[DATATYPE_MAP[datatype]], None)
        for name, datatype in fields
        if name not in base_fields
    }
    model = create_model("TestObject", __base__=SageRecord, **field_defs)
    model.tz_dt = datetime.now(UTC)
    model.field_names = [name for name, _ in fields]
    model.field_datatypes = [datatype for _, datatype in fields]
    return model


def test_currency_value_normalizes_to_string():
    # Regression for estuary/connectors#4873: a populated CURRENCY field (e.g.
    # ITEM.BASIC = "6.50") previously raised
    # `Unhandled datatype CURRENCY for BASIC: 6.50` during runTransactions.
    model = build_model([("RECORDNO", "INTEGER"), ("BASIC", "CURRENCY")])

    record = model.model_validate({"RECORDNO": 1, "BASIC": "6.50"})

    assert record.model_dump()["BASIC"] == "6.50"


def test_percent_value_normalizes_to_string():
    model = build_model([("RECORDNO", "INTEGER"), ("RATE", "PERCENT")])

    record = model.model_validate({"RECORDNO": 1, "RATE": "12.5"})

    assert record.model_dump()["RATE"] == "12.5"


def test_sequence_still_raises_pending_confirmed_shape():
    # SEQUENCE is intentionally left unhandled until a real value is observed;
    # keep the explicit failure so we don't silently emit an unverified shape.
    model = build_model([("RECORDNO", "INTEGER"), ("SEQ", "SEQUENCE")])

    with pytest.raises(ValueError, match="Unhandled datatype SEQUENCE"):
        model.model_validate({"RECORDNO": 1, "SEQ": "7"})


def test_sourced_schema_emits_currency_and_percent_as_number_strings():
    model = build_model(
        [
            ("RECORDNO", "INTEGER"),
            ("BASIC", "CURRENCY"),
            ("RATE", "PERCENT"),
            ("SEQ", "SEQUENCE"),
        ]
    )

    schema = model.sourced_schema()

    assert schema["properties"]["BASIC"] == {"type": "string", "format": "number"}
    assert schema["properties"]["RATE"] == {"type": "string", "format": "number"}
    assert "BASIC" in schema["required"]
    assert "RATE" in schema["required"]
    # SEQUENCE shape is unconfirmed, so it is still dropped from the schema.
    assert "SEQ" not in schema["properties"]
    assert "SEQ" not in schema["required"]
