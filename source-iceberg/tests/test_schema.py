import json
from decimal import Decimal

import pyarrow as pa
import pytest
from estuary_cdk.capture import response
from estuary_cdk.capture.task import Response, serialize_response
from estuary_cdk.flow import ValidationError
from jsonschema import Draft202012Validator
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimeType,
    TimestampNanoType,
    TimestampType,
    TimestamptzNanoType,
    TimestamptzType,
    UUIDType,
    UnknownType,
)

from source_iceberg.models import Document
from source_iceberg.schema import FALLBACK_KEY, collection_key, collection_schema, serialize_value, type_schema


@pytest.mark.parametrize(("iceberg_type", "arrow_type", "value", "expected"), [
    (TimestampType(), pa.timestamp("us"), 0, "1970-01-01T00:00:00Z"),
    (TimestamptzType(), pa.timestamp("us", "UTC"), 1_234_560, "1970-01-01T00:00:01.23456Z"),
    (TimestamptzType(), pa.timestamp("us", "+02:00"), 0, "1970-01-01T00:00:00Z"),
    (TimestampType(), pa.timestamp("us"), -1, "1969-12-31T23:59:59.999999Z"),
    (TimestampNanoType(), pa.timestamp("ns"), 1_234_567_891, "1970-01-01T00:00:01.234567891Z"),
    (TimestamptzNanoType(), pa.timestamp("ns", "UTC"), -1, "1969-12-31T23:59:59.999999999Z"),
    (TimestamptzNanoType(), pa.timestamp("ns", "UTC"), 1_000_000_000, "1970-01-01T00:00:01Z"),
    (TimestamptzType(), pa.timestamp("us", "UTC"), None, None),
])
def test_timestamp_serialization(iceberg_type, arrow_type, value, expected):
    assert serialize_value(pa.scalar(value, type=arrow_type), iceberg_type) == expected


@pytest.mark.parametrize(("iceberg_type", "arrow_type", "value", "expected"), [
    (TimestamptzType(), pa.timestamp("us", "UTC"), -62_167_219_200_000_000, "0000-01-01T00:00:00Z"),
    (TimestampType(), pa.timestamp("us"), -62_135_596_800_000_001, "0000-12-31T23:59:59.999999Z"),
    (TimestamptzType(), pa.timestamp("us", "UTC"), 253_402_300_799_999_999, "9999-12-31T23:59:59.999999Z"),
    (DateType(), pa.date32(), -719_528, "0000-01-01"),
    (DateType(), pa.date32(), -719_162, "0001-01-01"),
    (DateType(), pa.date32(), 2_932_896, "9999-12-31"),
    (DateType(), pa.date32(), 20_454, "2026-01-01"),
])
def test_values_outside_python_datetime_range(iceberg_type, arrow_type, value, expected):
    assert serialize_value(pa.scalar(value, type=arrow_type), iceberg_type) == expected


@pytest.mark.parametrize(("iceberg_type", "arrow_type", "value"), [
    (TimestamptzType(), pa.timestamp("us", "UTC"), 253_402_300_800_000_000),
    (TimestampType(), pa.timestamp("us"), -62_167_219_200_000_001),
    (DateType(), pa.date32(), 2_932_897),
    (DateType(), pa.date32(), -719_529),
])
def test_years_outside_rfc3339_raise_value_error(iceberg_type, arrow_type, value):
    with pytest.raises(ValueError, match="outside the supported range 0000-9999"):
        serialize_value(pa.scalar(value, type=arrow_type), iceberg_type)


@pytest.mark.parametrize(("value", "expected"), [
    ("0", "0.0000000000"),
    ("0.0000001", "0.0000001000"),
    ("-0.00000005", "-0.0000000500"),
    ("12.5", "12.5000000000"),
])
def test_decimal_serialization_is_fixed_point(value, expected):
    scalar = pa.scalar(Decimal(value), type=pa.decimal128(38, 10))
    assert serialize_value(scalar, DecimalType(38, 10)) == expected


@pytest.mark.parametrize(("iceberg_type", "arrow_type"), [
    (FloatType(), pa.float32()), (DoubleType(), pa.float64()),
])
@pytest.mark.parametrize("required", [False, True])
def test_non_finite_floats_through_capture_serialization(iceberg_type, arrow_type, required):
    schema = collection_schema(Schema(NestedField(1, "value", iceberg_type, required=required)))
    values = [(float("nan"), "NaN"), (float("inf"), "Infinity"), (float("-inf"), "-Infinity"), (1.5, 1.5)]
    if not required:
        values.append((None, None))

    for value, expected in values:
        document = Document.model_validate({
            "value": serialize_value(pa.scalar(value, type=arrow_type), iceberg_type),
            "_meta": {"snapshot_id": 1, "file": "file.parquet", "row": 0},
        })
        emitted = json.loads(serialize_response(Response(captured=response.Captured(binding=0, doc=document))))
        doc = emitted["captured"]["doc"]
        assert doc["value"] == expected
        Draft202012Validator(schema).validate(doc)

    assert schema["properties"]["value"] == {
        "type": ["number", "string"] + ([] if required else ["null"]),
        "format": "number",
    }


@pytest.mark.parametrize(("iceberg_type", "expected"), [
    (BooleanType(), {"type": "boolean"}),
    (IntegerType(), {"type": "integer"}),
    (LongType(), {"type": "integer"}),
    (FloatType(), {"type": ["number", "string"], "format": "number"}),
    (DoubleType(), {"type": ["number", "string"], "format": "number"}),
    (DecimalType(20, 4), {"type": "string", "format": "number"}),
    (DateType(), {"type": "string", "format": "date"}),
    (TimeType(), {"type": "string"}),
    (TimestampType(), {"type": "string", "format": "date-time"}),
    (TimestamptzType(), {"type": "string", "format": "date-time"}),
    (TimestampNanoType(), {"type": "string", "format": "date-time"}),
    (TimestamptzNanoType(), {"type": "string", "format": "date-time"}),
    (StringType(), {"type": "string"}),
    (UUIDType(), {"type": "string", "format": "uuid"}),
    (BinaryType(), {"type": "string", "contentEncoding": "base64"}),
    (FixedType(16), {"type": "string", "contentEncoding": "base64"}),
    (GeometryType(), {"type": "string", "contentEncoding": "base64"}),
    (GeographyType(), {"type": "string", "contentEncoding": "base64"}),
    (UnknownType(), {"type": "null"}),
])
def test_primitive_types(iceberg_type, expected):
    assert type_schema(iceberg_type) == expected


def test_nested_schema():
    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "detail", StructType(
            NestedField(3, "amount", DecimalType(20, 4), required=True, doc="Order amount."),
            NestedField(4, "tags", ListType(5, StringType(), element_required=False)),
        )),
        NestedField(6, "attributes", MapType(7, StringType(), 8, LongType(), value_required=False)),
        NestedField(9, "counts", MapType(10, LongType(), 11, StringType(), value_required=True)),
    )
    schema = collection_schema(iceberg_schema)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    meta = {"snapshot_id": 42, "file": "s3://warehouse/orders/file.parquet", "row": 0}
    doc = {
        "id": 1,
        "detail": {"amount": "12.3400", "tags": ["tag", None]},
        "attributes": {"a": 1, "b": None},
        "counts": [{"key": 3, "value": "three"}],
        "_meta": meta,
    }
    validator.validate(doc)
    validator.validate({"id": 1, "detail": None, "_meta": meta})
    assert schema["properties"]["detail"]["properties"]["amount"]["description"] == "Order amount."
    assert not validator.is_valid({**doc, "id": None})
    assert not validator.is_valid({**doc, "detail": {"amount": 12.34}})
    assert not validator.is_valid({**doc, "counts": [{"key": 3, "value": None}]})
    for field in meta:
        assert not validator.is_valid({**doc, "_meta": {k: v for k, v in meta.items() if k != field}})
    assert not validator.is_valid({"id": 1})


def test_identifier_keys_preserve_nesting_and_escape_pointers():
    schema = Schema(
        NestedField(1, "tenant/id", StringType(), required=True),
        NestedField(2, "detail.with.dots", StructType(
            NestedField(3, "~key/id", LongType(), required=True),
        ), required=True),
        identifier_field_ids=[3, 1],
    )
    assert collection_key(schema) == ["/detail.with.dots/~0key~1id", "/tenant~1id"]


def test_fallback_key():
    assert collection_key(Schema(NestedField(1, "id", LongType()))) == FALLBACK_KEY


def test_reserved_metadata_column():
    with pytest.raises(ValidationError, match="conflicts with reserved document metadata"):
        collection_schema(Schema(NestedField(1, "_meta", StringType())))
