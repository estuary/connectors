import base64
import math
from typing import Any

import pyarrow as pa
from estuary_cdk.flow import ValidationError
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
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
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


FALLBACK_KEY = ["/_meta/snapshot_id", "/_meta/file", "/_meta/row"]


def _format_date(days: int) -> str:
    # Integer calendar math: Python dates stop at years 1-9999, but Iceberg values can lie outside them.
    days += 719468
    era, day_of_era = divmod(days, 146097)
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    month_index = (5 * day_of_year + 2) // 153
    day = day_of_year - (153 * month_index + 2) // 5 + 1
    month = month_index + 3 if month_index < 10 else month_index - 9
    year = year_of_era + era * 400 + (month <= 2)
    # RFC 3339 has four-digit years only.
    if not 0 <= year <= 9999:
        raise ValueError(f"year {year} is outside the supported range 0000-9999")
    return f"{year:04d}-{month:02d}-{day:02d}"


def serialize_value(value: pa.Scalar, field_type: IcebergType) -> Any:
    if not value.is_valid:
        return None

    match field_type:
        case TimestampType() | TimestamptzType() | TimestampNanoType() | TimestamptzNanoType():
            assert isinstance(value, pa.TimestampScalar)
            precision = {"s": 0, "ms": 3, "us": 6, "ns": 9}[value.type.unit]
            seconds, fraction = divmod(value.value, 10**precision)
            days, seconds = divmod(seconds, 86400)
            minutes, seconds = divmod(seconds, 60)
            timestamp = f"{_format_date(days)}T{minutes // 60:02d}:{minutes % 60:02d}:{seconds:02d}"
            if fraction:
                timestamp += "." + f"{fraction:0{precision}d}".rstrip("0")
            return timestamp + "Z"
        case DateType():
            assert isinstance(value, pa.Date32Scalar)
            return _format_date(value.value)
        case TimeType():
            return value.as_py().isoformat()
        case DecimalType():
            # Fixed-point text; str() switches to exponent notation for small values.
            return format(value.as_py(), "f")
        case UUIDType():
            return str(value.as_py())
        case FloatType() | DoubleType():
            number = value.as_py()
            if math.isfinite(number):
                return number
            if math.isnan(number):
                return "NaN"
            return "Infinity" if number > 0 else "-Infinity"
        case BinaryType() | FixedType() | GeometryType() | GeographyType():
            return base64.b64encode(value.as_py()).decode("ascii")
        case StructType():
            return {
                field.name: serialize_value(value[field.name], field.field_type)
                for field in field_type.fields
            }
        case ListType():
            return [serialize_value(item, field_type.element_type) for item in value.values]
        case MapType():
            entries = [
                {
                    "key": serialize_value(entry["key"], field_type.key_type),
                    "value": serialize_value(entry["value"], field_type.value_type),
                }
                for entry in value.values
            ]
            if isinstance(field_type.key_type, StringType):
                return {entry["key"]: entry["value"] for entry in entries}
            return entries
        case _:
            return value.as_py()


def _nullable(schema: dict[str, Any], required: bool) -> dict[str, Any]:
    if not required and schema["type"] != "null":
        types = schema["type"]
        schema["type"] = [*types, "null"] if isinstance(types, list) else [types, "null"]
    return schema


def type_schema(field_type: IcebergType) -> dict[str, Any]:
    match field_type:
        case BooleanType():
            return {"type": "boolean"}
        case IntegerType() | LongType():
            return {"type": "integer"}
        case FloatType() | DoubleType():
            return {"type": ["number", "string"], "format": "number"}
        case DecimalType():
            return {"type": "string", "format": "number"}
        case DateType():
            return {"type": "string", "format": "date"}
        case TimeType():
            return {"type": "string"}
        case TimestampType() | TimestamptzType() | TimestampNanoType() | TimestamptzNanoType():
            return {"type": "string", "format": "date-time"}
        case StringType():
            return {"type": "string"}
        case UUIDType():
            return {"type": "string", "format": "uuid"}
        case BinaryType() | FixedType() | GeometryType() | GeographyType():
            return {"type": "string", "contentEncoding": "base64"}
        case UnknownType():
            return {"type": "null"}
        case StructType():
            properties = {}
            required = []
            for field in field_type.fields:
                field_schema = _nullable(type_schema(field.field_type), field.required)
                if field.doc:
                    field_schema["description"] = field.doc
                properties[field.name] = field_schema
                if field.required:
                    required.append(field.name)
            schema: dict[str, Any] = {"type": "object", "properties": properties}
            if required:
                schema["required"] = required
            return schema
        case ListType():
            return {
                "type": "array",
                "items": _nullable(type_schema(field_type.element_type), field_type.element_required),
            }
        case MapType():
            value_schema = _nullable(type_schema(field_type.value_type), field_type.value_required)
            if isinstance(field_type.key_type, StringType):
                return {"type": "object", "additionalProperties": value_schema}
            # Entry arrays preserve map keys that JSON object keys cannot represent.
            return {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"key": type_schema(field_type.key_type), "value": value_schema},
                    "required": ["key", "value"],
                },
            }
        case _:
            raise ValueError(f"Unsupported Iceberg type: {field_type}")


def collection_schema(schema: Schema) -> dict[str, Any]:
    if any(field.name == "_meta" for field in schema.fields):
        raise ValidationError(["Iceberg column '_meta' conflicts with reserved document metadata."])

    document_schema = type_schema(schema.as_struct())
    document_schema["properties"]["_meta"] = {
        "type": "object",
        "properties": {
            "snapshot_id": {"type": "integer"},
            "file": {"type": "string"},
            "row": {"type": "integer"},
        },
        "required": ["snapshot_id", "file", "row"],
    }
    document_schema.setdefault("required", []).append("_meta")
    return document_schema


def collection_key(schema: Schema) -> list[str]:
    if not schema.identifier_field_ids:
        return list(FALLBACK_KEY)

    paths: dict[int, str] = {}

    def visit(struct: StructType, parent: str):
        for field in struct.fields:
            path = parent + "/" + field.name.replace("~", "~0").replace("/", "~1")
            paths[field.field_id] = path
            if isinstance(field.field_type, StructType):
                visit(field.field_type, path)

    visit(schema.as_struct(), "")
    return [paths[field_id] for field_id in schema.identifier_field_ids]
