import json
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import islice
from typing import Any

import pyarrow as pa
from pyiceberg.avro.decoder import StreamingBinaryDecoder
from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import MapReader, Reader
from pyiceberg.avro.resolver import ReadSchemaResolver, SchemaPartnerAccessor
from pyiceberg.io import InputFile
from pyiceberg.io.pyarrow import ArrowProjectionVisitor, schema_to_pyarrow
from pyiceberg.schema import visit_with_partner
from pyiceberg.table.name_mapping import NameMapping, apply_name_mapping
from pyiceberg.typedef import Record
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType, UUIDType
from pyiceberg.utils.schema_conversion import AvroSchemaConversion


class ArrowProjection(ArrowProjectionVisitor):
    # Rebuild from projected children at every nesting level, preserving container validity.
    def list(self, list_type: ListType, list_array: pa.Array | None, value_array: pa.Array | None) -> pa.Array | None:
        if list_array is None or value_array is None:
            return None
        if isinstance(list_array, (pa.ListArray, pa.LargeListArray)):
            values = self._cast_if_needed(list_type.element_field, value_array)
            list_initializer = pa.large_list if isinstance(list_array, pa.LargeListArray) else pa.list_
            arrow_field = list_initializer(self._construct_field(list_type.element_field, values.type))
            # Arrow requires unsliced offsets when supplying a validity mask.
            return pa.LargeListArray.from_arrays(
                pa.concat_arrays([list_array.offsets]), values, mask=list_array.is_null(),
            ).cast(arrow_field)
        return super().list(list_type, list_array, value_array)

    def map(
        self, map_type: MapType, map_array: pa.Array | None, key_result: pa.Array | None, value_result: pa.Array | None
    ) -> pa.Array | None:
        if map_array is None or key_result is None or value_result is None:
            return None
        if isinstance(map_array, pa.MapArray):
            keys = self._cast_if_needed(map_type.key_field, key_result)
            values = self._cast_if_needed(map_type.value_field, value_result)
            arrow_field = pa.map_(
                self._construct_field(map_type.key_field, keys.type),
                self._construct_field(map_type.value_field, values.type),
            )
            return pa.MapArray.from_arrays(
                pa.concat_arrays([map_array.offsets]), keys, values, arrow_field, mask=map_array.is_null(),
            )
        return super().map(map_type, map_array, key_result, value_result)


def orc_field(field: pa.Field) -> pa.Field:
    metadata = dict(field.metadata or {})
    if field_id := metadata.get(b"iceberg.id"):
        metadata[b"PARQUET:field_id"] = field_id
    field_type = field.type
    if pa.types.is_struct(field_type):
        field_type = pa.struct([orc_field(child) for child in field_type])
    elif pa.types.is_list(field_type):
        field_type = pa.list_(orc_field(field_type.value_field))
    elif pa.types.is_map(field_type):
        field_type = pa.map_(orc_field(field_type.key_field), orc_field(field_type.item_field))
    elif metadata.get(b"iceberg.long-type") == b"TIME":
        field_type = pa.time64("us")
    elif metadata.get(b"iceberg.binary-type") == b"UUID":
        field_type = pa.uuid()
    elif metadata.get(b"iceberg.binary-type") == b"FIXED":
        field_type = pa.binary(int(metadata[b"iceberg.length"]))
    nullable = metadata[b"iceberg.required"] != b"true" if b"iceberg.required" in metadata else field.nullable
    return pa.field(field.name, field_type, nullable=nullable, metadata=metadata)


class _AvroReader(ReadSchemaResolver):
    def map(
        self, map_type: MapType, expected_map: IcebergType | None, key_reader: Reader, value_reader: Reader
    ) -> MapReader:
        reader = super().map(map_type, expected_map, key_reader, value_reader)
        # PyIceberg's integer-key fast path omits callbacks for optional values.
        reader._key_reader = reader.key.read
        reader._value_reader = reader.value.read
        return reader


class _AvroSchemaConversion(AvroSchemaConversion):
    # Allow missing IDs until the converted schema can use Iceberg's name mapping.
    def _convert_field(self, field: dict[str, Any]) -> NestedField:
        field.setdefault("field-id", -1)
        return super()._convert_field(field)

    def _convert_array_type(self, array_type: dict[str, Any]) -> ListType:
        array_type.setdefault("element-id", -1)
        return super()._convert_array_type(array_type)

    def _convert_map_type(self, map_type: dict[str, Any]) -> MapType:
        map_type.setdefault("key-id", -1)
        map_type.setdefault("value-id", -1)
        return super()._convert_map_type(map_type)


@contextmanager
def open_avro(input_file: InputFile, name_mapping: NameMapping | None = None) -> Iterator[AvroFile[Record]]:
    # PyIceberg buffers whole Avro manifests; data files need a streaming decoder.
    with input_file.open(seekable=False) as source:
        reader = AvroFile(input_file)
        reader.decoder = StreamingBinaryDecoder(source)
        reader.header = reader._read_header()
        if "avro.schema" not in reader.header.meta:
            raise ValueError(f"No schema found in Avro file headers: {input_file.location}")
        reader.schema = _AvroSchemaConversion().avro_to_iceberg(json.loads(reader.header.meta["avro.schema"]))
        if -1 in reader.schema.field_ids:
            if name_mapping is None:
                raise ValueError(
                    f"Avro file {input_file.location} has no field IDs; "
                    "the table requires schema.name-mapping.default to read it"
                )
            reader.schema = apply_name_mapping(reader.schema, name_mapping)
        reader.reader = visit_with_partner(reader.schema, reader.schema, _AvroReader({}, {}), SchemaPartnerAccessor())
        yield reader


def _arrow_value(value: Any, field_type: IcebergType) -> Any:
    if value is None:
        return None
    match field_type:
        case StructType():
            return {
                field.name: _arrow_value(value[index], field.field_type)
                for index, field in enumerate(field_type.fields)
            }
        case ListType():
            return [_arrow_value(item, field_type.element_type) for item in value]
        case MapType():
            return [
                (_arrow_value(key, field_type.key_type), _arrow_value(item, field_type.value_type))
                for key, item in value.items()
            ]
        case UUIDType():
            return value.bytes
        case _:
            return value


def avro_batches(reader: AvroFile[Record], record_count: int) -> Iterator[pa.RecordBatch]:
    arrow_schema = schema_to_pyarrow(reader.schema)
    count = 0
    while rows := list(islice(reader, 1000)):
        count += len(rows)
        yield pa.RecordBatch.from_pylist(
            [_arrow_value(row, reader.schema.as_struct()) for row in rows], schema=arrow_schema,
        )
    if count != record_count:
        raise ValueError(f"Avro data file contains {count} rows, expected {record_count}")
