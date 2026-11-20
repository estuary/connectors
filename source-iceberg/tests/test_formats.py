import gc
import io
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from time import sleep
from types import SimpleNamespace
from uuid import UUID

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem
from pyiceberg.avro.file import AvroFileHeader, AvroOutputFile, MAGIC, META_SCHEMA
from pyiceberg.avro.resolver import SchemaPartnerAccessor, WriteSchemaResolver, construct_writer
from pyiceberg.io.pyarrow import ArrowAccessor, PyArrowFile, schema_to_pyarrow
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema, visit, visit_with_partner
from pyiceberg.table import DataScan, TableProperties
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType, DateType, DecimalType, FixedType, IntegerType, ListType, LongType,
    MapType, NestedField, StringType, StructType, TimeType, TimestampNanoType, TimestampType, TimestamptzNanoType,
    TimestamptzType, UUIDType,
)
from pyiceberg.utils.datetime import date_to_days, datetime_to_micros, time_to_micros
from pyiceberg.utils.schema_conversion import ConvertSchemaToAvro

from source_iceberg import api, files

from .test_capture import append, capture, fetch, fresh_emit_lock as fresh_emit_lock, table_factory as table_factory


class NullableAvroSchema(ConvertSchemaToAvro):
    def list(self, list_type, element_result):
        return super().list(list_type, element_result if list_type.element_required else ["null", element_result])

    def map(self, map_type, key_result, value_result):
        return super().map(map_type, key_result, value_result if map_type.value_required else ["null", value_result])


class DataAvroOutputFile(AvroOutputFile):
    def _write_header(self):
        # The manifest writer omits nullable list/map unions from its generated header.
        schema = visit(self.file_schema, NullableAvroSchema(self.schema_name))
        header = AvroFileHeader(MAGIC, {**self.metadata, "avro.schema": json.dumps(schema)}, self.sync_bytes)
        construct_writer(META_SCHEMA).write(self.encoder, header)


def orc_storage_field(field):
    metadata = {
        "iceberg.id": field.metadata[b"PARQUET:field_id"].decode(),
        "iceberg.required": str(not field.nullable).lower(),
    }
    field_type = field.type
    if pa.types.is_struct(field_type):
        field_type = pa.struct([orc_storage_field(child) for child in field_type])
    elif pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
        field_type = pa.list_(orc_storage_field(field_type.value_field))
    elif pa.types.is_map(field_type):
        field_type = pa.map_(orc_storage_field(field_type.key_field), orc_storage_field(field_type.item_field))
    elif pa.types.is_time64(field_type):
        field_type = pa.int64()
        metadata["iceberg.long-type"] = "TIME"
    elif field_type == pa.uuid():
        field_type = pa.binary()
        metadata["iceberg.binary-type"] = "UUID"
    elif pa.types.is_fixed_size_binary(field_type):
        metadata.update({"iceberg.binary-type": "FIXED", "iceberg.length": str(field_type.byte_width)})
        field_type = pa.binary()
    return pa.field(field.name, field_type, nullable=field.nullable, metadata=metadata)


def avro_record(value, field_type):
    if value is None:
        return None
    if isinstance(field_type, StructType):
        return Record(*(avro_record(value.get(field.name), field.field_type) for field in field_type.fields))
    if isinstance(field_type, ListType):
        return [avro_record(item, field_type.element_type) for item in value]
    if isinstance(field_type, MapType):
        return {key: avro_record(item, field_type.value_type) for key, item in value.items()}
    return value


def write_file(table, path, file_format, rows, codec="null", schema=None):
    schema = schema or table.schema()
    if file_format == FileFormat.ORC:
        arrow_schema = pa.schema([orc_storage_field(field) for field in schema_to_pyarrow(schema)])
        orc.write_table(pa.Table.from_pylist(rows, schema=arrow_schema), path, compression="zlib")
    elif file_format == FileFormat.PARQUET:
        pq.write_table(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(schema)), path)
    else:
        with DataAvroOutputFile(table.io.new_output(path.as_uri()), schema, "data", record_schema=schema, metadata={"avro.codec": codec}) as writer:
            # The resolver handles nullable collection values, unlike the writer's equal-schema shortcut.
            writer.writer = visit_with_partner(schema, schema, WriteSchemaResolver(), SchemaPartnerAccessor())
            for offset in range(0, len(rows), 500):
                writer.write_block([avro_record(row, schema.as_struct()) for row in rows[offset:offset + 500]])
    data_file = DataFile.from_args(
        content=DataFileContent.DATA, file_path=path.as_uri(), file_format=file_format,
        partition=Record(), spec_id=0, record_count=len(rows), file_size_in_bytes=path.stat().st_size,
    )
    transaction = table.transaction()
    with transaction.update_snapshot().fast_append() as update:
        update.append_data_file(data_file)
    table.metadata = transaction.table_metadata
    return data_file


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.ORC, FileFormat.AVRO])
async def test_capture_formats_and_resume(monkeypatch, table_factory, tmp_path, file_format):
    table = table_factory()
    data_file = write_file(table, tmp_path / "first", file_format, [
        {"id": index, "canary": f"row-{index}"} for index in range(2001)
    ])
    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [(doc["id"], doc["canary"], doc["_meta"]["row"]) for doc in docs] == [
        (index, f"row-{index}", index) for index in range(2001)
    ]
    assert all(doc["_meta"]["file"] == data_file.file_path for doc in docs)
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)

    write_file(table, tmp_path / "second", file_format, [{"id": 2001, "canary": "new"}])
    append(table, [{"id": 2002, "canary": "parquet"}])
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [(doc["id"], doc["canary"], doc["_meta"]["row"]) for doc in docs] == [
        (2001, "new", 0), (2002, "parquet", 0),
    ]
    records, _, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.ORC, FileFormat.AVRO])
async def test_formats_project_field_ids_after_evolution(table_factory, tmp_path, file_format):
    table = table_factory(schema=Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "detail", StructType(NestedField(3, "old", LongType()))),
        NestedField(4, "label", StringType()),
    ))
    write_file(table, tmp_path / "data", file_format, [{"id": 1, "detail": {"old": 9}, "label": "obsolete"}])
    transaction = table.transaction()
    with transaction.update_schema() as update:
        update.rename_column("id", "order_id")
        update.update_column("id", field_type=LongType())
        update.rename_column("detail.old", "current")
        update.delete_column("label")
    with transaction.update_schema() as update:
        update.add_column("label", StringType())
    table.metadata = transaction.table_metadata
    items = [item async for item in fetch()]
    assert items[0].model_extra == {"order_id": 1, "detail": {"current": 9}, "label": None}
    assert items[0].meta_.row == 0
    assert items[1] == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC])
@pytest.mark.parametrize("container", ["list", "map"])
async def test_nested_container_renames(monkeypatch, table_factory, tmp_path, file_format, container):
    # Guard field-ID projection below nested containers, including null and empty values.
    inner = ListType(5, StructType(NestedField(6, "old", LongType())), element_required=False)
    if container == "list":
        field_type = ListType(3, inner, element_required=False)
        path = "data.element.element.old"
        before = [[[{"old": 42}, None], None, []], None, [], [[{"old": 7}]]]
        after = [[[{"new": 42}, None], None, []], None, [], [[{"new": 7}]]]
    else:
        field_type = MapType(3, StringType(), 4, inner, value_required=False)
        path = "data.value.element.old"
        before = [{"k": [{"old": 42}, None], "missing": None, "empty": []}, None, {}, {"k": [{"old": 7}]}]
        after = [{"k": [{"new": 42}, None], "missing": None, "empty": []}, None, {}, {"k": [{"new": 7}]}]
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True), NestedField(2, "data", field_type),
    ))
    write_file(table, tmp_path / "data", file_format, [{"id": index, "data": value} for index, value in enumerate(before)])
    snapshot_id = table.current_snapshot().snapshot_id
    transaction = table.transaction()
    with transaction.update_schema() as update:
        update.rename_column(path, "new")
    table.metadata = transaction.table_metadata
    assert table.current_snapshot().snapshot_id == snapshot_id

    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [{key: value for key, value in doc.items() if key != "_meta"} for doc in docs] == [
        {"id": index, "data": value} for index, value in enumerate(after)
    ]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(snapshot_id),)


@pytest.mark.parametrize("container", ["list", "large_list", "map"])
def test_nested_container_renames_in_sliced_arrays(container):
    # Guard projected children and validity masks when container offsets start beyond zero.
    def schema(name):
        inner = ListType(5, StructType(NestedField(6, name, LongType())), element_required=False)
        outer = (
            MapType(3, StringType(), 4, inner, value_required=False)
            if container == "map" else ListType(3, inner, element_required=False)
        )
        return Schema(NestedField(2, "data", outer))

    file_schema = schema("old")
    inner = pa.list_(pa.struct([pa.field("old", pa.int64())]))
    if container == "map":
        arrow_type = pa.map_(pa.string(), inner)
        before = [
            {"skip": [{"old": -1}]}, None, {},
            {"k": [{"old": 42}, None], "missing": None, "empty": []}, {"skip": [{"old": -2}]},
        ]
        after = [None, [], [("k", [{"new": 42}, None]), ("missing", None), ("empty", [])]]
    else:
        arrow_type = (pa.large_list if container == "large_list" else pa.list_)(inner)
        before = [[[{"old": -1}]], None, [], [[{"old": 42}, None], None, []], [[{"old": -2}]]]
        after = [None, [], [[{"new": 42}, None], None, []]]
    array = pa.array(before, type=arrow_type).slice(1, 3)
    assert array.offset > 0 and array.offsets[0].as_py() > 0
    projected = visit_with_partner(
        schema("new"), pa.RecordBatch.from_arrays([array], names=["data"]),
        files.ArrowProjection(file_schema), ArrowAccessor(file_schema),
    ).field("data")
    assert projected.to_pylist() == after
    assert type(projected) is type(array)


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format, codec", [(FileFormat.ORC, "null"), (FileFormat.AVRO, "null"), (FileFormat.AVRO, "deflate"), (FileFormat.AVRO, "snappy")])
async def test_format_logical_types(table_factory, tmp_path, file_format, codec):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "at", TimestampType()),
        NestedField(3, "amount", DecimalType(20, 4)),
        NestedField(4, "payload", BinaryType()),
        NestedField(5, "fixed", FixedType(3)),
        NestedField(6, "uuid", UUIDType()),
        NestedField(7, "date", DateType()),
        NestedField(8, "time", TimeType()),
        NestedField(9, "detail", StructType(NestedField(10, "times", ListType(11, TimestamptzType(), element_required=False)))),
        NestedField(12, "amounts", MapType(13, StringType(), 14, DecimalType(20, 4), value_required=False)),
        NestedField(15, "payloads", MapType(16, LongType(), 17, BinaryType(), value_required=False)),
    ))
    write_file(table, tmp_path / "data", file_format, [{
        "id": 1, "at": datetime_to_micros(datetime(2026, 1, 2, 3, 4, 5, 120000, tzinfo=UTC)),
        "amount": Decimal("1234567890123456.7800"), "payload": b"\x00\xff", "fixed": b"abc",
        "uuid": UUID("12345678-1234-5678-1234-567812345678").bytes,
        "date": date_to_days(date(2026, 1, 2)), "time": time_to_micros(time(3, 4, 5, 123456)),
        "detail": {"times": [datetime_to_micros(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)), None]},
        "amounts": {"first": Decimal("1.2300"), "missing": None}, "payloads": {1: b"\x00\xff", 2: None},
    }, {"id": 2}], codec)
    items = [item async for item in fetch()]
    assert len(items) == 3
    assert items[0].model_extra == {
        "id": 1, "at": "2026-01-02T03:04:05.12Z", "amount": "1234567890123456.7800",
        "payload": "AP8=", "fixed": "YWJj", "uuid": "12345678-1234-5678-1234-567812345678",
        "date": "2026-01-02", "time": "03:04:05.123456", "detail": {"times": ["2026-01-02T03:04:05Z", None]},
        "amounts": {"first": "1.2300", "missing": None},
        "payloads": [{"key": 1, "value": "AP8="}, {"key": 2, "value": None}],
    }
    assert items[1].model_extra == {field.name: 2 if field.name == "id" else None for field in table.schema().fields}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", [
        "logical_types", "evolution", "timestamps_v2", "timestamps_v3", "timestamps_v3_ns", "null_containers",
        "nested_list_rename", "nested_map_rename",
    ],
)
async def test_orc_projection_with_batch_reader(monkeypatch, table_factory, tmp_path, case):
    written = []
    write_table = orc.write_table

    def record_table(table, *args, **kwargs):
        written.append(table)
        return write_table(table, *args, **kwargs)

    class Fragment:
        def __init__(self, source):
            self.physical_schema = orc.ORCFile(source).schema

        def to_batches(self, *, columns, batch_size, batch_readahead, fragment_readahead, use_threads):
            assert batch_size == 1000
            assert batch_readahead == fragment_readahead == 0
            assert use_threads is False
            return written[0].cast(self.physical_schema).select(columns).to_batches(max_chunksize=batch_size)

    # Keep the actual ORC footer while substituting decoded batches for the native scanner.
    monkeypatch.setattr(orc, "write_table", record_table)
    monkeypatch.setattr(api.ds, "OrcFileFormat", lambda: SimpleNamespace(make_fragment=Fragment))
    if case == "logical_types":
        await test_format_logical_types(table_factory, tmp_path, FileFormat.ORC, "null")
    elif case == "evolution":
        await test_formats_project_field_ids_after_evolution(table_factory, tmp_path, FileFormat.ORC)
    elif case == "null_containers":
        await test_projection_preserves_null_containers(monkeypatch, table_factory, tmp_path, FileFormat.ORC)
    elif case in ("nested_list_rename", "nested_map_rename"):
        await test_nested_container_renames(monkeypatch, table_factory, tmp_path, FileFormat.ORC, case.split("_")[1])
    else:
        test_orc_timestamps_by_format_version(table_factory, tmp_path, int(case[12]), case.endswith("_ns"))


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.ORC, FileFormat.AVRO])
@pytest.mark.parametrize("delete_format", [FileFormat.PARQUET, FileFormat.AVRO])
async def test_format_position_deletes(monkeypatch, table_factory, tmp_path, file_format, delete_format):
    table = table_factory()
    data_file = write_file(table, tmp_path / "data", file_format, [{"id": index} for index in range(2001)])
    tasks = list(table.scan().plan_files())
    path = tmp_path / f"deletes.{delete_format.value.lower()}"
    rows = [{"file_path": data_file.file_path, "pos": pos} for pos in (0, 1000, 2000)]
    rows.append({"file_path": "s3://warehouse/another-file", "pos": 1})
    if delete_format == FileFormat.AVRO:
        schema = Schema(
            NestedField(2147483546, "file_path", StringType(), required=True),
            NestedField(2147483545, "pos", LongType(), required=True),
        )
        with DataAvroOutputFile(table.io.new_output(path.as_uri()), schema, "deletes", metadata={"avro.codec": "null"}) as writer:
            writer.write_block([avro_record(row, schema.as_struct()) for row in rows])
    else:
        pq.write_table(pa.Table.from_pylist(rows), path)
    tasks[0].delete_files.add(DataFile.from_args(
        content=DataFileContent.POSITION_DELETES, file_path=path.as_uri(), file_format=delete_format,
        partition=Record(), record_count=len(rows), file_size_in_bytes=path.stat().st_size,
    ))
    monkeypatch.setattr(DataScan, "plan_files", lambda self: tasks)
    items = [item async for item in fetch()]
    assert [(item.model_extra["id"], item.meta_.row) for item in items[:-1]] == [
        (index, index) for index in range(2001) if index not in (0, 1000, 2000)
    ]
    assert items[-1] == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.ORC, FileFormat.AVRO])
async def test_format_read_failure_resumes_unfinished_file(monkeypatch, table_factory, tmp_path, file_format):
    table = table_factory()
    first = write_file(table, tmp_path / "a", file_format, [{"id": 1}])
    second = write_file(table, tmp_path / "b", file_format, [{"id": 2}])
    read_batches = api._file_batches

    def fail_second(table, task, schema):
        yield from read_batches(table, task, schema)
        if task.file.file_path == second.file_path:
            raise OSError("made-up read failure")

    monkeypatch.setattr(api, "_file_batches", fail_second)
    records, state, error = await capture(monkeypatch, [table])
    assert isinstance(error, OSError)
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [1]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (f"{table.current_snapshot().snapshot_id}:1:full",)

    def resume_second(table, task, schema):
        assert task.file.file_path != first.file_path
        yield from read_batches(table, task, schema)

    monkeypatch.setattr(api, "_file_batches", resume_second)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [2]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.parametrize("file_format", [FileFormat.ORC, FileFormat.AVRO])
def test_format_batches_bound_memory(table_factory, tmp_path, file_format):
    table = table_factory()
    row_count = 100_001
    write_file(table, tmp_path / "data", file_format, [
        {"id": index, "canary": "x" * 400} for index in range(row_count)
    ])
    task = next(iter(table.scan().plan_files()))
    gc.collect()
    baseline = pa.total_allocated_bytes()
    batches = api._file_batches(table, task, table.schema())
    count = 0
    try:
        for batch in batches:
            if count == 0:
                sleep(0.1)
            assert pa.total_allocated_bytes() - baseline < 8_000_000
            assert 0 < batch.num_rows <= 1000
            assert batch.column("id").to_pylist() == list(range(count, count + batch.num_rows))
            count += batch.num_rows
        assert count == row_count
    finally:
        batches.close()


@pytest.mark.asyncio
async def test_truncated_avro_does_not_checkpoint(monkeypatch, table_factory, tmp_path):
    table = table_factory()
    path = tmp_path / "data.avro"
    write_file(table, path, FileFormat.AVRO, [{"id": index} for index in range(1001)])
    contents = path.read_bytes()
    path.write_bytes(contents[:-30])
    records, state, error = await capture(monkeypatch, [table])
    assert isinstance(error, (ValueError, EOFError))
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("",)


def test_avro_streams_and_closes_file(monkeypatch, table_factory, tmp_path):
    table = table_factory()
    path = tmp_path / "data.avro"
    write_file(table, path, FileFormat.AVRO, [{"id": index, "canary": "x" * 200} for index in range(2001)])
    task = next(iter(table.scan().plan_files()))
    source = path.open("rb")

    class Stream:
        def read(self, size=-1):
            assert size >= 0, "must not buffer a whole Avro data file"
            return source.read(size)

        def __getattr__(self, name):
            return getattr(source, name)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            source.close()

    monkeypatch.setattr(table.io, "new_input", lambda location: SimpleNamespace(open=lambda seekable=True: Stream()))
    batches = api._file_batches(table, task, table.schema())
    try:
        assert next(batches).num_rows == 1000
        assert source.tell() < path.stat().st_size
    finally:
        batches.close()
    assert source.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("name_mapping", [True, False])
async def test_avro_without_field_ids(monkeypatch, table_factory, tmp_path, name_mapping):
    convert_schema = NullableAvroSchema.schema

    def strip_ids(value):
        if isinstance(value, dict):
            return {key: strip_ids(item) for key, item in value.items() if key not in ("field-id", "element-id", "key-id", "value-id")}
        if isinstance(value, list):
            return [strip_ids(item) for item in value]
        return value

    monkeypatch.setattr(NullableAvroSchema, "schema", lambda self, schema, result: strip_ids(convert_schema(self, schema, result)))
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "detail", StructType(NestedField(3, "old", StringType()))),
        NestedField(4, "items", ListType(5, StructType(NestedField(6, "value", LongType())))),
        NestedField(7, "attrs", MapType(8, StringType(), 9, StructType(NestedField(10, "value", LongType())))),
        NestedField(11, "numbers", MapType(12, LongType(), 13, StringType())),
    ))
    path = tmp_path / "imported.avro"
    row = {"id": 1, "detail": {"old": "value"}, "items": [{"value": 2}], "attrs": {"key": {"value": 3}}, "numbers": {4: "four"}}
    write_file(table, path, FileFormat.AVRO, [row])
    if name_mapping:
        transaction = table.transaction()
        transaction.set_properties({TableProperties.DEFAULT_NAME_MAPPING: table.schema().name_mapping.model_dump_json()})
        with transaction.update_schema() as update:
            update.rename_column("detail.old", "current")
        table.metadata = transaction.table_metadata
    records, state, error = await capture(monkeypatch, [table])
    if name_mapping:
        assert error is None
        docs = [record["captured"]["doc"] for record in records if "captured" in record]
        assert len(docs) == 1
        assert {key: value for key, value in docs[0].items() if key != "_meta"} == {
            **row, "detail": {"current": "value"}, "numbers": [{"key": 4, "value": "four"}],
        }
        assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)
    else:
        assert isinstance(error, ValueError)
        assert path.as_uri() in str(error)
        assert "schema.name-mapping.default" in str(error)
        assert not any("captured" in record for record in records)
        assert state.bindingStateV1["shop/orders"].inc.cursor == ("",)


def test_avro_buffers_backend_reads(table_factory, tmp_path):
    table = table_factory()
    path = tmp_path / "data.avro"
    write_file(table, path, FileFormat.AVRO, [{"id": index} for index in range(20_000)])
    contents = path.read_bytes()
    reads = []

    class Stream(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    class FileSystem(pafs.FSSpecHandler):
        def open_input_file(self, path):
            return pa.PythonFile(Stream(contents), mode="r")

        def open_input_stream(self, path):
            return self.open_input_file(path)

    input_file = PyArrowFile(path.as_uri(), str(path), pafs.PyFileSystem(FileSystem(MemoryFileSystem())))
    with files.open_avro(input_file) as reader:
        assert [row[0] for row in reader] == list(range(20_000))
    assert len(reads) < 10
    assert 1 not in reads


@pytest.mark.parametrize("format_version, nanos", [(2, False), (3, False), (3, True)])
def test_orc_timestamps_by_format_version(table_factory, tmp_path, format_version, nanos):
    fields = [
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "at", TimestampType()),
        NestedField(3, "utc", TimestamptzType()),
        NestedField(4, "nested", StructType(NestedField(5, "at", TimestampType()))),
    ]
    # PyIceberg rejects nanosecond types in the v2 tables it can create.
    table = table_factory(schema=Schema(*fields))
    at = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
    row = {"id": 1, "at": datetime_to_micros(at), "utc": datetime_to_micros(at), "nested": {"at": datetime_to_micros(at)}}
    schema = table.schema()
    if nanos:
        fields += [NestedField(6, "at_ns", TimestampNanoType()), NestedField(7, "utc_ns", TimestamptzNanoType())]
        schema = Schema(*fields)
        row |= {"at_ns": datetime_to_micros(at) * 1000 + 789, "utc_ns": datetime_to_micros(at) * 1000 + 789}
    write_file(table, tmp_path / "data.orc", FileFormat.ORC, [row], schema=schema)
    task = next(iter(table.scan().plan_files()))
    # PyIceberg cannot write v3 manifests yet.
    table.metadata = table.metadata.model_copy(update={"format_version": format_version, "schemas": [schema]})
    result = pa.Table.from_batches(list(api._file_batches(table, task, table.schema())))
    assert result.select(["id", "at", "utc", "nested"]).to_pylist() == [{
        "id": 1, "at": at.replace(tzinfo=None), "utc": at, "nested": {"at": at.replace(tzinfo=None)},
    }]
    if nanos:
        assert result.schema.field("at_ns").type == pa.timestamp("ns")
        assert result.schema.field("utc_ns").type == pa.timestamp("ns", "UTC")
        assert result.select(["at_ns", "utc_ns"]).cast(pa.schema([
            pa.field("at_ns", pa.int64()), pa.field("utc_ns", pa.int64()),
        ])).to_pylist() == [{"at_ns": row["at_ns"], "utc_ns": row["utc_ns"]}]


def test_orc_timestamps_outside_nanosecond_range_fail_clearly(table_factory, tmp_path):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "valid_to", TimestampType()),
    ))
    rows = [
        {"id": 1, "valid_to": datetime(9999, 12, 31, tzinfo=UTC).replace(tzinfo=None)},
        {"id": 2, "valid_to": datetime(2020, 1, 1, tzinfo=UTC).replace(tzinfo=None)},
    ]
    data_file = write_file(table, tmp_path / "data.orc", FileFormat.ORC, rows)
    task = next(iter(table.scan().plan_files()))
    with pytest.raises(RuntimeError, match="ORC timestamp values before 1677-09-21 or after 2262-04-11") as raised:
        list(api._file_batches(table, task, table.schema()))
    assert data_file.file_path in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO])
async def test_projection_preserves_null_containers(monkeypatch, table_factory, tmp_path, file_format):
    def fields(start):
        return (
            NestedField(start, "items", ListType(start + 1, StructType(NestedField(start + 2, "value", LongType())), element_required=False)),
            NestedField(start + 3, "attrs", MapType(start + 4, StringType(), start + 5, StructType(NestedField(start + 6, "value", LongType())), value_required=False)),
            NestedField(start + 7, "tags", ListType(start + 8, StringType(), element_required=False)),
        )

    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True), *fields(2),
        NestedField(11, "nested", StructType(*fields(12))),
    ))
    containers = [
        {"items": None, "attrs": None, "tags": None},
        {"items": [], "attrs": {}, "tags": []},
        {"items": [{"value": 9}, None], "attrs": {"key": {"value": 8}, "missing": None}, "tags": ["tag", None]},
    ]
    rows = [{"id": index, **value, "nested": value} for index, value in enumerate(containers * 334)]
    rows.append({"id": len(rows), **containers[0], "nested": None})
    write_file(table, tmp_path / "data", file_format, rows)
    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [{key: value for key, value in doc.items() if key != "_meta"} for doc in docs] == rows
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)
