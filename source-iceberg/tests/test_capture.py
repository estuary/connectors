import asyncio
import gc
import io
import json
import logging
import runpy
import traceback
from contextlib import nullcontext
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import sleep
from unittest.mock import MagicMock
from uuid import UUID

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest
from estuary_cdk.capture import Task, _emit, request
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec, RangeSpec
from estuary_cdk.logger import FlowLogger, LogFormatter
from estuary_cdk.utils import json_merge_patch
from fsspec.implementations.memory import MemoryFileSystem
from jsonschema import Draft202012Validator
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import ForbiddenError, NoSuchTableError, ServerError, ServiceUnavailableError
from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO, schema_to_pyarrow
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import DataScan, Table, TableProperties
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update.snapshot import ExpireSnapshots, ManageSnapshots
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    FixedType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimeType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from source_iceberg import Connector, api, resources
from source_iceberg.api import IcebergCatalog, _table_batches, fetch_table
from source_iceberg.models import ConnectorState, Document, EndpointConfig, ResourceConfig
from source_iceberg.schema import FALLBACK_KEY, collection_key, collection_schema

from .conftest import CATALOG_URI


LOG = logging.getLogger(__name__)
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "canary", StringType()),
    NestedField(3, "at", TimestamptzType()),
)


@pytest.fixture(autouse=True)
def fresh_emit_lock(monkeypatch):
    # Each test has its own event loop; the CDK's stdout lock is process-wide.
    monkeypatch.setattr(_emit, "_emit_lock", asyncio.Lock())


@pytest.fixture
def catalog_retry_delays(monkeypatch):
    delays = []
    monkeypatch.setattr(IcebergCatalog.load_table.retry, "sleep", delays.append)
    return delays


@pytest.fixture
def table_factory(tmp_path, catalog_api):
    tables = {}
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables",
        json=lambda request, context: {"identifiers": [
            {"namespace": ["shop"], "name": name} for name in tables
        ]},
    )

    def create(name="orders", schema=SCHEMA, *, location=None, file_io=None, config=None):
        location = location or (tmp_path / name).as_uri()
        table = Table(
            identifier=("shop", name),
            metadata=new_table_metadata(
                schema, UNPARTITIONED_PARTITION_SPEC, UNSORTED_SORT_ORDER, location
            ),
            metadata_location=location + "/metadata.json",
            io=file_io or PyArrowFileIO(),
            catalog=NoopCatalog("test"),
        )
        tables[name] = table
        catalog_api.get(
            f"{CATALOG_URI}/v1/namespaces/shop/tables/{name}",
            json=lambda request, context: {
                "metadata-location": table.metadata_location,
                "metadata": table.metadata.model_dump(mode="json", by_alias=True),
                "config": config or {},
            },
        )
        return table

    return create


def append(table, rows):
    transaction = table.transaction()
    transaction.append(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(table.schema())))
    table.metadata = transaction.table_metadata


def fetch(cursor=("",)):
    return fetch_table({"uri": CATALOG_URI}, {}, ("shop", "orders"), LOG, cursor)


async def capture(monkeypatch, tables, state=None, config=None):
    output = io.BytesIO()
    stopping = Task.Stopping()
    polls = Counter()
    state_json = state.model_dump_json() if state else "{}"

    async def counted_fetch(properties, file_io_properties, identifier, log, cursor):
        async for item in fetch_table(properties, file_io_properties, identifier, log, cursor):
            yield item
        polls[identifier[-1]] += 1
        if all(polls[table.name()[-1]] >= 3 for table in tables):
            stopping.event.set()

    monkeypatch.setattr(resources, "fetch_table", counted_fetch)
    bindings = [
        CaptureBinding[ResourceConfig](
            collection=CollectionSpec(
                name=f"test/{table.name()[-1]}",
                key=collection_key(table.schema()),
                writeSchema=collection_schema(table.schema()),
            ),
            resourceConfig=ResourceConfig(
                namespace="shop", table=table.name()[-1], interval=timedelta(milliseconds=10)
            ),
            resourcePath=["shop", table.name()[-1]],
            stateKey=f"shop/{table.name()[-1]}",
        )
        for table in tables
    ]
    opened, run = await Connector().open(
        LOG,
        request.Open[EndpointConfig, ResourceConfig, ConnectorState](
            capture=CaptureSpec[EndpointConfig, ResourceConfig](
                name="test/capture", connectorType="IMAGE", intervalSeconds=0,
                config=config or EndpointConfig(catalog_uri=CATALOG_URI, namespaces=["shop"]),
                bindings=bindings,
            ),
            version="test", range=RangeSpec(), state=ConnectorState.model_validate_json(state_json),
        ),
    )
    assert opened.explicitAcknowledgements is False
    async with asyncio.timeout(10), asyncio.TaskGroup() as group:
        task = Task(
            log=LOG, connector_status=MagicMock(), name="test", output=output,
            stopping=stopping, tg=group, transactor=Transactor(output),
            catalog_task_name="test/capture",
        )
        await run(task)

    records = [json.loads(line) for line in output.getvalue().splitlines()]
    merged = json.loads(state_json)
    for record in records:
        if "checkpoint" in record:
            checkpoint = record["checkpoint"]["state"]
            assert checkpoint["mergePatch"] is True
            json_merge_patch(merged, checkpoint["updated"])
    return records, ConnectorState.model_validate(merged), stopping.first_error


@pytest.mark.asyncio
async def test_backfill_through_capture_protocol(monkeypatch, table_factory):
    orders = table_factory()
    append(orders, [
        {"id": 1, "canary": "o1", "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)},
        {"id": 2, "canary": "o2"},
        {"id": 3, "canary": "o3"},
    ])
    customers = table_factory("customers", Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "canary", StringType()),
    ))
    append(customers, [{"id": 10, "canary": "c10"}])
    tables = [orders, customers]

    # Match the harness's collections, which only declare the fallback metadata.
    schema = {
        "type": "object",
        "required": ["_meta"],
        "properties": {"_meta": {
            "type": "object",
            "required": ["snapshot_id", "file", "row"],
            "properties": {
                "snapshot_id": {"type": "integer"},
                "file": {"type": "string"},
                "row": {"type": "integer"},
            },
        }},
    }
    open_request = {"open": {
        "capture": {
            "name": "acmeCo/source-iceberg/capture",
            "connectorType": "IMAGE",
            "intervalSeconds": 0,
            "config": {"catalog_uri": CATALOG_URI, "namespaces": ["shop"]},
            "bindings": [{
                "resourceConfig": {"namespace": "shop", "table": name, "interval": "PT5S"},
                "resourcePath": ["shop", name],
                "stateKey": f"shop/{name}",
                "collection": {
                    "name": f"acmeCo/source-iceberg/{name}",
                    "key": FALLBACK_KEY,
                    "writeSchema": schema,
                },
            } for name in ("orders", "customers")],
        },
        "version": "test",
        "range": {"keyBegin": 0, "keyEnd": 4294967295},
        "state": {},
    }}
    connector = Connector()
    connector.output = io.BytesIO()
    stopping = Task.Stopping()
    monkeypatch.setattr(Task, "Stopping", lambda: stopping)
    records = []
    state = {}
    completed = set()

    async def requests(request_class):
        yield request_class.model_validate_json(json.dumps(open_request))
        offset = 0
        while len(completed) < len(tables):
            data = connector.output.getvalue()
            end = data.rfind(b"\n") + 1
            for line in data[offset:end].splitlines():
                record = json.loads(line)
                records.append(record)
                if "checkpoint" in record:
                    checkpoint = record["checkpoint"]["state"]
                    assert checkpoint["mergePatch"] is True
                    json_merge_patch(state, checkpoint["updated"])
                    for table in tables:
                        key = f"shop/{table.name()[-1]}"
                        if state["bindingStateV1"].get(key, {}).get("inc", {}).get("cursor") == [
                            str(table.current_snapshot().snapshot_id)
                        ]:
                            completed.add(key)
            offset = end
            await asyncio.sleep(0.01)
        stopping.event.set()

    async with asyncio.timeout(10):
        await connector.serve(log=FlowLogger(__name__), requests=requests)

    assert records[0] == {"opened": {"explicitAcknowledgements": False}}
    assert all(set(record) <= {"opened", "captured", "checkpoint"} for record in records)
    captured = [record["captured"] for record in records if "captured" in record]
    assert len(captured) == 4
    assert {(item["binding"], item["doc"]["id"], item["doc"]["canary"]) for item in captured} == {
        (0, 1, "o1"), (0, 2, "o2"), (0, 3, "o3"), (1, 10, "c10"),
    }
    assert next(item["doc"]["at"] for item in captured if item["doc"]["id"] == 1) == "2026-01-02T03:04:05Z"
    for item in captured:
        Draft202012Validator(schema).validate(item["doc"])
        table = tables[item["binding"]]
        meta = item["doc"]["_meta"]
        assert meta["snapshot_id"] == table.current_snapshot().snapshot_id
        assert meta["file"] in {task.file.file_path for task in table.scan().plan_files()}
        assert meta["row"] == (item["doc"]["id"] - 1 if item["binding"] == 0 else 0)
    assert completed == {"shop/orders", "shop/customers"}

    # Resume a fresh connector from the actual JSON checkpoints, as the harness does.
    open_request["open"]["state"] = json.loads(json.dumps(state))
    append(orders, [{"id": 4, "canary": "o4"}, {"id": 5, "canary": "o5"}])
    connector = Connector()
    connector.output = io.BytesIO()
    stopping = type(stopping)()
    records = []
    completed = set()
    async with asyncio.timeout(10):
        await connector.serve(log=FlowLogger(__name__), requests=requests)

    records = [json.loads(line) for line in connector.output.getvalue().splitlines()]
    assert records[0] == {"opened": {"explicitAcknowledgements": False}}
    captured = [record["captured"] for record in records if "captured" in record]
    assert len(captured) == 2
    assert [(item["binding"], item["doc"]["id"], item["doc"]["canary"]) for item in captured] == [
        (0, 4, "o4"), (0, 5, "o5"),
    ]
    for index, item in enumerate(captured):
        Draft202012Validator(schema).validate(item["doc"])
        meta = item["doc"]["_meta"]
        assert meta["snapshot_id"] == orders.current_snapshot().snapshot_id
        assert meta["file"] in {
            task.file.file_path for task in orders.incremental_append_scan(
                from_snapshot_id_exclusive=orders.current_snapshot().parent_snapshot_id,
            ).plan_files()
        }
        assert meta["row"] == index
    assert state["bindingStateV1"]["shop/orders"]["inc"]["cursor"] == [
        str(orders.current_snapshot().snapshot_id),
    ]
    assert state["bindingStateV1"]["shop/customers"] == open_request["open"]["state"]["bindingStateV1"]["shop/customers"]
    assert completed == {"shop/orders", "shop/customers"}


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_backfill_retries_catalog_server_errors(
    monkeypatch, table_factory, catalog_api, catalog_retry_delays, caplog, status_code
):
    orders = table_factory()
    append(orders, [
        {"id": 1, "canary": "o1", "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)},
        {"id": 2, "canary": "o2"},
        {"id": 3, "canary": "o3"},
    ])
    customers = table_factory("customers")
    append(customers, [{"id": 10, "canary": "c10"}])
    loaded = {"json": {
        "metadata-location": orders.metadata_location,
        "metadata": orders.metadata.model_dump(mode="json", by_alias=True),
    }}
    failed = {"status_code": status_code, "json": {"error": {
        "message": "Failed to get table shop.orders from catalog rest_backend",
        "type": "UncheckedSQLException",
        "code": status_code,
    }}}
    # Schema discovery succeeds before the capture's table load fails twice.
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables/orders",
        [loaded, failed, failed, loaded],
    )

    records, state, error = await capture(monkeypatch, [orders, customers])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert len(docs) == 4
    assert {(doc["id"], doc["canary"]) for doc in docs} == {
        (1, "o1"), (2, "o2"), (3, "o3"), (10, "c10"),
    }
    assert next(doc["at"] for doc in docs if doc["id"] == 1) == "2026-01-02T03:04:05Z"
    for table in [orders, customers]:
        assert state.bindingStateV1[f"shop/{table.name()[-1]}"].inc.cursor == (
            str(table.current_snapshot().snapshot_id),
        )
    assert catalog_retry_delays == [1, 2]
    assert "UncheckedSQLException" in caplog.text
    assert all(call.method == "GET" for call in catalog_api.request_history)

    resumed, _, error = await capture(monkeypatch, [orders, customers], state)
    assert error is None
    assert not any("captured" in record for record in resumed)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code, exception, attempts", [
    (500, ServerError, 5),
    (503, ServiceUnavailableError, 5),
    (403, ForbiddenError, 1),
    (404, NoSuchTableError, 1),
])
async def test_catalog_failure_does_not_checkpoint_snapshot(
    monkeypatch, table_factory, catalog_api, catalog_retry_delays, status_code, exception, attempts
):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    loaded = {"json": {
        "metadata-location": table.metadata_location,
        "metadata": table.metadata.model_dump(mode="json", by_alias=True),
    }}
    failed = {"status_code": status_code, "json": {"error": {
        "message": "made-up catalog failure",
        "type": "CatalogException",
        "code": status_code,
    }}}
    endpoint = f"{CATALOG_URI}/v1/namespaces/shop/tables/orders"
    table_load = catalog_api.get(endpoint, [loaded, failed])

    records, state, error = await capture(monkeypatch, [table])
    assert isinstance(error, exception)
    assert "made-up catalog failure" in str(error)
    assert table_load.call_count == 1 + attempts
    assert catalog_retry_delays == ([1, 2, 4, 8] if attempts == 5 else [])
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("",)

    catalog_api.get(endpoint, [loaded])
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [1]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code, exception", [(500, ServerError), (503, ServiceUnavailableError)])
async def test_incremental_catalog_failure_preserves_server_stack(
    monkeypatch, table_factory, catalog_api, catalog_retry_delays, caplog, status_code, exception
):
    orders = table_factory()
    append(orders, [{"id": number, "canary": f"o{number}"} for number in (1, 2, 3)])
    customers = table_factory("customers")
    append(customers, [{"id": 10, "canary": "c10"}])
    _, state, error = await capture(monkeypatch, [orders, customers])
    assert error is None
    original_cursor = state.bindingStateV1["shop/orders"].inc.cursor

    append(orders, [{"id": 4, "canary": "o4"}, {"id": 5, "canary": "o5"}])
    loaded = {"json": {
        "metadata-location": orders.metadata_location,
        "metadata": orders.metadata.model_dump(mode="json", by_alias=True),
    }}
    stack = [
        "org.apache.iceberg.jdbc.UncheckedSQLException: made-up catalog failure",
        "Caused by: java.sql.SQLException: made-up SQL cause",
    ]
    endpoint = f"{CATALOG_URI}/v1/namespaces/shop/tables/orders"
    table_load = catalog_api.get(endpoint, [loaded, {
        "status_code": status_code,
        "json": {"error": {
            "message": "Failed to get table shop.orders from catalog rest_backend",
            "type": "UncheckedSQLException", "code": status_code, "stack": stack,
        }},
    }])

    records, state, error = await capture(monkeypatch, [orders, customers], state)
    assert isinstance(error, exception)
    assert table_load.call_count == 6
    assert catalog_retry_delays == [1, 2, 4, 8]
    assert "UncheckedSQLException" in str(error)
    assert "\n".join(stack) in "".join(traceback.format_exception(error))
    # Previews can stop before retries are exhausted, so every warning needs the cause.
    retry_logs = [
        LogFormatter().format(record) for record in caplog.records
        if record.name == "source_iceberg.api" and record.levelno == logging.WARNING
    ]
    assert len(retry_logs) == 4
    assert all(all(line in entry for line in stack) for entry in retry_logs)
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == original_cursor

    catalog_api.get(endpoint, [loaded])
    records, state, error = await capture(monkeypatch, [orders, customers], state)
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [(doc["id"], doc["canary"]) for doc in docs] == [(4, "o4"), (5, "o5")]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(orders.current_snapshot().snapshot_id),)
    assert state.bindingStateV1["shop/customers"].inc.cursor == (str(customers.current_snapshot().snapshot_id),)


@pytest.mark.parametrize("stack", [None, [], "made-up invalid stack"])
def test_catalog_error_with_unusable_stack_keeps_original_failure(catalog_api, catalog_retry_delays, stack):
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables/orders",
        status_code=500,
        json={"error": {
            "message": "made-up catalog failure",
            "type": "UncheckedSQLException", "code": 500, "stack": stack,
        }},
    )
    with (
        IcebergCatalog("test", {}, uri=CATALOG_URI) as catalog,
        pytest.raises(ServerError, match="UncheckedSQLException: made-up catalog failure") as caught,
    ):
        catalog.load_table(("shop", "orders"))
    assert catalog_retry_delays == [1, 2, 4, 8]
    assert caught.value.__cause__.response.status_code == 500
    assert "Iceberg REST catalog server stack" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.asyncio
async def test_backfill_through_open_and_checkpoint(monkeypatch, table_factory):
    orders = table_factory()
    append(orders, [
        {"id": 1, "canary": "o1", "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)},
        {"id": 2, "canary": "o2"},
    ])
    append(orders, [{"id": 3, "canary": "o3"}])
    customers = table_factory("customers", Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "canary", StringType()),
        identifier_field_ids=[1],
    ))
    append(customers, [{"id": 10, "canary": "c10"}])

    records, state, error = await capture(monkeypatch, [orders, customers])
    assert error is None
    captured = [record["captured"] for record in records if "captured" in record]
    assert len(captured) == 4
    docs = {item["doc"]["id"]: item["doc"] for item in captured}
    assert {key: doc["canary"] for key, doc in docs.items()} == {
        1: "o1", 2: "o2", 3: "o3", 10: "c10"
    }
    assert docs[1]["at"] == "2026-01-02T03:04:05Z"
    assert docs[1]["_meta"]["row"] == 0
    assert docs[2]["_meta"]["row"] == 1
    assert docs[3]["_meta"]["row"] == 0
    assert docs[1]["_meta"]["file"] == docs[2]["_meta"]["file"]
    assert docs[1]["_meta"]["file"] != docs[3]["_meta"]["file"]
    for item in captured:
        table = [orders, customers][item["binding"]]
        Draft202012Validator(collection_schema(table.schema())).validate(item["doc"])
        assert item["doc"]["_meta"]["snapshot_id"] == table.current_snapshot().snapshot_id
        assert item["doc"]["_meta"]["file"] in {
            task.file.file_path for task in table.scan().plan_files()
        }
    for table in [orders, customers]:
        assert state.bindingStateV1[f"shop/{table.name()[-1]}"].inc.cursor == (
            str(table.current_snapshot().snapshot_id),
        )

    resumed, _, error = await capture(monkeypatch, [orders, customers], state)
    assert error is None
    assert not any("captured" in record for record in resumed)


@pytest.mark.asyncio
async def test_backfill_reads_s3_files_with_endpoint_overrides(monkeypatch, table_factory, catalog_api, tmp_path):
    # Replace the S3 transport while reading real Iceberg manifests and Parquet files.
    storage = pafs.PyFileSystem(pafs.FSSpecHandler(MemoryFileSystem()))
    clients = []

    def s3_filesystem(**kwargs):
        clients.append(kwargs)
        return storage

    monkeypatch.setattr(pafs, "S3FileSystem", s3_filesystem)
    catalog_api.get(
        f"{CATALOG_URI}/v1/config",
        json={"defaults": {}, "overrides": {
            "s3.endpoint": "http://catalog-storage.test",
            "s3.region": "catalog-region",
            "s3.access-key-id": "made-up-catalog-key",
            "s3.secret-access-key": "made-up-catalog-secret",
            "s3.force-virtual-addressing": "true",
        }},
    )
    tables = []
    for name, rows in [
        ("orders", [
            {"id": 1, "canary": "o1", "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)},
            {"id": 2, "canary": "o2"},
            {"id": 3, "canary": "o3"},
        ]),
        ("customers", [{"id": 10, "canary": "c10"}]),
    ]:
        table = table_factory(
            name,
            location=f"s3://warehouse/{tmp_path.name}/shop/{name}",
            file_io=PyArrowFileIO({"s3.region": "us-east-1"}),
            config={
                "s3.endpoint": "http://table-storage.test",
                "s3.region": "table-region",
                "s3.access-key-id": "made-up-table-key",
                "s3.secret-access-key": "made-up-table-secret",
                "s3.force-virtual-addressing": "true",
                "s3.request-timeout": "30",
            },
        )
        append(table, rows)
        tables.append(table)
    clients.clear()

    records, state, error = await capture(monkeypatch, tables, config=EndpointConfig(
        catalog_uri=CATALOG_URI,
        namespaces=["shop"],
        s3={
            "endpoint": "http://storage.test:9000",
            "access_key_id": "made-up-key",
            "secret_access_key": "made-up-secret",
            "path_style_access": True,
        },
    ))
    assert error is None
    captured = [record["captured"] for record in records if "captured" in record]
    assert len(captured) == 4
    docs = {item["doc"]["id"]: item["doc"] for item in captured}
    assert {key: doc["canary"] for key, doc in docs.items()} == {
        1: "o1", 2: "o2", 3: "o3", 10: "c10"
    }
    assert docs[1]["at"] == "2026-01-02T03:04:05Z"
    for item in captured:
        table = tables[item["binding"]]
        meta = item["doc"]["_meta"]
        assert meta["snapshot_id"] == table.current_snapshot().snapshot_id
        assert meta["file"] in {task.file.file_path for task in table.scan().plan_files()}
        assert meta["file"].startswith(f"{table.location()}/data/")
        assert meta["row"] == (item["doc"]["id"] - 1 if item["binding"] == 0 else 0)
    for table in tables:
        assert state.bindingStateV1[f"shop/{table.name()[-1]}"].inc.cursor == (
            str(table.current_snapshot().snapshot_id),
        )
    assert len(clients) >= 2
    for client in clients:
        assert client == {
            "endpoint_override": "http://storage.test:9000",
            "region": "us-east-1",
            "access_key": "made-up-key",
            "secret_key": "made-up-secret",
            "session_token": None,
            "force_virtual_addressing": False,
            "request_timeout": 30.0,
        }


@pytest.mark.asyncio
async def test_backfill_serializes_iceberg_values(monkeypatch, table_factory):
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "at", TimestampType()),
        NestedField(3, "amount", DecimalType(20, 4)),
        NestedField(4, "payload", BinaryType()),
        NestedField(5, "fixed", FixedType(3)),
        NestedField(6, "uuid", UUIDType()),
        NestedField(7, "date", DateType()),
        NestedField(8, "time", TimeType()),
        NestedField(9, "detail", StructType(
            NestedField(10, "times", ListType(11, TimestamptzType(), element_required=False)),
        )),
        NestedField(12, "amounts", MapType(13, StringType(), 14, DecimalType(20, 4), value_required=False)),
        NestedField(15, "payloads", MapType(16, LongType(), 17, BinaryType(), value_required=False)),
    )
    table = table_factory(schema=schema)
    append(table, [
        {
            "id": 1,
            "at": datetime(2026, 1, 2, 3, 4, 5, 120000, tzinfo=UTC).replace(tzinfo=None),
            "amount": Decimal("1234567890123456.7800"),
            "payload": b"\x00\xff",
            "fixed": b"abc",
            "uuid": UUID("12345678-1234-5678-1234-567812345678").bytes,
            "date": date(2026, 1, 2),
            "time": time(3, 4, 5, 123456),
            "detail": {"times": [
                datetime(2026, 1, 2, 5, 4, 5, tzinfo=timezone(timedelta(hours=2))), None,
            ]},
            "amounts": [("first", Decimal("1.2300")), ("missing", None)],
            "payloads": [(1, b"\x00\xff"), (2, None)],
        },
        {"id": 2},
    ])

    records, _, error = await capture(monkeypatch, [table])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert len(docs) == 2
    assert {key: value for key, value in docs[0].items() if key != "_meta"} == {
        "id": 1,
        "at": "2026-01-02T03:04:05.12Z",
        "amount": "1234567890123456.7800",
        "payload": "AP8=",
        "fixed": "YWJj",
        "uuid": "12345678-1234-5678-1234-567812345678",
        "date": "2026-01-02",
        "time": "03:04:05.123456",
        "detail": {"times": ["2026-01-02T03:04:05Z", None]},
        "amounts": {"first": "1.2300", "missing": None},
        "payloads": [{"key": 1, "value": "AP8="}, {"key": 2, "value": None}],
    }
    assert {key: value for key, value in docs[1].items() if key != "_meta"} == {
        field.name: 2 if field.name == "id" else None for field in schema.fields
    }
    for doc in docs:
        Draft202012Validator(collection_schema(schema)).validate(doc)


@pytest.mark.asyncio
@pytest.mark.parametrize("new_snapshot", [False, True])
async def test_backfill_projects_older_files_to_discovered_schema(monkeypatch, table_factory, new_snapshot):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    snapshot_id = table.current_snapshot().snapshot_id

    transaction = table.transaction()
    with transaction.update_schema() as update:
        update.rename_column("id", "order_id")
        update.add_column("amount", DecimalType(20, 4))
    table.metadata = transaction.table_metadata
    assert table.current_snapshot().snapshot_id == snapshot_id
    if new_snapshot:
        append(table, [{"order_id": 2, "canary": "o2", "amount": Decimal("12.3400")}])

    records, _, error = await capture(monkeypatch, [table])
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert len(docs) == (2 if new_snapshot else 1)
    for doc in docs:
        Draft202012Validator(collection_schema(table.schema())).validate(doc)
        assert "id" not in doc
    by_id = {doc["order_id"]: doc for doc in docs}
    assert by_id[1]["canary"] == "o1"
    assert by_id[1]["amount"] is None
    if new_snapshot:
        assert by_id[2]["amount"] == "12.3400"


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC])
async def test_backfill_projects_imported_partitioned_file(monkeypatch, table_factory, tmp_path, file_format):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "canary", StringType()),
        NestedField(3, "at", TimestampType()),
        NestedField(4, "obsolete", StringType()),
    ))
    path = tmp_path / f"imported.{file_format.value.lower()}"
    writer = pq if file_format == FileFormat.PARQUET else orc
    writer.write_table(pa.table({
        "id": [1, 2],
        "at": pa.array([1_700_000_000_123_456_789, None], type=pa.timestamp("ns", tz="UTC")),
        "obsolete": ["old", "old"],
    }), path)
    transaction = table.transaction()
    transaction.set_properties({
        TableProperties.DEFAULT_NAME_MAPPING: table.schema().name_mapping.model_dump_json(),
    })
    with transaction.update_spec() as update:
        update.add_identity("canary")
    with transaction.update_snapshot().fast_append() as update:
        update.append_data_file(DataFile.from_args(
            content=DataFileContent.DATA, file_path=path.as_uri(), file_format=file_format,
            partition=Record("partition value"), spec_id=transaction.table_metadata.default_spec_id,
            record_count=2, file_size_in_bytes=path.stat().st_size,
        ))
    with transaction.update_schema() as update:
        update.rename_column("id", "order_id")
        update.delete_column("obsolete")
        update.add_column("amount", DecimalType(20, 4))
    table.metadata = transaction.table_metadata
    columns_read = []
    read_batches = pq.ParquetFile.iter_batches

    def record_columns(self, *args, **kwargs):
        columns_read.append(kwargs["columns"])
        yield from read_batches(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", record_columns)
    items = [item async for item in fetch()]
    assert [item.model_extra for item in items[:-1]] == [
        {"order_id": 1, "canary": "partition value", "at": "2023-11-14T22:13:20.123456Z", "amount": None},
        {"order_id": 2, "canary": "partition value", "at": None, "amount": None},
    ]
    assert [item.meta_.row for item in items[:-1]] == [0, 1]
    assert items[-1] == (str(table.current_snapshot().snapshot_id),)
    if file_format == FileFormat.PARQUET:
        assert columns_read == [["id", "at"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("field_type", [TimestampType(), TimestamptzType()])
async def test_backfill_reads_imported_int96_timestamps_outside_nanosecond_range(table_factory, tmp_path, field_type):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "valid_to", field_type),
    ))
    path = tmp_path / "imported.parquet"
    pq.write_table(pa.table({
        "id": [1, 2, 3],
        "valid_to": pa.array([
            datetime(9999, 12, 31, tzinfo=UTC).replace(tzinfo=None),
            datetime(1, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC).replace(tzinfo=None),
        ], type=pa.timestamp("us")),
    }), path, use_deprecated_int96_timestamps=True)
    transaction = table.transaction()
    transaction.set_properties({
        TableProperties.DEFAULT_NAME_MAPPING: table.schema().name_mapping.model_dump_json(),
    })
    with transaction.update_snapshot().fast_append() as update:
        update.append_data_file(DataFile.from_args(
            content=DataFileContent.DATA, file_path=path.as_uri(), file_format=FileFormat.PARQUET,
            partition=Record(), spec_id=0, record_count=3, file_size_in_bytes=path.stat().st_size,
        ))
    table.metadata = transaction.table_metadata
    items = [item async for item in fetch()]
    assert [item.model_extra for item in items[:-1]] == [
        {"id": 1, "valid_to": "9999-12-31T00:00:00Z"},
        {"id": 2, "valid_to": "0001-01-01T00:00:00Z"},
        {"id": 3, "valid_to": "2024-01-02T03:04:05.123456Z"},
    ]


@pytest.mark.asyncio
async def test_empty_table_can_be_polled_after_first_snapshot(table_factory):
    table = table_factory()
    assert [item async for item in fetch()] == []
    append(table, [{"id": 1, "canary": "o1"}])
    items = [item async for item in fetch()]
    assert len(items) == 2
    assert isinstance(items[0], Document)
    assert items[-1] == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
async def test_read_error_does_not_checkpoint_snapshot(monkeypatch, table_factory):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    read_batches = api._file_batches

    def fail_after_batch(table, task, schema):
        yield from read_batches(table, task, schema)
        raise OSError("made-up read failure")

    monkeypatch.setattr(api, "_file_batches", fail_after_batch)
    records, state, error = await capture(monkeypatch, [table])
    assert isinstance(error, OSError)
    assert "made-up read failure" in str(error)
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("",)

    monkeypatch.setattr(api, "_file_batches", read_batches)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [r["captured"]["doc"]["id"] for r in records if "captured" in r] == [1]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["backfill", "append", "overwrite", "replace"])
@pytest.mark.parametrize("new_snapshot", [False, True])
async def test_read_failure_resumes_first_unfinished_file(monkeypatch, table_factory, operation, new_snapshot):
    table = table_factory()
    state = None
    if operation == "backfill":
        for number in range(1, 6):
            append(table, [{"id": number, "canary": f"o{number}"}])
    else:
        append(table, [{"id": 0, "canary": "o0"}])
        _, state, error = await capture(monkeypatch, [table])
        assert error is None
        staged = table_factory("staged")
        for number in range(1, 6):
            append(staged, [{"id": number, "canary": f"o{number}"}])
        transaction = table.transaction()
        with transaction.update_snapshot().fast_append() as update:
            for task in staged.scan().plan_files():
                update.append_data_file(task.file)
        table.metadata = transaction.table_metadata
        if operation in ("overwrite", "replace"):
            snapshot = table.current_snapshot()
            table.metadata.snapshots[-1] = snapshot.model_copy(update={
                "summary": snapshot.summary.model_copy(update={"operation": Operation(operation)}),
            })
        if operation == "replace":
            # A new binding must backfill even when its current snapshot is a compaction.
            state = None

    snapshot = table.current_snapshot()
    scan = table.incremental_append_scan(
        from_snapshot_id_exclusive=snapshot.parent_snapshot_id,
    ) if operation == "append" else table.scan()
    files = sorted(task.file.file_path for task in scan.plan_files())
    assert len(files) >= 5
    files_read = []
    read_batches = api._file_batches

    def fail_on_last_file(table, task, schema):
        files_read.append(task.file.file_path)
        yield from read_batches(table, task, schema)
        if task.file.file_path == files[-1]:
            raise OSError("made-up file read failure")

    monkeypatch.setattr(api, "_file_batches", fail_on_last_file)
    records, state, error = await capture(monkeypatch, [table], state)
    assert isinstance(error, OSError)
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [doc["_meta"]["file"] for doc in docs] == files[:-1]
    assert files_read == files
    mode = "append" if operation == "append" else "full"
    assert state.bindingStateV1["shop/orders"].inc.cursor == (f"{snapshot.snapshot_id}:{len(files) - 1}:{mode}",)
    checkpoints = [
        record["checkpoint"]["state"]["updated"]["bindingStateV1"]["shop/orders"]["inc"]["cursor"]
        for record in records if "checkpoint" in record
    ]
    assert checkpoints[-(len(files) - 1):] == [
        [f"{snapshot.snapshot_id}:{done}:{mode}"] for done in range(1, len(files))
    ]

    remaining_files = [files[-1]]
    if new_snapshot:
        append(table, [{"id": 6, "canary": "o6"}])
        remaining_files.append(next(iter(table.incremental_append_scan(
            from_snapshot_id_exclusive=snapshot.snapshot_id,
        ).plan_files())).file.file_path)
    files_read.clear()

    def record_read(table, task, schema):
        files_read.append(task.file.file_path)
        assert task.file.file_path not in files[:-1]
        yield from read_batches(table, task, schema)

    monkeypatch.setattr(api, "_file_batches", record_read)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    resumed_docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert len(resumed_docs) == len(remaining_files)
    assert files_read == remaining_files
    assert [doc["_meta"]["file"] for doc in resumed_docs] == files_read
    assert resumed_docs[0]["_meta"]["snapshot_id"] == snapshot.snapshot_id
    if new_snapshot:
        assert resumed_docs[1]["id"] == 6
    assert len({doc["id"] for doc in docs + resumed_docs}) == len(files) + int(new_snapshot)
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)

    records, _, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)
    assert files_read == remaining_files


@pytest.mark.asyncio
async def test_read_failure_resumes_mid_file_without_duplicates(monkeypatch, table_factory):
    monkeypatch.setattr(api, "ROW_CHECKPOINT_INTERVAL", 1000)
    table = table_factory()
    row_count = 3500
    append(table, [{"id": number, "canary": "o"} for number in range(row_count)])
    assert len(list(table.scan().plan_files())) == 1
    snapshot_id = table.current_snapshot().snapshot_id
    read_batches = api._file_batches

    def fail_on_third_batch(table, task, schema):
        for index, batch in enumerate(read_batches(table, task, schema)):
            if index == 2:
                raise OSError("made-up file read failure")
            yield batch

    monkeypatch.setattr(api, "_file_batches", fail_on_third_batch)
    records, state, error = await capture(monkeypatch, [table])
    assert isinstance(error, OSError)
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [doc["_meta"]["row"] for doc in docs] == list(range(2000))
    checkpoints = [
        record["checkpoint"]["state"]["updated"]["bindingStateV1"]["shop/orders"]["inc"]["cursor"]
        for record in records if "checkpoint" in record
    ]
    assert checkpoints[-2:] == [[f"{snapshot_id}:0:1000:full"], [f"{snapshot_id}:0:2000:full"]]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (f"{snapshot_id}:0:2000:full",)

    monkeypatch.setattr(api, "_file_batches", read_batches)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    resumed = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [doc["_meta"]["row"] for doc in resumed] == list(range(2000, row_count))
    assert [doc["id"] for doc in docs + resumed] == list(range(row_count))
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(snapshot_id),)


@pytest.mark.parametrize("cursor", ["1:0:-1:full", "1:0:5:6:full", "1:0:5:other"])
def test_rejects_invalid_row_cursor(table_factory, cursor):
    table_factory()
    with pytest.raises(ValueError, match="Invalid Iceberg snapshot cursor"):
        next(_table_batches({"uri": CATALOG_URI}, {}, ("shop", "orders"), cursor, LOG))


@pytest.mark.asyncio
async def test_in_progress_snapshot_must_not_expire(table_factory):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    append(table, [{"id": 2, "canary": "o2"}])
    batches = fetch()
    try:
        async for item in batches:
            if isinstance(item, tuple):
                cursor = item
                break
    finally:
        await batches.aclose()
    snapshot_id = table.current_snapshot().snapshot_id
    assert cursor == (f"{snapshot_id}:1:full",)
    append(table, [{"id": 3, "canary": "o3"}])
    table.metadata = table.metadata.model_copy(update={
        "snapshots": [snapshot for snapshot in table.snapshots() if snapshot.snapshot_id != snapshot_id],
        "snapshot_log": [entry for entry in table.metadata.snapshot_log if entry.snapshot_id != snapshot_id],
    })
    assert table.current_snapshot().parent_snapshot_id == snapshot_id
    with pytest.raises(RuntimeError, match="Re-backfill this binding"):
        async for _ in fetch(cursor):
            pytest.fail("an expired in-progress snapshot must not be skipped")


def test_wide_batches_bound_document_conversion(table_factory):
    schema = Schema(*[
        NestedField(index + 1, f"column_{index}", LongType() if index < 50 else StringType())
        for index in range(100)
    ])
    table = table_factory(schema=schema)
    row_count = 2501
    transaction = table.transaction()
    transaction.append(pa.table(
        {f"column_{index}": range(row_count) if index < 50 else ["value"] * row_count for index in range(100)},
        schema=schema_to_pyarrow(table.schema()),
    ))
    table.metadata = transaction.table_metadata

    batches = _table_batches({"uri": CATALOG_URI}, {}, ("shop", "orders"), "", LOG)
    rows = []
    try:
        first = next(batches)
        assert isinstance(first, list) and len(first) == 1000
        rows.extend(first)
        for batch in batches:
            if isinstance(batch, list):
                assert len(batch) <= 1000
                rows.extend(batch)
            else:
                assert len(rows) == row_count
                assert batch == (str(table.current_snapshot().snapshot_id),)
    finally:
        batches.close()
    assert [doc.meta_.row for doc in rows] == list(range(row_count))
    assert [doc.model_extra["column_0"] for doc in rows] == list(range(row_count))
    assert all(doc.model_extra["column_99"] == "value" for doc in rows)


def test_file_batches_bound_arrow_memory(table_factory):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "first", StringType()),
        NestedField(3, "second", StringType()),
    ))
    row_count = 200_000
    data = pa.table({
        "id": range(row_count),
        "first": ["a" * 200] * row_count,
        "second": ["b" * 200] * row_count,
    }, schema=schema_to_pyarrow(table.schema()))
    allocation_limit = 8 * data.slice(0, 1000).nbytes
    transaction = table.transaction()
    transaction.set_properties({TableProperties.PARQUET_ROW_GROUP_LIMIT: "20000"})
    transaction.append(data)
    table.metadata = transaction.table_metadata
    tasks = list(table.scan().plan_files())
    assert len(tasks) == 1
    with table.io.new_input(tasks[0].file.file_path).open() as source:
        assert pq.read_metadata(source).num_row_groups == 10
    del data
    gc.collect()
    baseline = pa.total_allocated_bytes()

    batches = _table_batches({"uri": CATALOG_URI}, {}, ("shop", "orders"), "", LOG)
    try:
        first = next(batches)
        assert isinstance(first, list) and len(first) == 1000
        # Allow any eager decoding to run while the consumer is idle.
        sleep(0.1)
        assert pa.total_allocated_bytes() - baseline < allocation_limit
        count = len(first)
        for batch in batches:
            assert pa.total_allocated_bytes() - baseline < allocation_limit
            if isinstance(batch, list):
                assert len(batch) <= 1000
                assert [doc.meta_.row for doc in batch] == list(range(count, count + len(batch)))
                assert [doc.model_extra["id"] for doc in batch] == list(range(count, count + len(batch)))
                count += len(batch)
            elif batch != (str(table.current_snapshot().snapshot_id),):
                assert batch == (f"{table.current_snapshot().snapshot_id}:0:{count}:full",)
        assert count == row_count
    finally:
        batches.close()


@pytest.mark.asyncio
async def test_streams_batches_and_preserves_file_row_offsets(monkeypatch, table_factory):
    table = table_factory()
    row_count = 131_073
    transaction = table.transaction()
    transaction.append(pa.table(
        {"id": range(row_count), "canary": [None] * row_count, "at": [None] * row_count},
        schema=schema_to_pyarrow(table.schema()),
    ))
    table.metadata = transaction.table_metadata
    assert len(list(table.scan().plan_files())) == 1
    batches_read = 0
    read_batches = api._file_batches

    def count_batches(table, task, schema):
        nonlocal batches_read
        for batch in read_batches(table, task, schema):
            batches_read += 1
            yield batch

    def forbid_eager_read(*args, **kwargs):
        raise AssertionError("must not buffer a whole file or table")

    monkeypatch.setattr(api, "_file_batches", count_batches)
    monkeypatch.setattr(ArrowScan, "_record_batches_from_scan_tasks_and_deletes", forbid_eager_read)
    monkeypatch.setattr(ArrowScan, "to_record_batches", forbid_eager_read)
    monkeypatch.setattr(DataScan, "to_arrow", forbid_eager_read)
    count = 0
    async for item in fetch():
        if isinstance(item, Document):
            if count == 0:
                assert batches_read == 1
            assert item.meta_.row == count
            assert item.model_extra["id"] == count
            count += 1
        elif count < row_count:
            assert item == (f"{table.current_snapshot().snapshot_id}:0:{count}:full",)
        else:
            assert item == (str(table.current_snapshot().snapshot_id),)
    assert count == row_count
    assert batches_read == (row_count + 999) // 1000


@pytest.mark.asyncio
async def test_position_deletes_keep_original_row_numbers(monkeypatch, table_factory, tmp_path):
    table = table_factory()
    append(table, [{"id": i, "canary": f"o{i}"} for i in range(4)])
    tasks = list(table.scan().plan_files())
    delete_path = tmp_path / "deletes.parquet"
    pq.write_table(pa.table({
        "file_path": [tasks[0].file.file_path] * 2,
        "pos": [0, 2],
    }), delete_path)
    tasks[0].delete_files.add(DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_path.as_uri(), file_format=FileFormat.PARQUET,
        partition=Record(), record_count=2, file_size_in_bytes=delete_path.stat().st_size,
    ))
    monkeypatch.setattr(DataScan, "plan_files", lambda self: tasks)

    items = [item async for item in fetch()]
    assert [item.model_extra["id"] for item in items[:-1]] == [1, 3]
    assert [item.meta_.row for item in items[:-1]] == [1, 3]
    assert items[-1] == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
async def test_shared_delete_file_is_decoded_once(monkeypatch, table_factory, tmp_path):
    table = table_factory()
    for number in range(3):
        append(table, [{"id": number * 10 + i, "canary": "o"} for i in range(3)])
    tasks = sorted(table.scan().plan_files(), key=lambda task: task.file.file_path)
    delete_path = tmp_path / "deletes.parquet"
    pq.write_table(pa.table({
        "file_path": [task.file.file_path for task in tasks],
        "pos": [1] * len(tasks),
    }), delete_path)
    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_path.as_uri(), file_format=FileFormat.PARQUET,
        partition=Record(), record_count=len(tasks), file_size_in_bytes=delete_path.stat().st_size,
    )
    for task in tasks:
        task.delete_files.add(delete_file)
    monkeypatch.setattr(DataScan, "plan_files", lambda self: tasks)
    decoded = []
    read_deletes = api._read_deletes

    def count_reads(io, data_file):
        decoded.append(data_file.file_path)
        return read_deletes(io, data_file)

    monkeypatch.setattr(api, "_read_deletes", count_reads)
    items = [item async for item in fetch()]
    documents = [item for item in items if isinstance(item, Document)]
    assert sorted(document.model_extra["id"] for document in documents) == [0, 2, 10, 12, 20, 22]
    assert {document.meta_.row for document in documents} == {0, 2}
    assert decoded == [delete_path.as_uri()]


@pytest.mark.asyncio
async def test_vended_storage_credentials_refresh_between_files(monkeypatch, table_factory):
    config = {}
    table = table_factory(config=config)
    for number in range(3):
        append(table, [{"id": number, "canary": f"o{number}"}])
    loads = []
    load_table = IcebergCatalog.load_table

    def count_loads(self, identifier):
        loads.append(identifier)
        config["made-up.generation"] = str(len(loads))
        return load_table(self, identifier)

    clock = iter(range(0, 10_000, 20 * 60))
    monkeypatch.setattr(api, "monotonic", lambda: next(clock))
    monkeypatch.setattr(IcebergCatalog, "load_table", count_loads)
    generations = []
    read_batches = api._file_batches

    def record_io(table, task, schema):
        generations.append(table.io.properties["made-up.generation"])
        yield from read_batches(table, task, schema)

    monkeypatch.setattr(api, "_file_batches", record_io)
    items = [item async for item in fetch()]
    assert sorted(item.model_extra["id"] for item in items if isinstance(item, Document)) == [0, 1, 2]
    # Loaded at minute 0; checked at 20, then at 40 (reload at 60), then at 80.
    assert generations == ["1", "2", "2"]
    assert len(loads) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_keys", [False, True])
async def test_expired_storage_token_reloads_and_resumes_file(monkeypatch, table_factory, explicit_keys):
    config = {}
    table = table_factory(config=config)
    append(table, [{"id": i, "canary": "o"} for i in range(1500)])
    loads = []
    load_table = IcebergCatalog.load_table

    def count_loads(self, identifier):
        loads.append(identifier)
        config["made-up.generation"] = str(len(loads))
        return load_table(self, identifier)

    monkeypatch.setattr(IcebergCatalog, "load_table", count_loads)
    generations = []
    read_batches = api._file_batches

    def expire_once(table, task, schema):
        generations.append(table.io.properties["made-up.generation"])
        for batch in read_batches(table, task, schema):
            yield batch
            if len(generations) == 1:
                raise OSError("AWS Error ACCESS_DENIED during GetObject operation: ExpiredToken")

    monkeypatch.setattr(api, "_file_batches", expire_once)
    file_io_properties = {"s3.access-key-id": "made-up-key"} if explicit_keys else {}
    rows = []
    with pytest.raises(OSError, match="ExpiredToken") if explicit_keys else nullcontext():
        async for item in fetch_table({"uri": CATALOG_URI}, file_io_properties, ("shop", "orders"), LOG, ("",)):
            if isinstance(item, Document):
                rows.append(item.meta_.row)
    if explicit_keys:
        assert rows == list(range(1000))
        assert len(loads) == 1
    else:
        # Rows emitted before the failure are not emitted again.
        assert rows == list(range(1500))
        assert generations == ["1", "2"]


@pytest.mark.asyncio
async def test_unrepresentable_value_names_column_file_and_row(table_factory, tmp_path):
    table = table_factory(schema=Schema(
        NestedField(1, "id", LongType(), required=True), NestedField(2, "at", TimestamptzType()),
    ))
    path = tmp_path / "imported.parquet"
    pq.write_table(pa.table({
        "id": [1, 2], "at": pa.array([0, 253_402_300_800_000_000], pa.timestamp("us", "UTC")),
    }), path)
    transaction = table.transaction()
    transaction.set_properties({
        TableProperties.DEFAULT_NAME_MAPPING: table.schema().name_mapping.model_dump_json(),
    })
    with transaction.update_snapshot().fast_append() as update:
        update.append_data_file(DataFile.from_args(
            content=DataFileContent.DATA, file_path=path.as_uri(), file_format=FileFormat.PARQUET,
            partition=Record(), spec_id=0, record_count=2, file_size_in_bytes=path.stat().st_size,
        ))
    table.metadata = transaction.table_metadata
    data_file = path.as_uri()
    with pytest.raises(ValueError, match=f"column 'at' at row 1 of Iceberg data file {data_file}: year 10000"):
        _ = [item async for item in fetch()]


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_processed", [False, True])
async def test_incremental_follows_ancestry_and_reads_only_added_files(
    monkeypatch, table_factory, expire_processed
):
    table = table_factory()
    snapshot_ids = iter([900, 200, 800])
    monkeypatch.setattr(type(table.metadata), "new_snapshot_id", lambda self: next(snapshot_ids))
    transaction = table.transaction()
    transaction.set_properties({
        TableProperties.MANIFEST_MERGE_ENABLED: "true",
        TableProperties.MANIFEST_MIN_MERGE_COUNT: "1",
    })
    table.metadata = transaction.table_metadata
    append(table, [{"id": 1, "canary": "o1"}])
    _, state, error = await capture(monkeypatch, [table])
    assert error is None
    old_files = {task.file.file_path for task in table.scan().plan_files()}

    append(table, [{"id": 2, "canary": "o2"}, {"id": 3, "canary": "o3"}])
    append(table, [{"id": 4, "canary": "o4"}])
    # Merged manifests contain both existing and newly added data files.
    assert len(table.current_snapshot().manifests(table.io)) == 1
    new_files = {task.file.file_path for task in table.scan().plan_files()} - old_files
    if expire_processed:
        # A catalog may retain the child's parent ID after expiring the parent's metadata.
        table.metadata = table.metadata.model_copy(update={
            "snapshots": [snapshot for snapshot in table.snapshots() if snapshot.snapshot_id != 900],
            "snapshot_log": [entry for entry in table.metadata.snapshot_log if entry.snapshot_id != 900],
        })
        assert table.snapshot_by_id(900) is None
        assert table.snapshot_by_id(200).parent_snapshot_id == 900

    files_read = Counter()
    new_input = PyArrowFileIO.new_input

    def record_input(self, location):
        assert location not in old_files, "an append must not read previously captured data files"
        if location in new_files:
            files_read[location] += 1
        return new_input(self, location)

    monkeypatch.setattr(PyArrowFileIO, "new_input", record_input)
    records, resumed_state, error = await capture(monkeypatch, [table], state)
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [(doc["id"], doc["canary"], doc["_meta"]["snapshot_id"], doc["_meta"]["row"]) for doc in docs] == [
        (2, "o2", 200, 0), (3, "o3", 200, 1), (4, "o4", 800, 0),
    ]
    assert files_read == Counter({file: 1 for file in new_files})
    checkpoints = [record["checkpoint"]["state"]["updated"] for record in records if "checkpoint" in record]
    assert [checkpoint["bindingStateV1"]["shop/orders"]["inc"]["cursor"] for checkpoint in checkpoints] == [
        ["200"], ["800"],
    ]
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("900",)
    assert resumed_state.bindingStateV1["shop/orders"].inc.cursor == ("800",)

    records, _, error = await capture(monkeypatch, [table], resumed_state)
    assert error is None
    assert not any("captured" in record for record in records)
    assert files_read == Counter({file: 1 for file in new_files})


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", [Operation.OVERWRITE, Operation.DELETE, Operation.REPLACE])
async def test_incremental_non_append_snapshots(monkeypatch, table_factory, caplog, operation):
    table = table_factory()
    original_rows = [{"id": 1, "canary": "o1"}, {"id": 2, "canary": "o2"}]
    append(table, original_rows)
    replaced_file = next(iter(table.scan().plan_files())).file
    append(table, [{"id": 3, "canary": "o3"}])
    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    old_keys = {
        (doc["_meta"]["snapshot_id"], doc["_meta"]["file"], doc["_meta"]["row"])
        for record in records if "captured" in record
        for doc in [record["captured"]["doc"]]
    }

    replacement = None
    expected = {(3, "o3")}
    if operation != Operation.DELETE:
        rows = [{"id": 2, "canary": "o2-updated"}] if operation == Operation.OVERWRITE else original_rows
        replacement = table_factory("replacement")
        append(replacement, rows)
        expected.update((row["id"], row["canary"]) for row in rows)
    transaction = table.transaction()
    if operation == Operation.DELETE:
        transaction.delete("id < 3")
    else:
        with transaction.update_snapshot().overwrite() as update:
            update.delete_data_file(replaced_file)
            update.append_data_file(next(iter(replacement.scan().plan_files())).file)
    table.metadata = transaction.table_metadata
    if operation == Operation.REPLACE:
        # PyIceberg cannot write REPLACE snapshots; these files preserve every row, as a compaction does.
        snapshot = table.current_snapshot()
        table.metadata.snapshots[-1] = snapshot.model_copy(update={
            "summary": snapshot.summary.model_copy(update={"operation": operation}),
        })
    rewritten_snapshot_id = table.current_snapshot().snapshot_id
    assert table.current_snapshot().summary.operation == operation
    if operation == Operation.REPLACE:
        expected = set()
        read_batches = api._file_batches
        rewritten_files = {task.file.file_path for task in table.scan().plan_files()}

        def forbid_replaced_files(table, task, schema):
            assert task.file.file_path not in rewritten_files, "compaction must not re-emit rows"
            yield from read_batches(table, task, schema)

        monkeypatch.setattr(api, "_file_batches", forbid_replaced_files)
    append(table, [{"id": 4, "canary": "o4"}])

    caplog.clear()
    caplog.set_level(logging.INFO)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert len(docs) == len(expected) + 1
    assert {(doc["id"], doc["canary"]) for doc in docs[:-1]} == expected
    for doc in docs[:-1]:
        meta = doc["_meta"]
        assert meta["snapshot_id"] == rewritten_snapshot_id
        assert (meta["snapshot_id"], meta["file"], meta["row"]) not in old_keys
        assert Document.model_validate(doc).meta_.op == "u"
    assert (docs[-1]["id"], docs[-1]["canary"]) == (4, "o4")
    assert docs[-1]["_meta"]["snapshot_id"] == table.current_snapshot().snapshot_id
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)
    assert any(
        record.message == ("skipping Iceberg replace snapshot" if operation == Operation.REPLACE else "reading full Iceberg snapshot")
        and record.args["snapshot_id"] == rewritten_snapshot_id
        and (operation == Operation.REPLACE or record.args["operation"] == str(operation))
        for record in caplog.records
    )
    if operation == Operation.REPLACE:
        checkpoints = [record["checkpoint"]["state"]["updated"] for record in records if "checkpoint" in record]
        assert [checkpoint["bindingStateV1"]["shop/orders"]["inc"]["cursor"] for checkpoint in checkpoints] == [
            [str(rewritten_snapshot_id)], [str(table.current_snapshot().snapshot_id)],
        ]

    records, _, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)


@pytest.mark.asyncio
async def test_replace_only_checkpoints_without_planning_files(monkeypatch, table_factory):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    _, state, error = await capture(monkeypatch, [table])
    assert error is None
    snapshots = []
    for index in range(2):
        replacement = table_factory(f"compacted{index}")
        append(replacement, [{"id": 1, "canary": "o1"}])
        transaction = table.transaction()
        with transaction.update_snapshot().overwrite() as update:
            for task in table.scan().plan_files():
                update.delete_data_file(task.file)
            update.append_data_file(next(iter(replacement.scan().plan_files())).file)
        table.metadata = transaction.table_metadata
        snapshot = table.current_snapshot()
        table.metadata.snapshots[-1] = snapshot.model_copy(update={
            "summary": snapshot.summary.model_copy(update={"operation": Operation.REPLACE}),
        })
        snapshots.append(str(snapshot.snapshot_id))

    def forbid_scan(*args, **kwargs):
        pytest.fail("replace snapshots must not plan or read files")

    monkeypatch.setattr(DataScan, "plan_files", forbid_scan)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)
    assert [
        record["checkpoint"]["state"]["updated"]["bindingStateV1"]["shop/orders"]["inc"]["cursor"]
        for record in records if "checkpoint" in record
    ] == [[snapshot] for snapshot in snapshots]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (snapshots[-1],)
    records, _, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record or "checkpoint" in record for record in records)


@pytest.mark.asyncio
async def test_incremental_read_failure_resumes_last_committed_snapshot(monkeypatch, table_factory):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    _, state, error = await capture(monkeypatch, [table])
    assert error is None
    append(table, [{"id": 2, "canary": "o2"}])
    completed_snapshot_id = table.current_snapshot().snapshot_id
    append(table, [{"id": 3, "canary": "o3"}])
    incomplete_files = {
        task.file.file_path for task in table.incremental_append_scan(
            from_snapshot_id_exclusive=completed_snapshot_id,
        ).plan_files()
    }
    read_batches = api._file_batches

    def fail_after_last_batch(table, task, schema):
        yield from read_batches(table, task, schema)
        if task.file.file_path in incomplete_files:
            raise OSError("made-up incremental read failure")

    monkeypatch.setattr(api, "_file_batches", fail_after_last_batch)
    records, state, error = await capture(monkeypatch, [table], state)
    assert isinstance(error, OSError)
    assert "made-up incremental read failure" in str(error)
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [2]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(completed_snapshot_id),)

    monkeypatch.setattr(api, "_file_batches", read_batches)
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [3]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
async def test_incremental_checkpoints_empty_snapshot(monkeypatch, table_factory):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    _, state, error = await capture(monkeypatch, [table])
    assert error is None
    original_cursor = state.bindingStateV1["shop/orders"].inc.cursor
    transaction = table.transaction()
    transaction.delete("id > 0")
    table.metadata = transaction.table_metadata
    assert table.current_snapshot().summary.operation == Operation.DELETE
    assert list(table.scan().plan_files()) == []

    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor != original_cursor
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)

    append(table, [{"id": 2, "canary": "o2"}])
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [2]
    assert state.bindingStateV1["shop/orders"].inc.cursor == (str(table.current_snapshot().snapshot_id),)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expire_pending", "expire_parent_link", "rollback"])
async def test_incremental_rejects_missing_history(monkeypatch, table_factory, change):
    table = table_factory()
    append(table, [{"id": 1, "canary": "o1"}])
    first_snapshot_id = table.current_snapshot().snapshot_id
    append(table, [{"id": 2, "canary": "o2"}])
    _, state, error = await capture(monkeypatch, [table])
    assert error is None
    original_cursor = state.bindingStateV1["shop/orders"].inc.cursor
    append(table, [{"id": 3, "canary": "o3"}])
    pending_snapshot_id = table.current_snapshot().snapshot_id
    append(table, [{"id": 4, "canary": "o4"}])
    transaction = table.transaction()
    if change in ("expire_pending", "expire_parent_link"):
        with ExpireSnapshots(transaction) as expire:
            # PyIceberg expiration also clears the surviving child's parent link.
            expire.by_id(pending_snapshot_id if change == "expire_pending" else int(original_cursor[0]))
    else:
        with ManageSnapshots(transaction) as manage:
            manage.set_current_snapshot(snapshot_id=first_snapshot_id)
    table.metadata = transaction.table_metadata

    records, state, error = await capture(monkeypatch, [table], state)
    assert isinstance(error, RuntimeError)
    assert "Re-backfill this binding" in str(error)
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == original_cursor


@pytest.fixture
def repository_fixture(monkeypatch, table_factory):
    fixture_dir = Path(__file__).resolve().parents[2] / "tests" / "source-iceberg"
    fixture = runpy.run_path(str(fixture_dir / "seed.py"))
    storage = pafs.PyFileSystem(pafs.FSSpecHandler(MemoryFileSystem()))
    monkeypatch.setattr(pafs, "S3FileSystem", lambda **kwargs: storage)
    file_io = PyArrowFileIO({"s3.region": "us-east-1"})
    metadata_location = fixture["write_fixture"](file_io)
    metadata = FromInputFile.table_metadata(file_io.new_input(metadata_location))
    table = table_factory(
        schema=metadata.schema(), location=metadata.location, file_io=file_io,
        config={"s3.region": "us-east-1"},
    )
    table.metadata = metadata
    table.metadata_location = metadata_location
    expected = (fixture_dir / "snapshot.jsonl").read_text().splitlines()
    return table, expected


@pytest.mark.asyncio
async def test_repository_fixture_matches_snapshot(monkeypatch, repository_fixture):
    table, expected = repository_fixture
    metadata = table.metadata
    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("102",)

    def preview_lines(records):
        lines = []
        for record in records:
            if "captured" not in record:
                continue
            doc = record["captured"]["doc"]
            # flowctl preview stamps this UUID after the connector emits each document.
            doc["_meta"]["uuid"] = "DocUUIDPlaceholder-329Bb50aa48EAa9ef"
            lines.append(json.dumps(
                ["tests/source-iceberg/data", doc], sort_keys=True, separators=(",", ":"),
            ))
        return sorted(lines)

    assert len(expected) == 5
    assert preview_lines(records) == expected

    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert not any("captured" in record for record in records)
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("102",)

    table.metadata = metadata.model_copy(update={
        "current_snapshot_id": 101,
        "refs": {"main": SnapshotRef(snapshot_id=101, snapshot_ref_type=SnapshotRefType.BRANCH)},
    })
    records, state, error = await capture(monkeypatch, [table])
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [1, 2, 3]
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("101",)

    table.metadata = metadata
    records, state, error = await capture(monkeypatch, [table], state)
    assert error is None
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == [4, 5]
    assert preview_lines(records) == expected[3:]
    assert state.bindingStateV1["shop/orders"].inc.cursor == ("102",)
