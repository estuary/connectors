from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO, parquet_file_to_data_file, schema_to_pyarrow
from pyiceberg.manifest import ManifestEntry, ManifestEntryStatus, write_manifest, write_manifest_list
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, SnapshotLogEntry, Summary
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import BinaryType, DecimalType, LongType, NestedField, StringType, TimestamptzType


LOCATION = "s3://warehouse/test/orders"
STORAGE_ENDPOINT = "http://localhost:9010"
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "canary", StringType(), required=True),
    NestedField(3, "at", TimestamptzType()),
    NestedField(4, "amount", DecimalType(12, 2)),
    NestedField(5, "payload", BinaryType()),
)
ROWS = [
    [
        {
            "id": 1, "canary": "o1",
            "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            "amount": Decimal("12.34"), "payload": b"\x00\xff",
        },
        {
            "id": 2, "canary": "o2",
            "at": datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
            "amount": Decimal("-0.10"), "payload": b"iceberg",
        },
        {"id": 3, "canary": "o3"},
    ],
    [
        {"id": 4, "canary": "o4", "amount": Decimal("0.00"), "payload": b""},
        {"id": 5, "canary": "o5", "amount": Decimal("1234567890.12")},
    ],
]


def write_fixture(file_io: FileIO) -> str:
    # Explicit snapshot IDs and file paths keep every captured _meta field stable.
    metadata = new_table_metadata(
        SCHEMA, UNPARTITIONED_PARTITION_SPEC, UNSORTED_SORT_ORDER, LOCATION,
        properties={"format-version": "2"},
        table_uuid=UUID("00000000-0000-0000-0000-000000000001"),
    )
    snapshots = []
    manifests = []
    timestamp_ms = 1767323045000
    for sequence, rows in enumerate(ROWS, start=1):
        snapshot_id = 100 + sequence
        parent_id = snapshots[-1].snapshot_id if snapshots else None
        file_path = f"{LOCATION}/data/orders-{sequence}.parquet"
        with file_io.new_output(file_path).create() as output:
            pq.write_table(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(SCHEMA)), output)

        with write_manifest(
            format_version=2,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=SCHEMA,
            output_file=file_io.new_output(f"{LOCATION}/metadata/manifest-{sequence}.avro"),
            snapshot_id=snapshot_id,
            avro_compression="null",
        ) as writer:
            writer.add_entry(ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence,
                file_sequence_number=sequence,
                data_file=parquet_file_to_data_file(file_io, metadata, file_path),
            ))
        manifests.append(writer.to_manifest_file())

        manifest_list = f"{LOCATION}/metadata/snap-{snapshot_id}.avro"
        with write_manifest_list(
            format_version=2,
            output_file=file_io.new_output(manifest_list),
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_id,
            sequence_number=sequence,
            avro_compression="null",
        ) as writer:
            writer.add_manifests(manifests)

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_id,
            sequence_number=sequence,
            timestamp_ms=timestamp_ms + sequence,
            manifest_list=manifest_list,
            summary=Summary(operation=Operation.APPEND),
            schema_id=SCHEMA.schema_id,
        )
        snapshots.append(snapshot)
        manifests = snapshot.manifests(file_io)

    current = snapshots[-1]
    metadata = metadata.model_copy(update={
        "snapshots": snapshots,
        "snapshot_log": [
            SnapshotLogEntry(snapshot_id=s.snapshot_id, timestamp_ms=s.timestamp_ms)
            for s in snapshots
        ],
        "current_snapshot_id": current.snapshot_id,
        "last_sequence_number": current.sequence_number,
        "last_updated_ms": current.timestamp_ms,
        "refs": {"main": SnapshotRef(
            snapshot_id=current.snapshot_id, snapshot_ref_type=SnapshotRefType.BRANCH,
        )},
    })
    metadata_location = f"{LOCATION}/metadata/00000.metadata.json"
    ToOutputFile.table_metadata(metadata, file_io.new_output(metadata_location))
    return metadata_location


def main(catalog_uri: str = "http://localhost:8181", storage_endpoint: str = STORAGE_ENDPOINT):
    storage = pafs.S3FileSystem(
        endpoint_override=storage_endpoint,
        access_key="test_key",
        secret_key="test_secret",
        region="us-east-1",
        force_virtual_addressing=False,
        allow_bucket_creation=True,
    )
    storage.create_dir("warehouse")
    properties = {
        "s3.endpoint": storage_endpoint,
        "s3.region": "us-east-1",
        "s3.access-key-id": "test_key",
        "s3.secret-access-key": "test_secret",
        "s3.force-virtual-addressing": "false",
    }
    with RestCatalog("test", uri=catalog_uri, **properties) as catalog:
        catalog.create_namespace_if_not_exists("test")
        if catalog.table_exists(("test", "orders")):
            table = catalog.load_table(("test", "orders"))
        else:
            metadata_location = write_fixture(PyArrowFileIO(properties))
            table = catalog.register_table(("test", "orders"), metadata_location)
        assert table.schema() == SCHEMA
        assert table.current_snapshot().snapshot_id == 102
        assert sum(task.file.record_count for task in table.scan().plan_files()) == 5
    print("Seeded test.orders: five rows in two data files and two append snapshots.")


if __name__ == "__main__":
    main()
