import asyncio
from collections import Counter
from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import ExitStack
from logging import Logger, WARNING, getLogger
from time import monotonic
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from estuary_cdk.capture.common import LogCursor
from pydantic import ValidationError
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import ForbiddenError, NoSuchNamespaceError, ServerError, ServiceUnavailableError
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.io.pyarrow import (
    ArrowAccessor,
    _get_column_projection_values,  # pyright: ignore[reportPrivateUsage]
    _read_deletes,  # pyright: ignore[reportPrivateUsage]
    pyarrow_to_schema,
)
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema, prune_columns, visit_with_partner
from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE, FileScanTask, Table
from pyiceberg.table.snapshots import Operation, Snapshot
from pyiceberg.typedef import Identifier
from pyiceberg.types import ListType, MapType
from pyiceberg.utils.config import Config
from requests import HTTPError
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .models import CatalogErrorResponse, Document
from .files import ArrowProjection, avro_batches, open_avro, orc_field
from .schema import serialize_value

# Vended storage tokens often last an hour; reload them well before that between files.
CREDENTIAL_REFRESH_SECONDS = 30 * 60
# Checkpoint within large data files so a restart does not re-read them from the start.
ROW_CHECKPOINT_INTERVAL = 100_000


class IcebergCatalog(RestCatalog):
    def __init__(
        self, name: str, file_io_properties: dict[str, str], **properties: str
    ):
        self.file_io_properties = file_io_properties
        super().__init__(name, **properties, **file_io_properties)

    # PyIceberg retries expired credentials, but not catalog server errors.
    @retry(
        retry=retry_if_exception_type((ServerError, ServiceUnavailableError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=8),
        before_sleep=before_sleep_log(getLogger(__name__), WARNING, exc_info=True),
        reraise=True,
    )
    def load_table(self, identifier: str | Identifier) -> Table:
        try:
            return super().load_table(identifier)
        except (ServerError, ServiceUnavailableError) as err:
            cause = err.__cause__
            if isinstance(cause, HTTPError) and cause.response is not None:
                try:
                    error = CatalogErrorResponse.model_validate_json(cause.response.text).error
                except ValidationError:
                    pass
                else:
                    # PyIceberg omits the server stack, including nested SQL causes.
                    if error.stack:
                        err.add_note("Iceberg REST catalog server stack:\n" + "\n".join(error.stack))
            raise

    def _load_file_io(
        self, properties: dict[str, Any] | None = None, location: str | None = None
    ) -> FileIO:
        # Explicit storage settings take precedence over both catalog and table config.
        merged = {**self.properties, **(properties or {})}
        if self.properties.get("rest.sigv4-enabled") == "true" and "s3.access-key-id" in merged:
            # A vended S3 key without a token must not inherit the catalog's session token.
            for credential in ("access-key-id", "secret-access-key", "session-token"):
                merged.pop(f"client.{credential}", None)
        if "s3.access-key-id" in self.file_io_properties:
            for prefix in ("s3", "client"):
                for credential in ("access-key-id", "secret-access-key", "session-token"):
                    merged.pop(f"{prefix}.{credential}", None)
        return load_file_io({**merged, **self.file_io_properties}, location)


def list_table_identifiers(log: Logger, catalog: Catalog, namespaces: list[str]) -> list[Identifier]:
    pending = (
        [tuple(namespace.split(".")) for namespace in namespaces]
        if namespaces
        else list(catalog.list_namespaces())
    )
    seen: set[Identifier] = set()
    tables: set[Identifier] = set()

    while pending:
        namespace = pending.pop()
        if namespace in seen:
            continue
        seen.add(namespace)
        try:
            tables.update(catalog.list_tables(namespace))
            if not namespaces:
                pending.extend(catalog.list_namespaces(namespace))
        except (NoSuchNamespaceError, ForbiddenError) as err:
            if namespaces:
                raise
            log.warning("skipping unreadable Iceberg namespace", {"namespace": ".".join(namespace), "error": str(err)})

    return sorted(tables)


def _pending_snapshots(table: Table, last_snapshot_id: str) -> list[Snapshot]:
    current = table.current_snapshot()
    if current is None:
        return []
    if not last_snapshot_id:
        return [current]

    snapshot_id, in_progress, _ = last_snapshot_id.partition(":")
    pending = []
    snapshot = current
    while str(snapshot.snapshot_id) != snapshot_id:
        pending.append(snapshot)
        # The processed snapshot may have expired; its child's parent ID is sufficient.
        if not in_progress and str(snapshot.parent_snapshot_id) == snapshot_id:
            break
        parent = (
            table.snapshot_by_id(snapshot.parent_snapshot_id)
            if snapshot.parent_snapshot_id is not None
            else None
        )
        if parent is None:
            raise RuntimeError(
                f"Snapshot {snapshot_id} is no longer an ancestor of "
                f"snapshot {current.snapshot_id}. Re-backfill this binding."
            )
        snapshot = parent

    if in_progress:
        pending.append(snapshot)
    return list(reversed(pending))


def _orc_batches(file_path: str, batches: Iterator[pa.RecordBatch]) -> Generator[pa.RecordBatch]:
    # Arrow decodes ORC timestamps only as nanoseconds, which cannot hold dates outside 1677-2262.
    try:
        yield from batches
    except pa.ArrowInvalid as error:
        if "out of range for nanosecond resolution" not in str(error):
            raise
        raise RuntimeError(
            f"Cannot read ORC data file {file_path}: ORC timestamp values before 1677-09-21 "
            f"or after 2262-04-11 are not supported. ({error})"
        ) from error


def _file_batches(table: Table, task: FileScanTask, schema: Schema) -> Generator[pa.RecordBatch]:
    downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE)
    if downcast_ns_timestamp_to_us is None:
        downcast_ns_timestamp_to_us = table.metadata.format_version <= 2
    # ORC stores Iceberg timestamps in microseconds, but Arrow returns nanoseconds. The projection
    # downcasts only timestamp and timestamptz fields, so timestamp_ns fields keep their precision.
    project_downcast_ns_timestamp_to_us = downcast_ns_timestamp_to_us or task.file.file_format == FileFormat.ORC
    projected_field_ids = {
        field_id for field_id in schema.field_ids
        if not isinstance(schema.find_type(field_id), (MapType, ListType))
    }
    with ExitStack() as stack:
        input_file = table.io.new_input(task.file.file_path)
        if task.file.file_format == FileFormat.AVRO:
            avro_file = stack.enter_context(open_avro(input_file, table.metadata.name_mapping()))
            file_schema = avro_file.schema
        else:
            source = stack.enter_context(input_file.open())
            if task.file.file_format == FileFormat.PARQUET:
                # Hive and Spark write INT96 timestamps; nanoseconds would overflow outside 1677-2262.
                parquet_file = stack.enter_context(
                    pq.ParquetFile(source, pre_buffer=False, coerce_int96_timestamp_unit="us")
                )
                arrow_schema = parquet_file.schema_arrow
            elif task.file.file_format == FileFormat.ORC:
                fragment = ds.OrcFileFormat().make_fragment(source)
                arrow_schema = pa.schema([orc_field(field) for field in fragment.physical_schema])
            else:
                raise ValueError(f"Unsupported Iceberg data file format: {task.file.file_format}")
            file_schema = pyarrow_to_schema(
                arrow_schema, table.metadata.name_mapping(),
                downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
                format_version=table.metadata.format_version,
            )
        # Match PyIceberg's _task_to_record_batches projection without scanner readahead.
        projected_missing_fields = _get_column_projection_values(
            task.file, schema, table.schema(), table.metadata.specs().get(task.file.spec_id), file_schema.field_ids,
        )
        file_project_schema = prune_columns(file_schema, projected_field_ids, select_full_types=False)
        columns = [column.name for column in file_project_schema.columns]
        if task.file.file_format == FileFormat.PARQUET:
            batches = parquet_file.iter_batches(batch_size=1000, columns=columns)
        elif task.file.file_format == FileFormat.ORC:
            batches = (
                batch.cast(pa.schema([arrow_schema.field(name) for name in columns]))
                for batch in _orc_batches(task.file.file_path, fragment.to_batches(
                    columns=columns, batch_size=1000, batch_readahead=0,
                    fragment_readahead=0, use_threads=False,
                ))
            )
        else:
            batches = avro_batches(avro_file, task.file.record_count)
            file_project_schema = file_schema
        for batch in batches:
            yield pa.RecordBatch.from_struct_array(visit_with_partner(
                schema, batch,
                ArrowProjection(
                    file_project_schema,
                    downcast_ns_timestamp_to_us=project_downcast_ns_timestamp_to_us,
                    projected_missing_fields=projected_missing_fields,
                    allow_timestamp_tz_mismatch=True,
                ),
                ArrowAccessor(file_project_schema),
            ))


def _delete_positions(io: FileIO, delete_file: DataFile) -> dict[str, Any]:
    """Map each data file path to the positions this delete file removes from it."""
    if delete_file.file_format != FileFormat.AVRO:
        return _read_deletes(io, delete_file)
    positions: dict[str, list[int]] = {}
    with open_avro(io.new_input(delete_file.file_path)) as reader:
        for batch in avro_batches(reader, delete_file.record_count):
            for row in batch.to_pylist():
                positions.setdefault(row["file_path"], []).append(row["pos"])
    return {path: pa.array(rows, pa.int64()) for path, rows in positions.items()}


def _serialize_row(batch: pa.RecordBatch, index: int, schema: Schema, file_path: str, row: int) -> dict[str, Any]:
    document = {}
    for field in schema.fields:
        try:
            document[field.name] = serialize_value(batch.column(field.name)[index], field.field_type)
        except ValueError as err:
            raise ValueError(
                f"Cannot capture column {field.name!r} at row {row} of Iceberg data file {file_path}: {err}"
            ) from err
    return document


def _table_batches(
    properties: dict[str, str],
    file_io_properties: dict[str, str],
    identifier: Identifier,
    last_snapshot_id: str,
    log: Logger,
) -> Iterator[list[Document] | tuple[str]]:
    files_done = 0
    rows_done = 0
    full_snapshot = not last_snapshot_id
    if ":" in last_snapshot_id:
        parts = last_snapshot_id.split(":")
        if len(parts) not in (3, 4):
            raise ValueError(f"Invalid Iceberg snapshot cursor: {last_snapshot_id}")
        files_done = int(parts[1])
        rows_done = int(parts[2]) if len(parts) == 4 else 0
        mode = parts[-1]
        if files_done < 0 or rows_done < 0 or mode not in ("full", "append"):
            raise ValueError(f"Invalid Iceberg snapshot cursor: {last_snapshot_id}")
        full_snapshot = mode == "full"

    with IcebergCatalog("estuary", file_io_properties, **properties) as catalog:
        table = catalog.load_table(identifier)
        loaded_at = monotonic()
        # Without explicit S3 keys, storage credentials come from the catalog and can expire mid-read.
        vended_credentials = "s3.access-key-id" not in file_io_properties

        def reload_file_io(message: str):
            nonlocal loaded_at
            log.info(message, {"table": ".".join(identifier)})
            # Keep the loaded metadata so the snapshot plan and schema stay stable.
            table.io = catalog.load_table(identifier).io
            loaded_at = monotonic()

        # Schema changes need not create a snapshot. Match the schema used by discovery.
        schema = table.schema()
        for snapshot in _pending_snapshots(table, last_snapshot_id):
            if not full_snapshot and snapshot.summary and snapshot.summary.operation == Operation.REPLACE:
                log.info("skipping Iceberg replace snapshot", {"table": ".".join(identifier), "snapshot_id": snapshot.snapshot_id})
                yield (str(snapshot.snapshot_id),)
                files_done = rows_done = 0
                continue
            append_only = not full_snapshot and snapshot.summary and snapshot.summary.operation == Operation.APPEND
            if append_only:
                tasks = table.incremental_append_scan(
                    from_snapshot_id_exclusive=snapshot.parent_snapshot_id,
                    to_snapshot_id_inclusive=snapshot.snapshot_id,
                ).plan_files()
            else:
                log.info(
                    "reading full Iceberg snapshot",
                    {
                        "table": ".".join(identifier),
                        "snapshot_id": snapshot.snapshot_id,
                        "operation": str(snapshot.summary.operation) if snapshot.summary else None,
                    },
                )
                tasks = table.scan(snapshot_id=snapshot.snapshot_id).plan_files()

            # Snapshot membership is immutable; sorting makes file offsets stable on resume.
            tasks = sorted(tasks, key=lambda task: task.file.file_path)
            mode = "append" if append_only else "full"
            if files_done > len(tasks) or (rows_done and files_done == len(tasks)):
                raise RuntimeError(
                    f"Snapshot {snapshot.snapshot_id} has fewer files than its saved cursor. Re-backfill this binding."
                )
            # A delete file can apply to many data files; decode it once and drop it after its last use.
            delete_uses = Counter(
                delete_file.file_path for task in tasks[files_done:] for delete_file in task.delete_files
            )
            decoded_deletes: dict[str, dict[str, Any]] = {}
            for file_index, task in enumerate(tasks):
                if file_index < files_done:
                    continue
                if vended_credentials and monotonic() - loaded_at >= CREDENTIAL_REFRESH_SECONDS:
                    reload_file_io("refreshing Iceberg storage credentials")
                # Physical rows already checkpointed in this file are skipped on resume.
                rows_emitted = rows_done if file_index == files_done else 0
                checkpointed_rows = rows_emitted
                retried = False
                while True:
                    try:
                        deleted_rows: set[int] = set()
                        for delete_file in task.delete_files:
                            if delete_file.file_path not in decoded_deletes:
                                decoded_deletes[delete_file.file_path] = _delete_positions(table.io, delete_file)
                            positions = decoded_deletes[delete_file.file_path].get(task.file.file_path)
                            if positions is not None:
                                deleted_rows.update(positions.to_pylist())

                        # Apply deletes after reading to preserve physical row offsets.
                        batches = _file_batches(table, task, schema)
                        row_offset = 0
                        try:
                            for batch in batches:
                                documents = []
                                for index in range(batch.num_rows):
                                    row = row_offset + index
                                    if row < rows_emitted or row in deleted_rows:
                                        continue
                                    documents.append(Document.model_validate({
                                        **_serialize_row(batch, index, schema, task.file.file_path, row),
                                        "_meta": {
                                            "snapshot_id": snapshot.snapshot_id,
                                            "file": task.file.file_path,
                                            "row": row,
                                        },
                                    }))
                                yield documents
                                row_offset += batch.num_rows
                                rows_emitted = max(rows_emitted, row_offset)
                                if rows_emitted - checkpointed_rows >= ROW_CHECKPOINT_INTERVAL:
                                    checkpointed_rows = rows_emitted
                                    yield (f"{snapshot.snapshot_id}:{file_index}:{rows_emitted}:{mode}",)
                        finally:
                            batches.close()
                        break
                    except OSError as err:
                        # Catalog-vended storage tokens expire; reload them once and resume this file.
                        if retried or not vended_credentials or "ExpiredToken" not in str(err):
                            raise
                        retried = True
                        reload_file_io("reloading expired Iceberg storage credentials")
                for delete_file in task.delete_files:
                    delete_uses[delete_file.file_path] -= 1
                    if delete_uses[delete_file.file_path] <= 0:
                        decoded_deletes.pop(delete_file.file_path, None)
                if file_index + 1 < len(tasks):
                    yield (f"{snapshot.snapshot_id}:{file_index + 1}:{mode}",)

            yield (str(snapshot.snapshot_id),)
            files_done = rows_done = 0
            full_snapshot = False


async def fetch_table(
    properties: dict[str, str],
    file_io_properties: dict[str, str],
    identifier: Identifier,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[Document | tuple[str], None]:
    """Backfill the current snapshot, checkpointing each fully read file.

    An empty cursor starts the backfill; a bare snapshot ID marks its completion.
    In-flight cursors are snapshot_id:files_done:mode, where mode is full or append.
    Within a large file they are snapshot_id:files_done:rows_done:mode, where rows_done
    counts physical rows of the next file, including deleted ones.
    The mode preserves a full backfill across restarts even for append snapshots.
    """
    assert isinstance(cursor, tuple) and isinstance(cursor[0], str)
    batches = _table_batches(properties, file_io_properties, identifier, cursor[0], log)
    try:
        while True:
            pending = asyncio.create_task(asyncio.to_thread(next, batches, None))
            try:
                batch = await asyncio.shield(pending)
            except asyncio.CancelledError:
                # Let the synchronous read finish before closing its generator.
                await pending
                raise
            if batch is None:
                return
            if isinstance(batch, tuple):
                yield batch
            else:
                for document in batch:
                    yield document
    finally:
        await asyncio.to_thread(batches.close)
