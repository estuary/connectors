import asyncio
from bisect import bisect_right
from datetime import datetime, timedelta, UTC
from enum import StrEnum
from logging import Logger
from typing import AsyncGenerator, AsyncIterator, Callable, Literal, cast, overload

import orjson

# Some functions that send a request to Azure for listing files or reading
# metadata files are wrapped with the alru_cache decorator. alru_cache
# prevents the connector from making multiple HTTP requests for the same
# data. alru_cache also addresses the thundering herd problem when there's
# a cache miss; simultaneous calls to the same function with the same parameters
# will share the same future.
from async_lru import alru_cache
from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPError

from .adls_gen2_client import ADLSGen2Client, ADLSPathMetadata
from .models import (
    AttributeDataType,
    ModelDotJson,
    TableMetadata,
)
from .shared import call_with_cache_logging, is_datetime_format, str_to_dt


MINIMUM_AZURE_SYNAPSE_LINK_EXPORT_INTERVAL = 300             # 5 minutes
# CACHE_TTL is shorter than the minimum export interval allowed by Azure
# in order to minimize how long stale data remains in the cache while
# still picking up on any changes in Azure relatively quickly.
CACHE_TTL = MINIMUM_AZURE_SYNAPSE_LINK_EXPORT_INTERVAL / 5   # 1 minute
# FOLDER_PROCESSING_SEMAPHORE is used to bound how many timestamp
# folders are processed concurrently. Processing an unbounded number
# of folders can easily trigger the connector to exceed its memory
# limit and get OOM killed. 5 was chosen to ensure large tables
# with a massive number of changes only have to compete with a few
# other streams for CPU time and can finish processing the contents
# of a timestamp folder in a reasonable amount of time.
FOLDER_PROCESSING_SEMAPHORE = asyncio.Semaphore(5)
# A timestamp folder's model.json is written when the folder finalizes at the
# end of its export interval, so it can lags behind the folder's CSV data. Retries
# can also abandon a folder that has CSV data but never gets a model.json,
# re-committing those changes into a later folder. SETTLE_DELAY is how long we
# wait after a folder should have finalized before treating a missing
# model.json as an abandoned folder we skip rather than one still being
# written.
SETTLE_DELAY = timedelta(hours=1)
# A transformed CSV row. Keys are the standard Synapse Link metadata
# columns (Id, IsDelete, versionnumber, SinkModifiedOn, _meta) plus
# arbitrary table-specific columns.
TransformedRow = dict[str, str | bool | None | dict[str, str]]


# Alongside the folder level model.json, D365's Synapse Link export (via its
# TrickleFeedService) writes a per-table model.json at
# {timestamp}/{TRICKLE_FEED_SERVICE_DIR}/{table}-model.json. It has the same
# shape as the folder level model.json but describes a single table, and serves
# as an fallback when the folder level model.json was written but
# truncated (see get_table_metadata).
TRICKLE_FEED_SERVICE_DIR = "Microsoft.Athena.TrickleFeedService"

# Flags a row as a deletion, and terminates the block of columns that every
# exported row carries. Columns after it were appended to the table later, so
# its position also marks how narrow a row may legitimately be.
IS_DELETE = "IsDelete"


class ModelFormat(StrEnum):
    BYTES = "bytes"
    PYDANTIC = "pydantic"


class TableSchemaUnavailableError(Exception):
    """Raised when a timestamp folder has CSV data for a table but no usable
    schema to read it with. Observed causes, all suspected to be due to
    aborted/interrupted Synapse Link exports, include:

    - the folder level model.json is absent (404),
    - the folder level model.json is present but lists no entities, or
    - neither the folder level model.json (which can be truncated, dropping a
      lexicographically-ordered tail of entities) nor the table's per-table
      model.json describes the table.
    """

    def __init__(self, folder: str, detail: str):
        self.folder = folder
        self.detail = detail
        super().__init__(
            f"No usable schema for the table in timestamp folder {folder}: {detail}."
        )


class RowSchemaMismatchError(Exception):
    """Raised when a CSV row's cells cannot be bound to the field names
    declared for its table.

    Synapse Link CSVs are headerless, so cells are bound to names by position
    and a row carries no evidence of which column any given cell belongs to.
    Misalignment therefore can't be detected directly; it can only be inferred
    from signals that a correctly aligned row would never produce:

    - a width outside the range a row may legitimately have (see bind_row),
    - a width narrower than an earlier row in the same file (see read_csv_rows),
    - a narrow row in a table whose columns stopped being append-only (see
      TableSchemaHistory and read_csv_rows).

    The first two are width signals, which is all a headerless row offers.
    Both rest on Synapse Link only ever appending columns to the end of a row.
    The third check watches that assumption as the capture runs rather than
    inferring from the rows, and withdraws permission to bind narrow rows
    once that assumption stops holding.

    Rows merely narrower than the schema because they predate an appended
    column are expected and do not raise.
    """

    def __init__(self, csv_name: str, row_number: int, detail: str):
        self.csv_name = csv_name
        self.row_number = row_number
        self.detail = detail
        super().__init__(f"Cannot read row {row_number} of {csv_name}: {detail}.")


# model.json metadata files are not updated after they're written.
# So there's no need to expire cache results with a TTL to ensure
# we capture updates to these files.
@alru_cache(maxsize=32, ttl=None)
async def _fetch_model_dot_json_bytes(
    client: ADLSGen2Client,
    directory: str | None = None,
) -> bytes:
    path = f"{directory}/model.json" if directory else "model.json"
    return await client.read_file(path)


@overload
async def fetch_model_dot_json(
    client: ADLSGen2Client,
    log: Logger,
    directory: str | None = None,
    *,
    format: Literal[ModelFormat.BYTES] = ...,
) -> bytes: ...


@overload
async def fetch_model_dot_json(
    client: ADLSGen2Client,
    log: Logger,
    directory: str | None = None,
    *,
    format: Literal[ModelFormat.PYDANTIC],
) -> ModelDotJson: ...


async def fetch_model_dot_json(
    client: ADLSGen2Client,
    log: Logger,
    directory: str | None = None,
    *,
    format: ModelFormat = ModelFormat.BYTES,
) -> bytes | ModelDotJson:
    raw_bytes = await call_with_cache_logging(
        _fetch_model_dot_json_bytes, log, client, directory
    )

    if format == ModelFormat.PYDANTIC:
        return ModelDotJson.model_validate_json(raw_bytes)
    return raw_bytes


def _find_entity(entities: list[dict], table_name: str) -> dict | None:
    return next((e for e in entities if e["name"] == table_name), None)


def _table_metadata_from_entity(entity: dict) -> TableMetadata:
    field_names = [attr["name"] for attr in entity["attributes"]]

    # Every table Synapse Link exports carries a boolean IsDelete, and the
    # connector depends on it. It determines each row's operation and orders
    # upserts ahead of deletes within a folder. Both checks fail loudly here
    # rather than downstream.
    is_delete_attr = next(
        (attr for attr in entity["attributes"] if attr["name"] == IS_DELETE), None
    )

    if is_delete_attr is None:
        raise ValueError(
            f"Table {entity['name']} has no {IS_DELETE} column in its model.json "
            f"entity. Every Synapse Link table is expected to have one, and the "
            f"connector cannot determine a row's operation without it."
        )

    if is_delete_attr["dataType"] != AttributeDataType.BOOLEAN:
        raise ValueError(
            f"Table {entity['name']} declares its {IS_DELETE} column as "
            f"{is_delete_attr['dataType']!r} rather than 'boolean' in its "
            f"model.json entity. The connector cannot determine a row's operation "
            f"unless {IS_DELETE} is converted to a boolean."
        )

    is_delete_index = field_names.index(IS_DELETE)

    return TableMetadata(
        name=entity["name"],
        field_names=field_names,
        boolean_fields=frozenset(
            attr["name"] for attr in entity["attributes"]
            if attr["dataType"] == AttributeDataType.BOOLEAN
        ),
        min_row_width=is_delete_index + 1,
    )


async def get_table_metadata(
    timestamp: str,
    table_name: str,
    client: ADLSGen2Client,
    log: Logger,
) -> TableMetadata:
    raw_bytes = await fetch_model_dot_json(client, log, timestamp)
    entities = orjson.loads(raw_bytes).get("entities") or []

    entity = _find_entity(entities, table_name)
    if entity is not None:
        return _table_metadata_from_entity(entity)

    if not entities:
        # We haven't observed the case where the folder level model.json
        # doesn't have _any_ entities present, but I'm not confident it
        # would be safe to fallback to the per-table model.json under
        # TRICKLE_FEED_SERVICE_DIR in that situation. Raise so we can
        # investigate if this happens.
        raise TableSchemaUnavailableError(
            timestamp, "folder level model.json lists no entities"
        )

    # The model.json lists other entities but not this one. It was written but
    # truncated (entities are emitted in lexicographic order, and an aborted
    # export can cut the list off partway through). The folder did finalize its
    # schemas, so fall back to the per-table model.json.
    return await _get_table_metadata_from_per_table_model_dot_json(
        timestamp, table_name, client, log
    )


async def _get_table_metadata_from_per_table_model_dot_json(
    timestamp: str,
    table_name: str,
    client: ADLSGen2Client,
    log: Logger,
) -> TableMetadata:
    path = f"{timestamp}/{TRICKLE_FEED_SERVICE_DIR}/{table_name}-model.json"
    try:
        raw_bytes = await client.read_file(path)
    except HTTPError as err:
        if err.code == 404:
            raise TableSchemaUnavailableError(
                timestamp,
                f"folder level model.json omits table {table_name} and no "
                f"per-table model.json exists for it",
            ) from err
        raise

    entity = _find_entity(orjson.loads(raw_bytes).get("entities") or [], table_name)
    if entity is None:
        raise TableSchemaUnavailableError(
            timestamp, f"per-table model.json for {table_name} does not describe the table"
        )

    log.info(
        "The folder level model.json omitted this table. Reading its CSVs with the "
        "per-table model.json instead.",
        {"folder": timestamp, "table": table_name, "path": path},
    )

    return _table_metadata_from_entity(entity)


async def get_in_progress_timestamp_folder(
    client: ADLSGen2Client,
) -> str:
    response = await client.read_file("Changelog/changelog.info")

    return response.decode('utf-8')


async def get_timestamp_folders(
    client: ADLSGen2Client,
) -> list[str]:
    timestamp_folders: list[str] = []

    async for path in client.list_paths():
        if path.isDirectory and is_datetime_format(path.name):
            timestamp_folders.append(path.name)

    sorted_timestamp_folders = sorted(timestamp_folders, key=str_to_dt)

    client.log.debug("Found timestamp folders", {
        "timestamp folders": sorted_timestamp_folders
    })

    return sorted_timestamp_folders


@alru_cache(maxsize=1, ttl=CACHE_TTL)
async def get_finalized_timestamp_folders(
    client: ADLSGen2Client,
) -> list[str]:
    folders = await get_timestamp_folders(client)
    in_progress_folder = await get_in_progress_timestamp_folder(client)

    finalized_folders = [
        # Do not return the in progress folder. Its model.json is
        # empty, meaning we don't know the tables' final schemas,
        # and data is still being written to it.
        folder for folder in folders if str_to_dt(folder) < str_to_dt(in_progress_folder)
    ]

    return sorted(finalized_folders, key=str_to_dt)


async def get_folder_contents_for_table(
    folder: str,
    table_name: str,
    client: ADLSGen2Client,
) -> list[ADLSPathMetadata]:
    metadata: list[ADLSPathMetadata] = []

    # The {folder}/{table_name} path will only exist if data exists for
    # table_name within the folder. If no data exists for this table,
    # we'll receive a 404 response. That's ok - it just means there were
    # no changes to that table in the timespan covered by the folder.
    path = f"{folder}/{table_name}"

    try:
        async for m in client.list_paths(
            directory=path,
            recursive=True
        ):
            metadata.append(m)
    except HTTPError as err:
        if err.code == 404 and "The specified path does not exist" in err.message:
            pass
        else:
            raise err

    return metadata


async def _prepend[T](first: T, rest: AsyncGenerator[T, None]) -> AsyncGenerator[T, None]:
    """Yield first, then everything from rest."""
    yield first
    async for item in rest:
        yield item


async def _read_upsert_file(
    rows: AsyncGenerator[TransformedRow, None],
    csv_name: str,
) -> AsyncGenerator[TransformedRow, None]:
    """Yield rows, raising if any is a delete."""
    async for row in rows:
        if row[IS_DELETE] is True:
            raise RuntimeError(
                f"{csv_name} contains a delete row after upserts. "
                f"Each CSV must contain only deletes or non-deletes."
            )
        yield row


async def _read_delete_file(
    rows: AsyncGenerator[TransformedRow, None],
    csv_name: str,
) -> AsyncGenerator[TransformedRow, None]:
    """Yield rows, raising if any is not a delete (file was classified as deletes)."""
    async for row in rows:
        if row[IS_DELETE] is not True:
            raise RuntimeError(
                f"{csv_name} contains a non-delete row after deletes. "
                f"Each CSV must contain only deletes or non-deletes."
            )
        yield row


async def stream_folder_rows(
    csvs: list[ADLSPathMetadata],
    open_csv: Callable[[ADLSPathMetadata], AsyncGenerator[TransformedRow, None]],
) -> AsyncGenerator[TransformedRow, None]:
    """
    Yield rows from a (folder, table) pair in deferred-delete order.

    CSVs are mtime-sorted, but mtime isn't necessarily commit order;
    a delete may appear in a file modified before the upsert it should
    follow. Each CSV is assumed homogeneous - either all upserts or all
    deletes. Based on that assumption, the strategy is:

        Pass 1: stream upsert files immediately; defer files containing deletes.
        Pass 2: stream the deletes in the deferred files.

    Reading every upsert file before any delete file ensures the
    destination's last-write-wins reduce on Id resolves to the deleted
    state when an Id is both upserted and deleted within a folder.

    Raises RuntimeError if the file-homogeneity assumption is violated.

    Args:
        csvs: List of CSVs to process.
        open_csv: Returns a fresh row stream per call; pass 2 re-opens
            deferred CSVs, so this must be a factory.
    """
    csvs = sorted(csvs, key=lambda c: c.last_modified_datetime)

    deferred_csvs: list[ADLSPathMetadata] = []

    # Pass 1: Stream upsert files. Defer reading files whose first row is a delete.
    for csv in csvs:
        stream = open_csv(csv)
        try:
            first_row = await stream.__anext__()
        except StopAsyncIteration:
            continue

        if first_row[IS_DELETE] is True:
            deferred_csvs.append(csv)
            await stream.aclose()
            continue

        async for row in _read_upsert_file(_prepend(first_row, stream), csv.name):
            yield row

    # Pass 2: Stream deletes in deferred files.
    for csv in deferred_csvs:
        async for row in _read_delete_file(open_csv(csv), csv.name):
            yield row


class TableSchemaHistory:
    """Remembers the schema last seen for a table so consecutive folders can be
    compared.

    Binding a row narrower than its folder's model.json relies on Synapse Link
    only ever appending columns: that is what makes the narrow row a prefix of
    the declared schema rather than a misaligned one. This watches that
    property hold as the capture runs. Each folder's schema must begin with the
    whole of the previously seen one, which an append satisfies and an
    insertion, reorder or removal does not - all three move a column to a
    different position, and position is all that binding has to go on.

    Comparing against the last schema *seen* rather than the immediately
    preceding folder is fine: a table only appears in folders where it changed,
    and the prefix relation is transitive, so skipped folders cannot hide a
    violation.

    This only observes changes that happen while the connector is running.
    A schema that stopped being append-only before the current cursor is
    already reflected in both schemas being compared, so it goes unnoticed.

    Note this assumes the folder level model.json and the per-table one under
    TRICKLE_FEED_SERVICE_DIR list a table's attributes in the same order, since
    get_table_metadata falls back to the latter. If they ever disagree, the
    fallback looks like a reorder, and narrow rows are refused in two folders
    rather than one: the folder using the fallback, and the folder after it,
    which is compared against the fallback's order once it becomes the
    baseline.
    """

    def __init__(self) -> None:
        self._previous: list[str] | None = None
        self._previous_folder: str | None = None

    def observe(self, folder: str, field_names: list[str]) -> str | None:
        """Record this folder's schema, returning a description of how it
        breaks the append-only property, or None if it upholds it."""
        previous, previous_folder = self._previous, self._previous_folder
        self._previous = field_names
        self._previous_folder = folder

        if previous is None or field_names[:len(previous)] == previous:
            return None

        divergence = next(
            (i for i, (a, b) in enumerate(zip(previous, field_names)) if a != b),
            min(len(previous), len(field_names)),
        )
        now = (
            repr(field_names[divergence])
            if divergence < len(field_names)
            else "nothing - the column is gone"
        )

        return (
            f"the schema in {previous_folder} had {len(previous)} columns and this "
            f"folder's has {len(field_names)}, but column {divergence} changed from "
            f"{previous[divergence]!r} to {now}. Columns are expected to only ever "
            f"be appended to the end, which would leave the earlier schema intact "
            f"as a prefix of this one"
        )


async def read_csvs_in_folder(
    folder: str,
    table_name: str,
    client: ADLSGen2Client,
    log: Logger,
    schema_history: TableSchemaHistory,
) -> AsyncGenerator[TransformedRow, None]:
    folder_contents = await get_folder_contents_for_table(folder, table_name, client)

    csvs: list[ADLSPathMetadata] = []

    for metadata in folder_contents:
        if (
            not metadata.isDirectory and
            metadata.name.endswith('.csv') and
            metadata.name.startswith(f"{folder}/{table_name}/")
        ):
            csvs.append(metadata)

    if not csvs:
        return

    # The folder has CSV data for this table but its schema is
    # fetched separately. A 404 means the folder level model.json is missing
    # entirely. get_table_metadata raises TableSchemaUnavailableError for
    # the other unusable-schema cases. We surface it so fetch_changes can decide
    # whether to wait for finalization or skip the incomplete folder.
    try:
        table_metadata = await get_table_metadata(
            timestamp=folder,
            table_name=table_name,
            client=client,
            log=log,
        )
    except HTTPError as err:
        if err.code == 404:
            raise TableSchemaUnavailableError(
                folder, "folder level model.json is missing"
            ) from err
        raise

    # Reported rather than raised here, because a schema can break the
    # append-only property without any row being read wrongly - a rename shifts
    # nothing, so every row still binds to the right positions. It only becomes
    # fatal where the property is actually depended on, which is why it is
    # handed to read_csv_rows rather than acted on now.
    schema_violation = schema_history.observe(folder, table_metadata.field_names)

    if schema_violation is not None:
        log.warning(
            "This table's columns changed in a way other than being appended to. "
            "Rows narrower than the schema cannot be read while that holds, since "
            "they may no longer line up with it.",
            {"folder": folder, "table": table_name, "detail": schema_violation},
        )

    def open_csv(csv: ADLSPathMetadata) -> AsyncGenerator[TransformedRow, None]:
        return read_csv_rows(
            client.stream_csv(csv.name), table_metadata, csv.name, log,
            schema_violation,
        )

    async for row in stream_folder_rows(csvs, open_csv):
        yield row


async def read_csv_rows(
    rows: AsyncIterator[list[str]],
    table_metadata: TableMetadata,
    csv_name: str,
    log: Logger,
    schema_violation: str | None,
) -> AsyncGenerator[TransformedRow, None]:
    """
    Bind and transform one CSV's rows, enforcing the invariants that hold
    across a file rather than within a single row.

    `schema_violation` describes how this table's columns changed other than by
    being appended to, if TableSchemaHistory saw that happen, and is None
    otherwise. A narrow row is only safe to bind as a prefix while columns are
    append-only, so its presence makes narrow rows an error rather than the
    expected consequence of an append. Rows at the full width are unaffected -
    they line up with the schema whatever changed.

    Row widths within a file may only increase. Rows are appended in the order
    Synapse Link writes them to the lake, and the schema change that adds a
    column is observed by the exporter, so every row written before an append
    precedes every row written after it. A row narrower than one already seen
    therefore did not come from an append.

    Note the ordering that matters here is the export's, not the source's.
    SinkModifiedOn is non-decreasing across a whole file. versionnumber is
    non-decreasing only within a single Id - across Ids it carries no ordering,
    and adjacent rows have been observed going backwards - so it cannot stand
    in for write order here, however well it orders changes to one row.

    This is what distinguishes an old row from a truncated one, which bind_row
    alone cannot do: an export interrupted mid-write leaves a final row cut
    short, and its width can easily land within the range bind_row accepts.
    Such a row would otherwise be captured with a partial value in its last
    column and its remaining columns silently dropped.

    Row numbers in errors count data rows from 1. They are not line numbers -
    blank lines are skipped upstream and quoted values may span lines.
    """
    declared_columns = len(table_metadata.field_names)
    widest_row_seen = 0
    logged_narrow_row = False
    row_number = 0

    async for values in rows:
        row_number += 1

        if len(values) < widest_row_seen:
            raise RowSchemaMismatchError(
                csv_name,
                row_number,
                f"row has {len(values)} columns after an earlier row in the same "
                f"file had {widest_row_seen}. Column counts within a CSV only ever "
                f"increase, as Synapse Link appends columns added to the table, so "
                f"this row did not come from such an append. An export interrupted "
                f"mid-write would leave a final row cut short like this; so would a "
                f"column being removed from the table partway through the file",
            )

        widest_row_seen = len(values)

        row = bind_row(values, table_metadata, csv_name, row_number)

        if len(values) < declared_columns and schema_violation is not None:
            raise RowSchemaMismatchError(
                csv_name,
                row_number,
                f"row has {len(values)} of this table's {declared_columns} columns, "
                f"which would normally mean it predates the columns appended since "
                f"it was written. That cannot be assumed here: {schema_violation}. "
                f"Binding the row would risk reading its values under the wrong "
                f"column names",
            )

        if not logged_narrow_row and len(values) < declared_columns:
            logged_narrow_row = True
            log.info(
                "CSV contains rows written before columns were appended to "
                "this table. Those columns are absent from such rows.",
                {
                    "csv": csv_name,
                    "table": table_metadata.name,
                    "row columns": len(values),
                    "declared columns": declared_columns,
                    "absent columns": table_metadata.field_names[len(values):],
                },
            )

        yield transform_row(row, table_metadata.boolean_fields, csv_name)


def bind_row(
    values: list[str],
    table_metadata: TableMetadata,
    csv_name: str,
    row_number: int,
) -> dict[str, str | None]:
    """
    Bind a headerless CSV row's cells to their field names by position.

    Synapse Link appends columns added to a table after its initial export to
    the end of every row, past the standard metadata block that ends with
    IsDelete. A CSV can be written across such an append - rows written before
    it are narrower than rows written after, within the same file - while the
    folder's model.json describes only the schema as of finalization. Those
    narrower rows are a prefix of the declared schema, so their cells still
    bind correctly and the appended columns are left absent rather than
    fabricated as null.

    Any other width means the positions no longer line up: a row that stops
    inside the standard block is misaligned, and a row with more cells than the
    schema names has data we cannot label. Both raise.

    Width is the only signal available here, so this cannot prove alignment.
    A column inserted before IsDelete rather than appended after it would
    leave an older row inside the accepted width range while shifting every
    cell past the insertion point, and would be bound without complaint. See
    RowSchemaMismatchError for why that is an accepted risk.

    Empty cells are converted to None.
    """
    field_names = table_metadata.field_names

    if len(values) > len(field_names):
        raise RowSchemaMismatchError(
            csv_name,
            row_number,
            f"row has {len(values)} columns but this folder's model.json declares "
            f"only {len(field_names)} for {table_metadata.name}, leaving "
            f"{len(values) - len(field_names)} with no field name to bind to",
        )

    if len(values) < table_metadata.min_row_width:
        raise RowSchemaMismatchError(
            csv_name,
            row_number,
            f"row has {len(values)} columns, but every {table_metadata.name} row is "
            f"at least {table_metadata.min_row_width} columns wide - the columns up "
            f"to and including {IS_DELETE}. Only columns appended after {IS_DELETE} "
            f"may be absent from a row, so this row cannot be aligned to the "
            f"{len(field_names)} columns declared in this folder's model.json",
        )

    # zip stops at the shorter of the two, so a row predating an appended
    # column simply leaves that column's name unbound.
    row = {
        name: value if value != "" else None
        for name, value in zip(field_names, values)
    }

    return row


def transform_row(row: dict[str, str | None], boolean_fields: frozenset[str], csv_name: str) -> TransformedRow:
    """
    Apply Dynamics 365-specific transformations to a CSV row.

    Transformations:
    - Convert boolean fields from "True"/"False"/empty strings to actual booleans
    - Add _meta field with operation type based on IsDelete field
      (IsDelete is "True" for deletions, "" otherwise)
    """
    result = cast(TransformedRow, row)

    for field_name in boolean_fields:
        # A row predating an appended column has no cell for it. Leave the
        # column absent instead of fabricating a False for it.
        if field_name not in row:
            continue
        value = row[field_name]
        result[field_name] = value.lower() == "true" if value else False

    result["_meta"] = {
        "op": "d" if result.get(IS_DELETE) else "u",
        "source_file": csv_name,
    }

    return result


def should_wait_for_finalization(next_folder: str, now: datetime) -> bool:
    """Decide whether to keep waiting for a folder that has table data but no
    model.json to finalize (True) rather than treating it as incomplete and
    skipping it (False).

    `next_folder` is the chronological successor of the folder in question.
    Its creation time is when the folder in question stopped being written,
    since one folder closes as the next one opens. We give the folder
    SETTLE_DELAY after it closed for its model.json to appear before treating
    a still-missing model.json as an abandoned folder.

        folder created     folder closes (successor created)      +SETTLE_DELAY
           |------ folder is being written ------|------ grace period ------|
                                                 ^                          ^
                                                 model.json should          if still no
                                                 appear around here         model.json, skip

    We can't measure this from the folder's own timestamp because that's when
    it was created (started), not when it closed, and the interval length is
    configurable, so the successor's creation time is what tells us when the
    folder closed. This basis is interval-agnostic: an old abandoned folder is
    skipped immediately, while only a folder that closed recently gets the
    benefit of the doubt.
    """
    return now - str_to_dt(next_folder) < SETTLE_DELAY


async def fetch_changes(
    client: ADLSGen2Client,
    table_name: str,
    schema_history: TableSchemaHistory,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[dict | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    finalized_folders = await call_with_cache_logging(
        get_finalized_timestamp_folders, log, client
    )

    # Use binary search to find the first folder after log_cursor,
    # skipping folders we've already read on previous sweeps.
    start_index = bisect_right(finalized_folders, log_cursor, key=str_to_dt)

    for index in range(start_index, len(finalized_folders)):
        folder = finalized_folders[index]
        async with FOLDER_PROCESSING_SEMAPHORE:
            log.debug(f"Reading CSVs in {folder}/{table_name}.")
            try:
                async for row in read_csvs_in_folder(
                    folder, table_name, client, log, schema_history
                ):
                    yield row
            except TableSchemaUnavailableError as err:
                # The folder has table data but no usable schema for this table.
                # This folder should be finalized when the next folder is created,
                # and we use the next folder to determine if SETTLE_DELAY has elapsed.
                # If so, then we assume the current folder's export was interrupted/aborted
                # by D365 & the changes shouldn't be replicated.
                if index + 1 < len(finalized_folders):
                    next_folder = finalized_folders[index + 1]
                else:
                    next_folder = await get_in_progress_timestamp_folder(client)

                if should_wait_for_finalization(next_folder, datetime.now(tz=UTC)):
                    log.info(
                        "Timestamp folder has table data but no usable schema yet "
                        "and it may still be finalizing. Will retry on the next sweep.",
                        {"folder": folder, "table": table_name, "detail": err.detail},
                    )
                    return

                # The folder had ample time to finalize but still has no usable
                # schema, so it's an incomplete/abandoned folder. Skip it and
                # advance past it. Its changes are expected to be re-committed in
                # a later folder.
                log.warning(
                    "Skipping timestamp folder with table data but no usable schema. "
                    "The folder appears incomplete and was never finalized. Its changes "
                    "are expected to be re-committed in a later folder.",
                    {"folder": folder, "table": table_name, "detail": err.detail},
                )

            log.debug(f"Read all CSVs in folder. Yielding folder name as new cursor.", {
                "folder": str_to_dt(folder),
            })
            yield str_to_dt(folder)
