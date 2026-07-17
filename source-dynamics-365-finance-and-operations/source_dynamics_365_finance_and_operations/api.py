import asyncio
from bisect import bisect_right
from datetime import datetime, timedelta, UTC
from enum import StrEnum
from logging import Logger
from typing import AsyncGenerator, Callable, Literal, cast, overload

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
    return TableMetadata(
        name=entity["name"],
        field_names=[attr["name"] for attr in entity["attributes"]],
        boolean_fields=frozenset(
            attr["name"] for attr in entity["attributes"]
            if attr["dataType"] == "boolean"
        ),
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
        if row["IsDelete"] is True:
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
        if row["IsDelete"] is not True:
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

        if first_row["IsDelete"] is True:
            deferred_csvs.append(csv)
            await stream.aclose()
            continue

        async for row in _read_upsert_file(_prepend(first_row, stream), csv.name):
            yield row

    # Pass 2: Stream deletes in deferred files.
    for csv in deferred_csvs:
        async for row in _read_delete_file(open_csv(csv), csv.name):
            yield row


async def read_csvs_in_folder(
    folder: str,
    table_name: str,
    client: ADLSGen2Client,
    log: Logger,
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

    def open_csv(csv: ADLSPathMetadata) -> AsyncGenerator[TransformedRow, None]:
        async def gen() -> AsyncGenerator[TransformedRow, None]:
            async for raw_row in client.stream_csv(csv.name, table_metadata.field_names):
                yield transform_row(raw_row, table_metadata.boolean_fields, csv.name)
        return gen()

    async for row in stream_folder_rows(csvs, open_csv):
        yield row


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
        value = row.get(field_name)
        result[field_name] = value.lower() == "true" if value else False

    result["_meta"] = {
        "op": "d" if result.get("IsDelete") else "u",
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
                async for row in read_csvs_in_folder(folder, table_name, client, log):
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
