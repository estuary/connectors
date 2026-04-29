import asyncio
from bisect import bisect_right
from datetime import datetime
from enum import StrEnum
from logging import Logger
from typing import AsyncGenerator, Literal, cast, overload

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
# A transformed CSV row. Keys are the standard Synapse Link metadata
# columns (Id, IsDelete, versionnumber, SinkModifiedOn, _meta) plus
# arbitrary table-specific columns.
TransformedRow = dict[str, str | bool | None | dict[str, str]]
# SinkModifiedOn arrives as a US-locale string like "4/29/2026 9:01:26 PM".
# We parse it only internally as a tiebreaker for order_key.
_SINK_MODIFIED_ON_FORMAT = "%m/%d/%Y %I:%M:%S %p"


class ModelFormat(StrEnum):
    BYTES = "bytes"
    PYDANTIC = "pydantic"


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


async def get_table_metadata(
    timestamp: str,
    table_name: str,
    client: ADLSGen2Client,
    log: Logger,
) -> TableMetadata:
    raw_bytes = await fetch_model_dot_json(client, log, timestamp)
    data = orjson.loads(raw_bytes)

    for entity in data["entities"]:
        if entity["name"] == table_name:
            return TableMetadata(
                name=entity["name"],
                field_names=[attr["name"] for attr in entity["attributes"]],
                boolean_fields=frozenset(
                    attr["name"] for attr in entity["attributes"]
                    if attr["dataType"] == "boolean"
                ),
            )

    raise KeyError(f"Table {table_name} not found in timestamp folder {timestamp}.")


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

    return sorted(timestamp_folders, key=str_to_dt)


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


def order_key(row: TransformedRow) -> tuple[int, datetime]:
    """
    Build the comparison key used by the per-folder ordering state machine.

    versionnumber is the source-of-truth ordering primitive. SinkModifiedOn
    is a defensive tiebreaker for the unexpected case where two rows for
    the same Id share a versionnumber.

    Both fields are required on every Synapse Link CSV row.
    """
    # versionnumber and SinkModifiedOn pass straight through transform_row,
    # so values here are guaranteed to be str | None from client.stream_csv.
    version = cast(str | None, row["versionnumber"])
    sink_modified_on = cast(str | None, row["SinkModifiedOn"])
    if version is None or sink_modified_on is None:
        raise ValueError(
            f"Row is missing required ordering fields "
            f"(versionnumber={version!r}, SinkModifiedOn={sink_modified_on!r}). "
        )
    return (int(version), datetime.strptime(sink_modified_on, _SINK_MODIFIED_ON_FORMAT))


async def defer_deletes(rows: AsyncGenerator[TransformedRow, None]) -> AsyncGenerator[TransformedRow, None]:
    """
    Buffer deletes from a single (folder, table) pair and emit them after
    all upserts, so the destination ends up with the correct latest state
    for each Id.

    Rows in a folder don't arrive in versionnumber order. CSVs are sorted
    by file modification time, which only loosely tracks the source commit
    sequence. A delete might appear in an earlier-modified file than the
    upsert that should follow it. So we can't just emit rows as they come.

    For each Id we track two things:

      - The highest versionnumber of any upsert we've already emitted for
        this Id in this folder.
      - The highest-versionnumber delete we've seen for this Id, held
        until the end of the folder.

    Upserts emit immediately, unless an upsert with a higher versionnumber
    for the same Id has already gone out (in which case the incoming one
    is stale). Deletes are buffered. At end-of-folder, a buffered delete
    emits only if no upsert with a higher versionnumber for that Id was
    seen during streaming.

    State is reset per call. A delete in folder N+1 against an upsert from
    folder N must still emit, and starting each call with empty maps
    ensures it does. (Synapse Link gives us monotonically-increasing
    versionnumbers across folders, so this is safe.)

    Tradeoff: if a delete and a later recreate for the same Id appear in
    the same folder, the delete is coalesced away. The destination ends
    up in the right state, but the connector does not produce a faithful
    event log.
    """
    # latest_upsert_key tracks the highest order_key for non-deletion events.
    latest_upsert_key: dict[str, tuple[int, datetime]] = {}
    # pending_deletes buffers all deletions. It gets drained after yielding
    # all non-deletions.
    pending_deletes: dict[str, TransformedRow] = {}

    # Yield all non-deletions as they're read.
    async for row in rows:
        row_id = cast(str, row["Id"])
        row_key = order_key(row)

        # Defer yielding deletions until all non-deletions have been yielded.
        if row["IsDelete"] is True:
            prior_delete = pending_deletes.get(row_id)
            if prior_delete is None or row_key > order_key(prior_delete):
                pending_deletes[row_id] = row
            continue

        prior_key = latest_upsert_key.get(row_id)
        if prior_key is not None and prior_key >= row_key:
            # Drop stale upserts so the cache stays an accurate high-water mark.
            continue

        yield row
        latest_upsert_key[row_id] = row_key

    for row_id, delete_row in pending_deletes.items():
        prior_key = latest_upsert_key.get(row_id)
        if prior_key is not None and prior_key >= order_key(delete_row):
            continue
        yield delete_row


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

    table_metadata = await get_table_metadata(
        timestamp=folder,
        table_name=table_name,
        client=client,
        log=log,
    )

    # Sorting by last_modified_datetime should ensure that all create/update events
    # are sorted in modification order. But deletions live in separate files whose last_modified_datetime can
    # precede the files containing creates/updates, so a delete might come up before
    # the upsert it should follow. defer_deletes defers emitting deletions until
    # we've processed all non-deletions in this folder.
    csvs.sort(key=lambda c: c.last_modified_datetime)

    async def transformed_rows() -> AsyncGenerator[TransformedRow, None]:
        for csv in csvs:
            async for raw_row in client.stream_csv(csv.name, table_metadata.field_names):
                yield transform_row(raw_row, table_metadata.boolean_fields, csv.name)

    async for row in defer_deletes(transformed_rows()):
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

    for folder in finalized_folders[start_index:]:
        async with FOLDER_PROCESSING_SEMAPHORE:
            log.debug(f"Reading CSVs in {folder}/{table_name}.")
            async for row in read_csvs_in_folder(folder, table_name, client, log):
                yield row

            log.debug(f"Read all CSVs in folder. Yielding folder name as new cursor.", {
                "folder": str_to_dt(folder),
            })
            yield str_to_dt(folder)
