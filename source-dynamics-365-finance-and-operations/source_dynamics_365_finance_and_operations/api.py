import asyncio
from bisect import bisect_right
from datetime import datetime
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
# A transformed CSV row. Keys are the standard Synapse Link metadata
# columns (Id, IsDelete, versionnumber, SinkModifiedOn, _meta) plus
# arbitrary table-specific columns.
TransformedRow = dict[str, str | bool | None | dict[str, str]]


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

    table_metadata = await get_table_metadata(
        timestamp=folder,
        table_name=table_name,
        client=client,
        log=log,
    )

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
