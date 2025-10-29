from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

# Any functions that send a request to Azure for listing files or reading
# metadata files are wrapped with the alru_cache decorator. alru_cache
# prevents the connector from making multiple HTTP requests for the same
# data. alru_cache also addresses the thundering herd problem when there's
# a cache miss; simultaneous calls to the same function with the same parameters
# will share the same future.
from async_lru import alru_cache
from estuary_cdk.capture.common import LogCursor

from .adls_gen2_client import ADLSGen2Client, ADLSPathMetadata
from .models import (
    BaseTable,
    ModelDotJson,
    tables_from_model_dot_json,
)
from .shared import is_datetime_format, str_to_dt


MINIMUM_AZURE_SYNAPSE_LINK_EXPORT_INTERVAL = 300             # 5 minutes
# CACHE_TTL is shorter than the minimum export interval allowed by Azure
# in order to minimize how long stale data remains in the cache while
# still picking up on any changes in Azure relatively quickly.
CACHE_TTL = MINIMUM_AZURE_SYNAPSE_LINK_EXPORT_INTERVAL / 5   # 1 minute


# model.json metadata files are not updated after they're written.
# So there's no need to expire cache results with a TTL to ensure
# we capture updates to these files.
@alru_cache(maxsize=32, ttl=None)
async def fetch_model_dot_json(
    client: ADLSGen2Client,
    directory: str | None = None,
) -> ModelDotJson:
    if directory:
        path = f"{directory}/model.json"
    else:
        path = "model.json"

    model_dot_json = ModelDotJson.model_validate_json(
        await client.read_file(path)
    )

    return model_dot_json


async def get_table(
    timestamp: str,
    table_name: str,
    client: ADLSGen2Client,
) -> type[BaseTable]:
    model_dot_json = await fetch_model_dot_json(client, timestamp)
    tables = tables_from_model_dot_json(model_dot_json)

    for table in tables:
        if table.name == table_name:
            return table

    raise KeyError(f"Table {table_name} not found for in timestamp folder {timestamp}.")


@alru_cache(maxsize=1, ttl=CACHE_TTL)
async def get_in_progress_timestamp_folder(
    client: ADLSGen2Client,
) -> str:
    response = await client.read_file("Changelog/changelog.info")

    return response.decode('utf-8')


@alru_cache(maxsize=1, ttl=CACHE_TTL)
async def get_timestamp_folders(
    client: ADLSGen2Client,
) -> list[str]:
    timestamp_folders: list[str] = []

    async for path in client.list_paths():
        if path.isDirectory and is_datetime_format(path.name):
            timestamp_folders.append(path.name)

    return sorted(timestamp_folders, key=str_to_dt)


@alru_cache(maxsize=32, ttl=CACHE_TTL)
async def get_folder_contents(
    folder: str,
    client: ADLSGen2Client,
) -> list[ADLSPathMetadata]:
    metadata: list[ADLSPathMetadata] = []

    async for m in client.list_paths(
        directory=folder,
        recursive=True
    ):
        metadata.append(m)

    return metadata


async def read_csvs_in_folder(
    folder: str,
    table_name: str,
    client: ADLSGen2Client,
) -> AsyncGenerator[BaseTable, None]:
    folder_contents = await get_folder_contents(folder, client)

    csvs: list[ADLSPathMetadata] = []

    for metadata in folder_contents:
        if (
            not metadata.isDirectory and
            metadata.name.endswith('.csv') and
            metadata.name.startswith(f"{folder}/{table_name}/")
        ):
            csvs.append(metadata)

    if csvs:
        table_model = await get_table(
            timestamp=folder,
            table_name=table_name,
            client=client,
        )

        csvs.sort(key=lambda c: c.last_modified_datetime)

        for csv in csvs:
            async for row in client.stream_csv(csv.name, table_model, table_model.field_names):
                yield row


async def fetch_changes(
    client: ADLSGen2Client,
    table_name: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[BaseTable | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    in_progress_folder = await get_in_progress_timestamp_folder(client)

    folders = await get_timestamp_folders(client)

    for folder in folders:
        if (
            # Do not read folders we've already read on previous sweeps.
            str_to_dt(folder) <= log_cursor
            # Do not read the in progress folder. Its model.json is empty (meaning we don't
            # know the tables' final schemas) and data is still being written to it.
            or folder == in_progress_folder
        ):
            log.debug("Skipping folder", {
                "folder": folder,
                "in_progress_folder": in_progress_folder,
                "log_cursor": log_cursor
            })
            continue

        log.debug(f"Reading CSVs in {folder}/{table_name}.")
        async for row in read_csvs_in_folder(folder, table_name, client):
            yield row

        log.debug(f"Read all CSVs in folder. Yielding folder name as new cursor.", {
            "folder": str_to_dt(folder),
        })
        yield str_to_dt(folder)
