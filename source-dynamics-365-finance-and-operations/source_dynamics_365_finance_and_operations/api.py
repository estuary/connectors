import asyncio
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, cast

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
    BaseTable,
    CHECKPOINT_EVERY,
    ModelDotJson,
    ParsedCursor,
    format_cursor,
    parse_cursor,
    tables_from_model_dot_json,
)
from .shared import is_datetime_format, str_to_dt


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
FOLDER_PROCESSING_SEMAPHORE = asyncio.Semaphore(10)


# model.json metadata files are not updated after they're written.
# So there's no need to expire cache results with a TTL to ensure
# we capture updates to these files.
@alru_cache(maxsize=4, ttl=None)
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


def datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO 8601 string format for cursor."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def read_csvs_in_folder(
    folder: str,
    table_name: str,
    client: ADLSGen2Client,
    resume_csv_last_modified: str = "",
    resume_row_offset: int = 0,
) -> AsyncGenerator[tuple[dict | None, str, int], None]:
    """
    Read CSVs in a folder, yielding (row, csv_last_modified, row_index) tuples.

    Args:
        folder: Folder timestamp string
        table_name: Name of table to read
        client: ADLS client
        resume_csv_last_modified: If resuming, the last_modified of CSV to resume from
        resume_row_offset: If resuming, number of rows already processed in that CSV

    Yields:
        Tuple of (transformed_row, csv_last_modified_iso, row_index_in_csv)
        When a CSV is complete, yields (None, csv_last_modified_iso, total_rows) as a signal
    """
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

    table_model = await get_table(
        timestamp=folder,
        table_name=table_name,
        client=client,
    )

    csvs.sort(key=lambda c: c.last_modified_datetime)

    for csv in csvs:
        csv_last_modified_iso = datetime_to_iso(csv.last_modified_datetime)

        # Skip CSVs we've already fully processed
        if resume_csv_last_modified:
            if csv_last_modified_iso < resume_csv_last_modified:
                continue

            # This is the CSV we're resuming from
            if csv_last_modified_iso == resume_csv_last_modified:
                skip_rows = resume_row_offset
            else:
                # Past the resume point, start from beginning
                skip_rows = 0
                resume_csv_last_modified = ""  # Clear so we don't check again
        else:
            skip_rows = 0

        row_index = skip_rows
        rows_yielded_from_csv = 0
        async for row in client.stream_csv(csv.name, table_model.field_names, skip=skip_rows):
            yield (transform_row(row, table_model.boolean_fields), csv_last_modified_iso, row_index)
            row_index += 1
            rows_yielded_from_csv += 1

        # Signal that this CSV is complete (row=None signals completion)
        # Only signal if we actually processed rows from this CSV
        if rows_yielded_from_csv > 0:
            yield (None, csv_last_modified_iso, row_index)


def transform_row(row: dict[str, str], boolean_fields: frozenset[str]) -> dict[str, str | bool | dict[str, str]]:
    """
    Apply Dynamics 365-specific transformations to a CSV row.

    Transformations:
    - Convert boolean fields from "True"/"False"/empty strings to actual booleans
    - Add _meta field with operation type based on IsDelete field
      (IsDelete is "True" for deletions, "" otherwise)
    """
    result = cast(dict[str, str | bool | dict[str, str]], row)

    for field_name in boolean_fields:
        value = row.get(field_name)
        result[field_name] = value.lower() == "true" if value else False

    result["_meta"] = {"op": "d" if result.get("IsDelete") else "u"}

    return result


async def fetch_changes(
    client: ADLSGen2Client,
    table_name: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[dict | LogCursor, None]:
    """
    Fetch incremental changes for a table, yielding rows and checkpoints.

    Yields a checkpoint cursor every CHECKPOINT_EVERY rows to keep buffer sizes small.
    """
    assert isinstance(log_cursor, tuple) and len(log_cursor) == 1 and isinstance(log_cursor[0], str)
    parsed = parse_cursor(log_cursor)

    finalized_folders = await get_finalized_timestamp_folders(client)

    rows_since_checkpoint = 0

    for folder in finalized_folders:
        folder_dt = str_to_dt(folder)
        folder_iso = datetime_to_iso(folder_dt)

        # Skip folders we've already fully processed
        if parsed.folder_timestamp and not parsed.is_initial:
            if folder_iso < parsed.folder_timestamp:
                log.debug("Skipping already-processed folder", {"folder": folder})
                continue

            if folder_iso == parsed.folder_timestamp and parsed.is_folder_complete:
                log.debug("Skipping completed folder", {"folder": folder})
                continue

        # Determine resume position within this folder
        if folder_iso == parsed.folder_timestamp and not parsed.is_folder_complete:
            resume_csv = parsed.csv_last_modified
            resume_offset = parsed.row_offset
            log.debug("Resuming mid-folder", {
                "folder": folder,
                "resume_csv": resume_csv,
                "resume_offset": resume_offset,
            })
        else:
            resume_csv = ""
            resume_offset = 0

        async with FOLDER_PROCESSING_SEMAPHORE:
            log.debug(f"Reading CSVs in {folder}/{table_name}.")

            # Track last checkpointed position to avoid duplicate cursors
            last_checkpoint_csv = resume_csv
            last_checkpoint_row = resume_offset

            async for (row, csv_last_modified, row_index) in read_csvs_in_folder(
                folder, table_name, client, resume_csv, resume_offset
            ):
                # Reset tracking when we move to a new CSV
                if csv_last_modified != last_checkpoint_csv:
                    last_checkpoint_csv = csv_last_modified
                    last_checkpoint_row = 0

                # row=None signals CSV completion
                if row is None:
                    # Only checkpoint if we've made progress since last checkpoint
                    if row_index > last_checkpoint_row:
                        cursor = format_cursor(folder_iso, csv_last_modified, row_index)
                        log.debug("CSV complete, yielding checkpoint", {
                            "folder": folder,
                            "csv_last_modified": csv_last_modified,
                            "total_rows": row_index,
                        })
                        yield cursor
                        rows_since_checkpoint = 0
                        last_checkpoint_row = row_index
                    continue

                yield row
                rows_since_checkpoint += 1

                # Checkpoint every N rows
                if rows_since_checkpoint >= CHECKPOINT_EVERY:
                    # row_index is 0-based, so row_index + 1 = rows emitted from this CSV
                    cursor = format_cursor(folder_iso, csv_last_modified, row_index + 1)
                    log.debug("Yielding mid-CSV checkpoint", {
                        "folder": folder,
                        "csv_last_modified": csv_last_modified,
                        "rows_emitted": row_index + 1,
                    })
                    yield cursor
                    rows_since_checkpoint = 0
                    last_checkpoint_row = row_index + 1

            # Folder complete - yield cursor marking folder as done
            cursor = format_cursor(folder_iso, "", 0)
            log.debug("Folder complete, yielding cursor", {"folder": folder})
            yield cursor
            rows_since_checkpoint = 0

            # Update parsed cursor for next folder comparison
            parsed = parse_cursor(cursor)
