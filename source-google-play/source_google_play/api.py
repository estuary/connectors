from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    GooglePlayRow,
)

from .gcs import GCSClient, GCSFileMetadata
from .shared import next_month, dt_to_str, str_to_dt


async def fetch_resources(
    gcs_client: GCSClient,
    model: type[GooglePlayRow],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[GooglePlayRow | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # The code below this return is a best-effort implementation based on the Google Play
    # documentation about how the GCS bucket organizes data. It boils down to:
    # - Find all files updated on or after the log_cursor.
    # - Yield all rows from those files.
    #
    # Once we have valid credentials, development can continue and we can iterate on the code below.
    return

    files: list[GCSFileMetadata] = []
    async for file in gcs_client.list_files(prefix=model.prefix, globPattern=model.get_glob_pattern()):
        if file.updated >= log_cursor:
            files.append(file)

    for file in files:
        async for row in gcs_client.stream_csv(
            file.name,
            model,
            model.validation_context_model(filename=file.name),
        ):
            yield row

    if len(files) > 0:
        latest_file = max(files, key=lambda f: f.updated)
        yield latest_file.updated + timedelta(milliseconds=1)


async def backfill_resources(
    gcs_client: GCSClient,
    model: type[GooglePlayRow],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[GooglePlayRow | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)
    cursor_month = str_to_dt(page)

    if cursor_month >= cutoff:
        return

    # The code below this return is a best-effort implementation based on the Google Play
    # documentation about how the GCS bucket organizes data. It boils down to:
    # - Find all files containing data for the same month as the page cursor.
    # - Yield all rows from those files.
    # - Stop when the page cursor reaches the cutoff.
    #
    # Once we have valid credentials, development can continue and we can iterate on the code below.
    return

    files: list[GCSFileMetadata] = []
    async for file in gcs_client.list_files(prefix=model.prefix, globPattern=model.get_glob_pattern(cursor_month)):
        files.append(file)

    for file in files:
        async for row in gcs_client.stream_csv(
            file.name,
            model,
            model.validation_context_model(filename=file.name)
        ):
            yield row

    # Move the cursor forward to the next month.
    next_page = next_month(cursor_month)
    if next_page >= cutoff:
        return

    yield dt_to_str(next_page)
