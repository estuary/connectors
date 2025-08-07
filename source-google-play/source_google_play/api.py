from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    GooglePlayRow,
    Reviews,
    Statistics,
)

from .gcs import GCSClient, GCSFileMetadata
from .shared import next_month, dt_to_str, str_to_dt


async def fetch_statistics(
    gcs_client: GCSClient,
    model: type[Statistics],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Statistics | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

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


async def fetch_reviews(
    gcs_client: GCSClient,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Reviews | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    files: list[GCSFileMetadata] = []
    async for file in gcs_client.list_files(prefix=Reviews.prefix, globPattern=Reviews.get_glob_pattern()):
        if file.updated >= log_cursor:
            files.append(file)

    last_review_updated_at = log_cursor
    count = 0
    for file in files:
        async for row in gcs_client.stream_csv(
            file.name,
            Reviews,
            Reviews.validation_context_model(filename=file.name),
        ):
            if row.updated_at >= log_cursor:
                count += 1
                yield row
                last_review_updated_at = max(last_review_updated_at, row.updated_at)

    if count > 0:
        yield last_review_updated_at + timedelta(seconds=1)


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
