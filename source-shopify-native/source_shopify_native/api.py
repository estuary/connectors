from datetime import datetime, UTC, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from .models import (
    TimestampedResource,
)

from .graphql.bulk_job_manager import BulkJobManager
from .graphql import products


async def fetch_products(
        http: HTTPSession,
        window_size: int,
        bulk_job_manager: BulkJobManager,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + timedelta(days=window_size)
    end = min(max_end, datetime.now(tz=UTC))

    query = products.build_query(log_cursor, end)
    url = await bulk_job_manager.execute(query)
    # If the bulk job completes successfully but the job found no results, the URL is None.
    if url is None:
        log.info(f"Bulk query job found no results between {log_cursor} and {end}.")
        yield end
        return

    last_seen_dt = log_cursor

    _, lines = await http.request_lines(log, url)
    async for record in products.process_result(log, lines()):
        product = TimestampedResource.model_validate(record)

        if product.updatedAt > last_seen_dt:
            last_seen_dt = product.updatedAt

        if product.updatedAt > log_cursor:
            yield product

    if last_seen_dt > log_cursor:
        yield last_seen_dt
    else:
        yield end
