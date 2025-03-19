from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .bulk_job_manager import BulkJobError, BulkJobManager, NOT_SUPPORTED_BY_BULK_API, CANNOT_FETCH_COMPOUND_DATA
from .rest_query_manager import RestQueryManager
from .shared import dt_to_str, str_to_dt
from .models import (
    FieldDetailsDict,
    FullRefreshResource,
    SalesforceResource,
    CursorFields,
)

REST_CHECKPOINT_INTERVAL = 2_000
BULK_CHECKPOINT_INTERVAL = 200_000

def _determine_cursor_field(
    fields: FieldDetailsDict,
) -> CursorFields:
    field_names = list(fields.keys())
    if CursorFields.SYSTEM_MODSTAMP in field_names:
        return CursorFields.SYSTEM_MODSTAMP
    elif CursorFields.LAST_MODIFIED_DATE in field_names:
        return CursorFields.LAST_MODIFIED_DATE
    elif CursorFields.CREATED_DATE in field_names:
        return CursorFields.CREATED_DATE
    elif CursorFields.LOGIN_TIME in field_names:
        return CursorFields.LOGIN_TIME
    else:
        raise RuntimeError("Attempted to find cursor field but no valid cursor field exists.")


async def snapshot_resources(
    http: HTTPSession,
    bulk_job_manager: BulkJobManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:

    async for record in bulk_job_manager.execute(
        name,
        fields,
    ):
        yield FullRefreshResource.model_validate(record)


# _execution_wrapper centralizes the cursor management logic for incremental resources.
async def _execution_wrapper(
    record_generator: AsyncGenerator[dict[str, Any], None],
    cursor_field: CursorFields,
    start: datetime,
    end: datetime,
    checkpoint_interval: int,
) -> AsyncGenerator[dict[str, Any] | datetime, None]:
    last_seen_dt = start
    count = 0

    async for record in record_generator:
        record_dt = str_to_dt(record[cursor_field])

        if (
            count >= checkpoint_interval and 
            record_dt > last_seen_dt
        ):
            yield last_seen_dt
            count = 0

        yield record
        count += 1
        last_seen_dt = record_dt

    if last_seen_dt != start and count > 0:
        yield last_seen_dt
    else:
        yield end


async def backfill_incremental_resources(
    http: HTTPSession,
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    window_size: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[SalesforceResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)

    if start >= cutoff:
        return

    end = min(cutoff, start + timedelta(days=window_size))

    cursor_field = _determine_cursor_field(fields)

    async def _execute(
        manager: BulkJobManager | RestQueryManager, 
        checkpoint_interval: int
    ) -> AsyncGenerator[SalesforceResource | str, None]:
        gen = manager.execute(
            name,
            fields,
            cursor_field,
            start,
            end,
        )

        async for record_or_dt in _execution_wrapper(gen, cursor_field, start, end, checkpoint_interval):
            if isinstance(record_or_dt, datetime):
                yield dt_to_str(record_or_dt)
            else:
                yield SalesforceResource.model_validate(record_or_dt)

    try:
        gen = _execute(bulk_job_manager, BULK_CHECKPOINT_INTERVAL) if is_supported_by_bulk_api else _execute(rest_query_manager, REST_CHECKPOINT_INTERVAL)
        async for doc_or_str in gen:
            yield doc_or_str
    except BulkJobError as err:
        # If this object can't be queried via the Bulk API, fallback to using the REST API.
        if err.errors and (CANNOT_FETCH_COMPOUND_DATA in err.errors or NOT_SUPPORTED_BY_BULK_API in err.errors):
            log.info(f"{name} cannot be queried via the Bulk API. Attempting to use the REST API instead.", {"errors": err.errors})
            async for doc_or_str in _execute(rest_query_manager, REST_CHECKPOINT_INTERVAL):
                yield doc_or_str
        
        else:
            raise


async def fetch_incremental_resources(
    http: HTTPSession,
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SalesforceResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    end = min(datetime.now(tz=UTC), log_cursor + timedelta(days=window_size))

    cursor_field = _determine_cursor_field(fields)

    gen = rest_query_manager.execute(
        name,
        fields,
        cursor_field,
        log_cursor,
        end,
    )

    async for record_or_dt in _execution_wrapper(gen, cursor_field, log_cursor, end, REST_CHECKPOINT_INTERVAL):
        if isinstance(record_or_dt, datetime):
            yield record_or_dt
        else:
            yield SalesforceResource.model_validate(record_or_dt)
