from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .bulk_job_manager import (
    BulkJobError,
    BulkJobManager,
    CANNOT_FETCH_COMPOUND_DATA,
    DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED,
    NOT_SUPPORTED_BY_BULK_API,
    MAX_BULK_QUERY_SET_SIZE,
)
from .rest_query_manager import RestQueryManager
from .shared import dt_to_str, str_to_dt, now
from .models import (
    FieldDetails,
    FieldDetailsDict,
    FullRefreshResource,
    SalesforceResource,
    CursorFields,
)

REST_CHECKPOINT_INTERVAL = 2_000
BULK_CHECKPOINT_INTERVAL = MAX_BULK_QUERY_SET_SIZE

# We have reason to believe the Salesforce API is eventually consistent to some degree. Fivetran
# re-fetches all records in the 5 minutes before their cursor value to combat eventual consistency.
# That approach emits duplicate data. Another approach is to ensure the incremental tasks never
# fetch data more recent than 5 minutes in the past. This caps how "real-time" the streams can be
# but it's a simple fix that easily rolled back if we come up with a better solution. The default
# interval is already 5 minutes, so the connector is not really "real-time" anyway.
LAG = timedelta(minutes=5)
MIN_INCREMENTAL_WINDOW_SIZE = timedelta(minutes=1)


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


def _filter_to_only_formula_fields(all_fields: FieldDetailsDict, cursor_field: str) -> tuple[bool, FieldDetailsDict]:
    mandatory_fields: list[str] = ['Id', cursor_field]

    mandatory_and_formula_fields: dict[str, FieldDetails] = {}
    has_formula_fields = False
    for field, details in all_fields.items():
        if field in mandatory_fields or details.calculated:
            mandatory_and_formula_fields[field] = details
        
        if details.calculated:
            has_formula_fields = True

    return (has_formula_fields, FieldDetailsDict.model_validate(mandatory_and_formula_fields))


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
    max_window_size: timedelta,
    checkpoint_interval: int,
) -> AsyncGenerator[dict[str, Any] | datetime, None]:
    last_seen_dt = start
    count = 0

    async for record in record_generator:
        record_dt = str_to_dt(record[cursor_field])

        # If we see a record updated more recently than the previous record we emitted,
        # checkpoint the previous records.
        if (
            count >= checkpoint_interval and 
            record_dt > last_seen_dt
        ):
            yield last_seen_dt
            count = 0

        yield record
        count += 1
        last_seen_dt = record_dt

    # Emit a final checkpoint if we saw records.
    if last_seen_dt != start and count > 0:
        yield last_seen_dt
    # If we didn't find any results in this date window, only checkpoint if we've checked the entire date window.
    # The connector should reuse the same start on the next invocation but have a more recent end if we haven't
    # checked a maximum size date window yet.
    elif (end - start >= max_window_size):
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
    is_connector_initiated: bool,
) -> AsyncGenerator[SalesforceResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)

    if start >= cutoff:
        return

    max_window_size = min(timedelta(days=window_size), cutoff - start)
    end = min(cutoff, start + max_window_size)

    cursor_field = _determine_cursor_field(fields)

    # On connector-initiated backfills, only fetch formula fields and rely on the top level
    # merge reduction strategy to merge in partial documents containing updated formula fields.
    if is_connector_initiated:
        has_formula_fields, fields = _filter_to_only_formula_fields(fields, cursor_field)
        # If there are no formula fields in this object, return early.
        if not has_formula_fields:
            return

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

        async for record_or_dt in _execution_wrapper(gen, cursor_field, start, end, max_window_size, checkpoint_interval):
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
        should_fallback_to_rest_api = False
        if err.errors:
            if CANNOT_FETCH_COMPOUND_DATA in err.errors or NOT_SUPPORTED_BY_BULK_API in err.errors:
                log.info(f"{name} cannot be queried via the Bulk API. Attempting to use the REST API instead.", {"errors": err.errors})
                should_fallback_to_rest_api = True
            elif DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED in err.errors:
                log.info(f"{err.message}. Attempting to use the REST API instead.", {"errors": err.errors})
                should_fallback_to_rest_api = True

        if should_fallback_to_rest_api:
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

    max_window_size = timedelta(days=window_size)
    max_end = now() - LAG
    end = min(max_end, log_cursor + max_window_size)

    # Return early and sleep if the end date is before the start date or if the date window is too small.
    # This is done to prevent repeatedly sending API requests for tiny date windows.
    if end < log_cursor or (end - log_cursor < MIN_INCREMENTAL_WINDOW_SIZE):
        return

    cursor_field = _determine_cursor_field(fields)

    gen = rest_query_manager.execute(
        name,
        fields,
        cursor_field,
        log_cursor,
        end,
    )

    async for record_or_dt in _execution_wrapper(gen, cursor_field, log_cursor, end, max_window_size, REST_CHECKPOINT_INTERVAL):
        if isinstance(record_or_dt, datetime):
            yield record_or_dt
        else:
            yield SalesforceResource.model_validate(record_or_dt)
