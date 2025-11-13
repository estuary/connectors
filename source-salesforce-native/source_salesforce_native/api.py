from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .bulk_job_manager import (
    BulkJobError,
    BulkJobManager,
    CANNOT_FETCH_COMPOUND_DATA,
    DAILY_MAX_BULK_API_QUERY_LIMIT_EXCEEDED,
    DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED,
    NOT_SUPPORTED_BY_BULK_API,
    MAX_BULK_QUERY_SET_SIZE,
)
from .rest_query_manager import RestQueryManager
from .shared import dt_to_str, str_to_dt, now, VERSION
from .models import (
    FieldDetails,
    FieldDetailsDict,
    SalesforceRecord,
    CursorFields,
    SObject,
    create_salesforce_model,
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


async def fetch_object_fields(
    log: Logger,
    http: HTTPSession,
    instance_url: str,
    name: str,
    additional_fields_to_include_in_refresh: list[str],
) -> FieldDetailsDict:
    url = f"{instance_url}/services/data/v{VERSION}/sobjects/{name}/describe"

    response = SObject.model_validate_json(
        await http.request(log, url)
    )

    fields = {}

    for field in response.fields:
        should_include_in_refresh = field.calculated or field.name in additional_fields_to_include_in_refresh

        fields[field.name] = {
            "soapType": field.soapType,
            "calculated": field.calculated,
            "custom": field.custom,
            "should_include_in_refresh": should_include_in_refresh,
        }

    return FieldDetailsDict.model_validate(fields)


def _filter_to_only_refresh_fields(all_fields: FieldDetailsDict, cursor_field: str) -> tuple[bool, FieldDetailsDict]:
    mandatory_fields: list[str] = ['Id', cursor_field]

    mandatory_and_formula_fields: dict[str, FieldDetails] = {}
    has_fields_to_refresh = False
    for field, details in all_fields.items():
        if field in mandatory_fields or details.should_include_in_refresh:
            mandatory_and_formula_fields[field] = details

        if details.should_include_in_refresh:
            has_fields_to_refresh = True

    return (has_fields_to_refresh, FieldDetailsDict.model_validate(mandatory_and_formula_fields))


async def snapshot_resources(
    http: HTTPSession,
    bulk_job_manager: BulkJobManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    model_cls: type[SalesforceRecord],
    log: Logger,
) -> AsyncGenerator[SalesforceRecord, None]:

    async for record in bulk_job_manager.execute(
        name,
        fields,
        model_cls,
    ):
        yield record


# _execution_wrapper centralizes the cursor management logic for incremental resources.
async def _execution_wrapper(
    record_generator: AsyncGenerator[SalesforceRecord, None],
    cursor_field: CursorFields,
    start: datetime,
    end: datetime,
    max_window_size: timedelta,
    checkpoint_interval: int,
) -> AsyncGenerator[SalesforceRecord | datetime, None]:
    last_seen_dt = start
    count = 0

    async for record in record_generator:
        record_dt = str_to_dt(getattr(record, cursor_field))

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
    model_cls: type[SalesforceRecord],
    window_size: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
    is_connector_initiated: bool,
) -> AsyncGenerator[SalesforceRecord | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)

    if start >= cutoff:
        log.debug("Page cursor is after cutoff, indicating backfill is complete.", {
            "start": start,
            "cutoff": cutoff,
        })
        return

    max_window_size = min(timedelta(days=window_size), cutoff - start)
    end = min(cutoff, start + max_window_size)

    cursor_field = _determine_cursor_field(fields)

    # On connector-initiated backfills, only fetch formula fields and rely on the top level
    # merge reduction strategy to merge in partial documents containing updated formula fields.
    if is_connector_initiated:
        has_fields_to_refresh, fields = _filter_to_only_refresh_fields(fields, cursor_field)

        # Formula fields can only contain scalar values. Even if the object contains complex
        # fields that can't be quried via the Bulk API, formula fields should always be query-able
        # via the Bulk API & we should default to using it for formula field refreshes.
        is_supported_by_bulk_api = True

        # If there are no formula fields in this object, return early.
        if not has_fields_to_refresh:
            return

    async def _execute(
        manager: BulkJobManager | RestQueryManager, 
        checkpoint_interval: int
    ) -> AsyncGenerator[SalesforceRecord | str, None]:
        gen = manager.execute(
            name,
            fields,
            model_cls,
            cursor_field,
            start,
            end,
        )

        async for record_or_dt in _execution_wrapper(gen, cursor_field, start, end, max_window_size, checkpoint_interval):
            if isinstance(record_or_dt, datetime):
                yield dt_to_str(record_or_dt)
            else:
                yield record_or_dt

    log.debug("Executing backfill.", {
        "start": start,
        "end": end,
        "is_support_by_bulk_api": is_supported_by_bulk_api,
    })

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
            elif DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED in err.errors or DAILY_MAX_BULK_API_QUERY_LIMIT_EXCEEDED in err.errors:
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
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SalesforceRecord | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_window_size = timedelta(days=window_size)
    max_end = now() - LAG
    end = min(max_end, log_cursor + max_window_size)

    # Return early and sleep if the end date is before the start date or if the date window is too small.
    # This is done to prevent repeatedly sending API requests for tiny date windows.
    if end < log_cursor or (end - log_cursor < MIN_INCREMENTAL_WINDOW_SIZE):
        return

    # Fetch the object's current set of fields on each invocation of fetch_incremental_resources. This is done
    # to detect when new custom fields are created on a Salesforce object while the connector is running & ensure
    # those new fields are included in REST API queries. Otherwise, if we were only to fetch an object's fields
    # at the beginning of a connector invocation, it's possible for `fetch_incremental_resources` to process incremental
    # changes with an out-of-date list of object fields & end up omitting recently created fields in yielded documents.
    fields = await fetch_object_fields(log, http, instance_url, name, additional_fields_to_include_in_refresh=[])
    model_cls = create_salesforce_model(name, fields)

    cursor_field = _determine_cursor_field(fields)

    log.debug("Fetching incremental changes.", {
        "start": log_cursor,
        "end": end,
        "is_support_by_bulk_api": is_supported_by_bulk_api,
    })

    gen = rest_query_manager.execute(
        name,
        fields,
        model_cls,
        cursor_field,
        log_cursor,
        end,
    )

    async for record_or_dt in _execution_wrapper(gen, cursor_field, log_cursor, end, max_window_size, REST_CHECKPOINT_INTERVAL):
        yield record_or_dt

    log.debug("Finished fetching incremental changes.", {
        "start": log_cursor,
        "end": end,
    })
