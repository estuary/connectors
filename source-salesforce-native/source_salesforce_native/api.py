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
from .rest_query_manager import MAX_REST_RESPONSE_SIZE, RestQueryManager, chunk_fields
from .shared import build_date_window_query, build_id_page_query, build_snapshot_query, dt_to_str, is_salesforce_id, str_to_dt, now, should_retry, VERSION
from .models import (
    MIN_INCREMENTAL_WINDOW_SIZE,
    FieldDetails,
    FieldDetailsDict,
    QueryResponse,
    SalesforceRecord,
    CursorFields,
    SObject,
    create_salesforce_model,
)

# Checkpoint intervals match the transports' page sizes; records arrive at most a page at a time.
REST_CHECKPOINT_INTERVAL = MAX_REST_RESPONSE_SIZE
BULK_CHECKPOINT_INTERVAL = MAX_BULK_QUERY_SET_SIZE

# Page sizes for Id-based keyset pagination during backfills.
BULK_ID_PAGE_LIMIT = 500_000
REST_ID_PAGE_LIMIT = MAX_REST_RESPONSE_SIZE

# We have reason to believe the Salesforce API is eventually consistent to some degree. Fivetran
# re-fetches all records in the 5 minutes before their cursor value to combat eventual consistency.
# That approach emits duplicate data. Another approach is to ensure the incremental tasks never
# fetch data more recent than 5 minutes in the past. This caps how "real-time" the streams can be
# but it's a simple fix that easily rolled back if we come up with a better solution. The default
# interval is already 5 minutes, so the connector is not really "real-time" anyway.
LAG = timedelta(minutes=5)


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
        await http.request(log, url, should_retry=should_retry)
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
        build_snapshot_query(name, list(fields.keys())),
        model_cls,
    ):
        yield record


# _datetime_execution_wrapper centralizes the datetime cursor management logic for incremental resources.
async def _datetime_execution_wrapper(
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


# _id_execution_wrapper interleaves Id page cursors into a strictly Id-ordered record stream.
# A trailing cursor is emitted only if the page was full: a short page means no records remain.
async def _id_execution_wrapper(
    record_generator: AsyncGenerator[SalesforceRecord, None],
    checkpoint_interval: int,
    limit: int,
) -> AsyncGenerator[SalesforceRecord | str, None]:
    last_id: str | None = None
    count = 0
    count_since_checkpoint = 0

    async for record in record_generator:
        if last_id is not None and count_since_checkpoint >= checkpoint_interval:
            yield last_id
            count_since_checkpoint = 0

        yield record
        last_id = record.Id
        count += 1
        count_since_checkpoint += 1

    if count >= limit and last_id is not None:
        yield last_id


# _fetch_id_page_boundary locates the Id of the next page's last record so every field-chunk
# query can share an identical closed Id range and merge deterministically. A None boundary
# means fewer than a full page remains, which stays true since updates only ever move records'
# cursor fields past the cutoff and out of the window.
async def _fetch_id_page_boundary(
    http: HTTPSession,
    instance_url: str,
    name: str,
    cursor_field: CursorFields,
    start: datetime,
    end: datetime,
    last_id: str | None,
    log: Logger,
) -> str | None:
    query = build_id_page_query(
        name,
        ["Id"],
        cursor_field,
        start,
        end,
        last_id=last_id,
        limit=1,
        offset=REST_ID_PAGE_LIMIT - 1,
    )
    url = f"{instance_url}/services/data/v{VERSION}/queryAll"

    response = QueryResponse.model_validate_json(
        await http.request(log, url, params={"q": query}, should_retry=should_retry)
    )

    if not response.records:
        return None

    return response.records[0]["Id"]


def _should_fallback_to_rest_api(err: BulkJobError, name: str, log: Logger) -> bool:
    # If this object can't be queried via the Bulk API, fallback to using the REST API.
    should_fallback = False
    if err.errors:
        if CANNOT_FETCH_COMPOUND_DATA in err.errors or NOT_SUPPORTED_BY_BULK_API in err.errors:
            log.info(f"{name} cannot be queried via the Bulk API. Attempting to use the REST API instead.", {"errors": err.errors})
            should_fallback = True
        elif DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED in err.errors or DAILY_MAX_BULK_API_QUERY_LIMIT_EXCEEDED in err.errors:
            log.info(f"{err.message}. Attempting to use the REST API instead.", {"errors": err.errors})
            should_fallback = True

    return should_fallback


async def backfill_incremental_resources(
    http: HTTPSession,
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    model_cls: type[SalesforceRecord],
    window_size: timedelta,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
    is_connector_initiated: bool,
) -> AsyncGenerator[SalesforceRecord | PageCursor, None]:
    assert isinstance(cutoff, datetime)

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

    if page is not None:
        assert isinstance(page, str)

    # Backfills that started before Id-based pagination existed persisted a datetime page cursor
    # and are driven to completion with the original date window strategy.
    # TODO(bair): Remove the backwards-compatible date window logic once we're sure all on-going
    # backfills have completed.
    if isinstance(page, str) and not is_salesforce_id(page):
        gen = _backfill_with_date_windows(
            is_supported_by_bulk_api,
            bulk_job_manager,
            rest_query_manager,
            name,
            fields,
            model_cls,
            cursor_field,
            window_size,
            log,
            str_to_dt(page),
            cutoff,
        )
    else:
        gen = _backfill_with_id_pages(
            http,
            is_supported_by_bulk_api,
            bulk_job_manager,
            rest_query_manager,
            instance_url,
            name,
            fields,
            model_cls,
            cursor_field,
            start_date,
            log,
            page,
            cutoff,
        )

    async for record_or_cursor in gen:
        yield record_or_cursor


async def _backfill_with_id_pages(
    http: HTTPSession,
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    instance_url: str,
    name: str,
    fields: FieldDetailsDict,
    model_cls: type[SalesforceRecord],
    cursor_field: CursorFields,
    start_date: datetime,
    log: Logger,
    last_id: str | None,
    cutoff: datetime,
) -> AsyncGenerator[SalesforceRecord | str, None]:
    if start_date >= cutoff:
        log.debug("Start date is at or after cutoff, indicating there is no date range to backfill.", {
            "start_date": start_date,
            "cutoff": cutoff,
        })
        return

    async def _execute_bulk() -> AsyncGenerator[SalesforceRecord | str, None]:
        gen = bulk_job_manager.execute(
            name,
            build_id_page_query(name, list(fields.keys()), cursor_field, start_date, cutoff, last_id=last_id, limit=BULK_ID_PAGE_LIMIT),
            model_cls,
        )

        async for record_or_id in _id_execution_wrapper(gen, BULK_CHECKPOINT_INTERVAL, BULK_ID_PAGE_LIMIT):
            yield record_or_id

    # The boundary Id is yielded as the page cursor after the page's records; no boundary means
    # this is the final page, and ending without a trailing cursor completes the backfill.
    async def _execute_rest() -> AsyncGenerator[SalesforceRecord | str, None]:
        boundary_id = await _fetch_id_page_boundary(http, instance_url, name, cursor_field, start_date, cutoff, last_id, log)

        queries = [
            build_id_page_query(name, chunk, cursor_field, start_date, cutoff, last_id=last_id, max_id=boundary_id)
            for chunk in chunk_fields(fields, cursor_field)
        ]

        async for record in rest_query_manager.execute(name, queries, model_cls, cursor_field, cutoff):
            yield record

        if boundary_id is not None:
            yield boundary_id

    log.debug("Executing backfill page.", {
        "last_id": last_id,
        "cutoff": cutoff,
        "is_supported_by_bulk_api": is_supported_by_bulk_api,
    })

    try:
        gen = _execute_bulk() if is_supported_by_bulk_api else _execute_rest()
        async for record_or_id in gen:
            yield record_or_id
    except BulkJobError as err:
        if _should_fallback_to_rest_api(err, name, log):
            async for record_or_id in _execute_rest():
                yield record_or_id
        else:
            raise


async def _backfill_with_date_windows(
    is_supported_by_bulk_api: bool,
    bulk_job_manager: BulkJobManager,
    rest_query_manager: RestQueryManager,
    name: str,
    fields: FieldDetailsDict,
    model_cls: type[SalesforceRecord],
    cursor_field: CursorFields,
    window_size: timedelta,
    log: Logger,
    start: datetime,
    cutoff: datetime,
) -> AsyncGenerator[SalesforceRecord | str, None]:
    if start >= cutoff:
        log.debug("Page cursor is after cutoff, indicating backfill is complete.", {
            "start": start,
            "cutoff": cutoff,
        })
        return

    max_window_size = min(window_size, cutoff - start)
    end = min(cutoff, start + max_window_size)

    async def _execute(
        gen: AsyncGenerator[SalesforceRecord, None],
        checkpoint_interval: int
    ) -> AsyncGenerator[SalesforceRecord | str, None]:
        async for record_or_dt in _datetime_execution_wrapper(gen, cursor_field, start, end, max_window_size, checkpoint_interval):
            if isinstance(record_or_dt, datetime):
                yield dt_to_str(record_or_dt)
            else:
                yield record_or_dt

    def _execute_bulk() -> AsyncGenerator[SalesforceRecord | str, None]:
        gen = bulk_job_manager.execute(
            name,
            build_date_window_query(name, list(fields.keys()), cursor_field, start, end),
            model_cls,
        )

        return _execute(gen, BULK_CHECKPOINT_INTERVAL)

    def _execute_rest() -> AsyncGenerator[SalesforceRecord | str, None]:
        queries = [
            build_date_window_query(name, chunk, cursor_field, start, end)
            for chunk in chunk_fields(fields, cursor_field)
        ]
        gen = rest_query_manager.execute(name, queries, model_cls, cursor_field, end)

        return _execute(gen, REST_CHECKPOINT_INTERVAL)

    log.debug("Executing backfill.", {
        "start": start,
        "end": end,
        "is_support_by_bulk_api": is_supported_by_bulk_api,
    })

    try:
        gen = _execute_bulk() if is_supported_by_bulk_api else _execute_rest()
        async for doc_or_str in gen:
            yield doc_or_str
    except BulkJobError as err:
        if _should_fallback_to_rest_api(err, name, log):
            async for doc_or_str in _execute_rest():
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
    window_size: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SalesforceRecord | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_end = now() - LAG
    end = min(max_end, log_cursor + window_size)

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

    queries = [
        build_date_window_query(name, chunk, cursor_field, log_cursor, end)
        for chunk in chunk_fields(fields, cursor_field)
    ]
    gen = rest_query_manager.execute(name, queries, model_cls, cursor_field, end)

    async for record_or_dt in _datetime_execution_wrapper(gen, cursor_field, log_cursor, end, window_size, REST_CHECKPOINT_INTERVAL):
        yield record_or_dt

    log.debug("Finished fetching incremental changes.", {
        "start": log_cursor,
        "end": end,
    })
