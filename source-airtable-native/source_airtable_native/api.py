from datetime import datetime, timedelta, UTC
from logging import Logger
import time
from typing import AsyncGenerator, TypeVar

from estuary_cdk.capture.common import BaseDocument, LogCursor, PageCursor
import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .shared import dt_to_str, now, str_to_dt
from .models import (
    Base,
    BasesResponse,
    Table,
    TableValidationContext,
    AirtableField,
    AirtableRecord,
    IncrementalAirtableRecord,
    RecordsResponse,
)

RecordT = TypeVar("RecordT", bound=AirtableRecord)


BASE_URL = "https://api.airtable.com"
PAGE_SIZE = 100
CHECKPOINT_INTERVAL = PAGE_SIZE * 5
TARGET_FETCH_PAGE_INVOCATION_RUN_TIME = 60 * 5 # 5 minutes
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _build_incremental_params(
    cursor_field_name: str,
    lower_bound: datetime,
    upper_bound: datetime,
) -> dict[str, str | list[str]]:
    """Build query params for fetching records modified between lower_bound and upper_bound.

    Airtable pagination uses snapshot-based ordering: the sort order is frozen at the time of the
    first request, but field values can change during pagination. This means records modified
    after pagination starts will appear "out of order" - they keep their original position but
    show updated cursor field values. All records with cursor values between lower_bound and
    upper_bound are ordered. However, records modified during pagination may appear interspersed
    in the stream with cursor field values beyond the upper_bound.
    """
    return {
        "sort[0][direction]": "asc",
        "sort[0][field]": cursor_field_name,
        "filterByFormula": (
            f"AND("
            f"IS_AFTER({{{cursor_field_name}}}, '{dt_to_str(lower_bound)}'), "
            f"IS_BEFORE({{{cursor_field_name}}}, '{dt_to_str(upper_bound)}')"
            f")"
        ),
    }


async def snapshot_bases(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Base, None]:
    url = f"{BASE_URL}/v0/meta/bases"

    params = {}

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            f"bases.item",
            Base,
            BasesResponse,
        )

        async for base in processor:
            yield base

        remainder = processor.get_remainder()

        if not remainder.offset:
            break

        params["offset"] = remainder.offset


async def fetch_tables_in_base(
    http: HTTPSession,
    base_id: str,
    log: Logger,
) -> AsyncGenerator[Table, None]:
    url = f"{BASE_URL}/v0/meta/bases/{base_id}/tables"

    _, body = await http.request_stream(log, url)
    processor = IncrementalJsonProcessor(
        input=body(),
        prefix=f"tables.item",
        streamed_item_cls=Table,
        validation_context=TableValidationContext(base_id=base_id),
    )

    async for table in processor:
        yield table


async def snapshot_tables(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Table, None]:
    bases: list[Base] = []

    async for b in snapshot_bases(http, log):
        bases.append(b)

    for base in bases:
        async for table in fetch_tables_in_base(http, base.id, log):
            yield table


async def _paginate_through_records(
    http: HTTPSession,
    path: str,
    additionalParams: dict[str, str | list[str]] | None,
    record_cls: type[RecordT],
    log: Logger,
) -> AsyncGenerator[RecordT, None]:
    url = f"{BASE_URL}/v0/{path}"

    if additionalParams:
        params = additionalParams.copy()
    else:
        params: dict[str, str | list[str]] = {}

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            f"records.item",
            record_cls,
            RecordsResponse,
        )

        async for resource in processor:
            yield resource

        remainder = processor.get_remainder()

        if not remainder.offset:
            break

        params["offset"] = remainder.offset


async def snapshot_records(
    http: HTTPSession,
    path: str,
    log: Logger,
) -> AsyncGenerator[BaseDocument, None]:
    async for record in _paginate_through_records(
        http,
        path,
        None,
        AirtableRecord,
        log,
    ):
        yield record


async def fetch_incremental_records(
    http: HTTPSession,
    path: str,
    record_cls: type[IncrementalAirtableRecord],
    horizon: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalAirtableRecord | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    upper_bound = now() - horizon
    lower_bound = log_cursor

    if lower_bound >= upper_bound:
        return

    params = _build_incremental_params(record_cls.cursor_field_name, lower_bound, upper_bound)

    most_recent_dt = log_cursor

    async for doc in _paginate_through_records(
        http,
        path,
        params,
        record_cls,
        log,
    ):
        # A record's cursor_value could change mid-pagination. When we start paginating through
        # records, Airtable creates a snapshot of the result set at the time of the first request.
        # The pagination cursor maintains this snapshot's ordering, even if records are modified
        # during pagination. This means:
        # - The order of documents is determined when we make the first API call without an offset.
        # - Field values can change mid-pagination, but their cursor_value will also change. Since
        #   we set upper_bound = now() - horizon before pagination starts, any record modified
        #   during pagination gets a lastModifiedTime of "now", which is always >= upper_bound.
        #
        # We treat this as a mostly-ordered stream: records with cursor_values between lower_bound
        # and upper_bound appear in strictly increasing order. Records with cursor_value >= upper_bound
        # were modified after we began paginating, so we skip them here and capture them on the next
        # `fetch_incremental_records` sweep.
        if doc.cursor_value >= upper_bound:
            continue

        if (
            doc.cursor_value > log_cursor
            and cache.should_yield(path, doc.id, doc.cursor_value)
        ):
            yield doc

        if doc.cursor_value > most_recent_dt:
            most_recent_dt = doc.cursor_value

    if most_recent_dt > log_cursor:
        yield most_recent_dt


async def backfill_incremental_records(
    http: HTTPSession,
    path: str,
    record_cls: type[IncrementalAirtableRecord],
    fields: list[AirtableField],
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
    is_connector_initiated: bool,
) -> AsyncGenerator[IncrementalAirtableRecord | PageCursor, None]:
    if page is None:
        page = dt_to_str(EPOCH)

    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    upper_bound = cutoff + timedelta(seconds=1)
    lower_bound = str_to_dt(page)

    if lower_bound >= upper_bound:
        return

    params = _build_incremental_params(record_cls.cursor_field_name, lower_bound, upper_bound)

    if is_connector_initiated:
        fields_to_refresh = [f.name for f in fields if f.type == 'formula']

        # If there are no formula fields in this table, don't perform
        # a formula field refresh.
        if not fields_to_refresh:
            return

        fields_to_refresh.append(record_cls.cursor_field_name)
        params["fields"] = fields_to_refresh

    most_recent_dt = lower_bound
    count = 0
    start_time = time.time()

    async for doc in _paginate_through_records(
        http,
        path,
        params,
        record_cls,
        log,
    ):
        # If a document is updated after the cutoff, skip it. It has been updated since we started
        # paging through results, and the incremental task will capture it.
        if doc.cursor_value >= upper_bound:
            continue

        # If we see a document updated more recently than the previous
        # document we emitted, checkpoint the previous documents.
        if (
            count >= CHECKPOINT_INTERVAL
            and doc.cursor_value > most_recent_dt
        ):
            yield dt_to_str(most_recent_dt)
            count = 0

            # If backfill_incremental_resources has been running for more than TARGET_FETCH_PAGE_INVOCATION_RUN_TIME
            # minutes, then yield control back to the CDK after a checkpoint. This forces backfills to check the
            # stopping event every so often & gracefully exit if it's set.
            if time.time() - start_time > TARGET_FETCH_PAGE_INVOCATION_RUN_TIME:
                return

        yield doc
        count += 1
        most_recent_dt = doc.cursor_value
