from __future__ import annotations

from collections.abc import AsyncGenerator, Generator, Iterable
from datetime import UTC, datetime
from itertools import batched, repeat
from logging import Logger

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from source_zoho.utils import achain, areduce, dt_to_ts

from .models import (
    ALWAYS_REQUIRED_DOCUMENT_FIELDS,
    FieldTypeFilter,
    ModuleFieldListing,
    ModuleListing,
    ZohoModule,
)

UNSUPPORTED_FIELD_NAMES = {
    "Calls": {"CTI_Entry"},
    "Cases": {"Comments"},
    "Events": {"Participants"},
    "Price_Books": {"Pricing_Details"},
    "Solutions": {"Comments"},
    # "*_Items" fields are listed as their own modules
    # and are being collected on separate streams
    "Invoices": {"Invoiced_Items"},
    "Purchase_Orders": {"Purchase_Items"},
    "Quotes": {"Quoted_Items"},
    "Sales_Orders": {"Ordered_Items"},
}
MAX_QUERY_FIELD_COUNT = 500


class FieldListingUnavailable(Exception):
    """Raised when field listing returns 204 (no content) for a module."""

    module_api_name: str

    def __init__(self, module_api_name: str):
        self.module_api_name = module_api_name
        super().__init__(f"Field listing returned 204 for module: {module_api_name}")


async def fetch_available_modules(
    base_url: str, log: Logger, http: HTTPSession
) -> AsyncGenerator[ModuleListing, None]:
    """Lists all API-supported modules for classification."""
    url = f"{base_url}/crm/v8/settings/modules"

    _, body = await http.request_stream(log, url)
    processor = IncrementalJsonProcessor(body(), "modules.item", ModuleListing)

    async for module in processor:
        if module.api_supported:
            yield module


async def fetch_supported_fields(
    base_url: str, module_api_name: str, log: Logger, http: HTTPSession
) -> AsyncGenerator[ModuleFieldListing, None]:
    """
    Lists all queryable fields for a given module.
    Raises FieldListingUnavailable if the API returns no content.
    """
    url = f"{base_url}/crm/v8/settings/fields?module={module_api_name}&type=all"

    _, body = await http.request_stream(log, url)

    # Peek at the first chunk to detect empty response (204 No Content)
    body_gen = body()
    first_chunk = await anext(body_gen, None)

    if first_chunk is None:
        raise FieldListingUnavailable(module_api_name)

    async def body_with_first_chunk():
        yield first_chunk
        async for chunk in body_gen:
            yield chunk

    processor = IncrementalJsonProcessor(
        body_with_first_chunk(), "fields.item", ModuleFieldListing
    )

    denylist = UNSUPPORTED_FIELD_NAMES.get(module_api_name, {})

    found_any_field = False
    async for field in processor:
        found_any_field = True
        if field.is_api_supported and field.api_name not in denylist:
            yield field

    if not found_any_field:
        raise FieldListingUnavailable(module_api_name)


def _batch_fields(
    fields_to_query: Iterable[str],
) -> Iterable[set[str]]:
    """
    COQL limits the number of fields we can query in a single SELECT clause,
    so we may need to run multiple queries for partial documents and then merge
    in memory before yielding.
    """
    batches = batched(
        fields_to_query,
        MAX_QUERY_FIELD_COUNT - len(ALWAYS_REQUIRED_DOCUMENT_FIELDS),
    )

    return map(
        lambda batch: set(batch) | ALWAYS_REQUIRED_DOCUMENT_FIELDS,
        batches,
    )


async def _execute_coql_query(
    base_url: str,
    query: str,
    module: type[ZohoModule],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[ZohoModule, None]:
    try:
        _, body = await http.request_stream(
            log,
            f"{base_url}/crm/v8/coql",
            method="POST",
            headers={"Accept": "application/json"},
            json={"select_query": query},
        )
        log.debug(f"Running query: {query}")

        async for item in IncrementalJsonProcessor(
            body(), "data.item", module, validation_context={"is_coql_data": True}
        ):
            yield item
    except HTTPError as e:
        e.add_note(f'Running query: "{query}"')
        raise e


def page_sizes_gen() -> Generator[int]:
    yield 200
    yield 1000
    yield from repeat(2000)


def _merge_into_document_dict(
    acc: dict[int, ZohoModule], item: ZohoModule
) -> dict[int, ZohoModule]:
    acc[item.id] = acc[item.id].update(item) if item.id in acc else item
    return acc


async def _query_module(
    base_url: str,
    module: type[ZohoModule],
    start_date: datetime,
    end_date: datetime,
    http: HTTPSession,
    log: Logger,
    fields_filter: FieldTypeFilter = FieldTypeFilter.ALL,
) -> AsyncGenerator[ZohoModule, None]:
    cursor_ts = dt_to_ts(start_date)
    cursor_id: int | None = None
    end_date_ts = dt_to_ts(end_date)

    field_names = module.get_field_api_names(fields_filter)
    assert (
        field_names is not None
    ), "We can't run COQL queries on modules with no known fields"

    field_batches = _batch_fields(field_names)
    page_sizes = page_sizes_gen()
    page_size = next(page_sizes)

    while True:
        tie_breaker_id_filter = (
            f" AND (Modified_Time > '{cursor_ts}' OR id > {cursor_id})"
            if cursor_id is not None
            else ""
        )

        batch_queries = [
            f"SELECT {', '.join(fb)} FROM {module.api_name}"
            + f" WHERE Modified_Time >= '{cursor_ts}'"
            + f" AND Modified_Time < '{end_date_ts}'"
            + tie_breaker_id_filter
            + f" ORDER BY Modified_Time ASC, id ASC LIMIT {page_size}"
            for fb in field_batches
        ]

        results_by_id = await areduce(
            _merge_into_document_dict,
            achain(
                *(
                    _execute_coql_query(base_url, query, module, http, log)
                    for query in batch_queries
                )
            ),
            {},
        )

        # Filter complete documents (incomplete = updated mid-query), sort, and yield
        complete_docs = sorted(
            (
                item
                for item in results_by_id.values()
                if item.is_complete_document(fields_filter)
            ),
            key=lambda x: (x.Modified_Time, x.id),
        )

        if not complete_docs:
            return

        for item in complete_docs:
            yield item

        if len(results_by_id) < page_size:
            return

        last = complete_docs[-1]
        cursor_ts = dt_to_ts(last.Modified_Time)
        cursor_id = last.id
        page_size = next(page_sizes)


async def fetch_module(
    base_url: str,
    module: type[ZohoModule],
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZohoModule | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    truncated_log_cursor = log_cursor.replace(microsecond=0)
    now = datetime.now(tz=UTC).replace(microsecond=0)

    if now == truncated_log_cursor:
        return

    log.info("Starting incremental fetch", {"log_cursor": log_cursor})

    async for item in _query_module(base_url, module, log_cursor, now, http, log):
        yield item

    yield now
