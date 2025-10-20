from datetime import UTC, datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    CatalogEntries,
    CatalogTypes,
    IncidentAttachments,
    Incidents,
    PaginatedFullRefreshStream,
    TIncrementalStream,
    TStream,
    create_paginated_response_model,
    create_response_model,
)


API = "https://api.incident.io"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


async def _snapshot_resources(
    http: HTTPSession,
    log: Logger,
    stream: type[TStream],
    params: dict[str, str | int] | None = None,
) -> AsyncGenerator[TStream, None]:
    url = f"{API}/{stream.path}"

    response_model = create_response_model(stream, stream.response_field)

    resp = response_model.model_validate_json(
        await http.request(log, url, params=params)
    )

    for r in resp.resources:
        yield r


async def _paginate_through_resources(
    http: HTTPSession,
    stream: type[TStream],
    parameters: dict[str, str | int],
    log: Logger,
) -> AsyncGenerator[TStream, None]:
    url = f"{API}/{stream.path}"
    params = parameters.copy()
    response_model = create_paginated_response_model(stream, stream.response_field)

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            # NOTE: The IncrementalJsonProcessor parses the response before any
            # Pydantic validation or aliasing occurs. Meaning, the prefix passed
            # to the IncrementalJsonProcessor is the prefix in the API response,
            # not the `resources` alias defined in in the Pydantic models
            f"{stream.response_field}.item",
            stream,
            response_model,
        )

        async for resource in processor:
            yield resource

        remainder = processor.get_remainder()

        if remainder.pagination_meta.after:
            params["after"] = remainder.pagination_meta.after
        else:
            break


async def snapshot_resources(
    http: HTTPSession,
    stream: type[TStream],
    log: Logger,
) -> AsyncGenerator[TStream, None]:
    if issubclass(stream, PaginatedFullRefreshStream):
        params: dict[str, str | int] = {"page_size": stream.page_size}
        gen = _paginate_through_resources(http, stream, params, log)
    else:
        gen = _snapshot_resources(http, log, stream)

    async for resource in gen:
        yield resource


async def fetch_client_side_filtered_resources(
    http: HTTPSession,
    stream: type[TIncrementalStream],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TIncrementalStream | datetime, None]:
    assert isinstance(log_cursor, datetime)

    most_recent_dt = log_cursor

    async for r in _snapshot_resources(http, log, stream):
        if r.updated_at > log_cursor:
            yield r
            most_recent_dt = max(most_recent_dt, r.updated_at)

    if most_recent_dt != log_cursor:
        yield most_recent_dt


async def backfill_client_side_filtered_resources(
    http: HTTPSession,
    stream: type[TIncrementalStream],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[TIncrementalStream, None]:
    assert isinstance(cutoff, datetime)

    async for r in _snapshot_resources(http, log, stream):
        if r.updated_at <= cutoff:
            yield r


async def _fetch_all_catalog_entries(
    http: HTTPSession,
    stream: type[CatalogEntries],
    log: Logger,
) -> AsyncGenerator[CatalogEntries, None]:
    catalog_type_ids: set[str] = set()

    async for catalog_type in _snapshot_resources(http, log, CatalogTypes):
        catalog_type_ids.add(catalog_type.id)

    for id in catalog_type_ids:
        params = {
            "page_size": stream.page_size,
            "catalog_type_id": id,
        }

        async for r in _paginate_through_resources(http, stream, params, log):
            yield r


async def fetch_catalog_entries(
    http: HTTPSession,
    stream: type[CatalogEntries],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CatalogEntries | datetime, None]:
    assert isinstance(log_cursor, datetime)

    most_recent_dt = log_cursor

    async for r in _fetch_all_catalog_entries(http, stream, log):
        if r.updated_at > log_cursor:
            most_recent_dt = max(most_recent_dt, r.updated_at)
            yield r

    if most_recent_dt != log_cursor:
        yield most_recent_dt


async def backfill_catalog_entries(
    http: HTTPSession,
    stream: type[CatalogEntries],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor
) -> AsyncGenerator[CatalogEntries, None]:
    assert isinstance(cutoff, datetime)

    async for r in _fetch_all_catalog_entries(http, stream, log):
        if r.updated_at <= cutoff:
            yield r


async def _fetch_incidents_between(
    http: HTTPSession,
    stream: type[Incidents],
    log: Logger,
    start: datetime,
    end: datetime,
) -> AsyncGenerator[Incidents, None]:
    params = {
        "page_size": stream.page_size,
        # The "updated_at[date_range]" query parameter is inclusive of both bounds.
        "updated_at[date_range]": f"{start.date()}~{end.date()}",
    }

    async for incident in _paginate_through_resources(http, stream, params, log):
        if start <= incident.updated_at <= end:
            yield incident


async def fetch_incidents(
    http: HTTPSession,
    stream: type[Incidents],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Incidents | datetime, None]:
    assert isinstance(log_cursor, datetime)

    most_recent_dt = log_cursor

    now = datetime.now(tz=UTC)

    async for incident in _fetch_incidents_between(
        http,
        stream,
        log,
        start = log_cursor,
        end = now,
    ):
        if incident.updated_at > log_cursor:
            most_recent_dt = max(most_recent_dt, incident.updated_at)
            yield incident

    if most_recent_dt != log_cursor:
        yield most_recent_dt


async def backfill_incidents(
    http: HTTPSession,
    stream: type[Incidents],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Incidents, None]:
    assert isinstance(cutoff, datetime)

    async for incident in _fetch_incidents_between(
        http,
        stream,
        log,
        start = EPOCH,
        end = cutoff,
    ):
        yield incident


async def snapshot_incident_attachments(
    http: HTTPSession,
    stream: type[IncidentAttachments],
    log: Logger,
) -> AsyncGenerator[IncidentAttachments | datetime, None]:
    incident_ids: set[str] = set()

    async for incident in _fetch_incidents_between(
        http,
        Incidents,
        log,
        start = EPOCH,
        end = datetime.now(tz=UTC)
    ):
        incident_ids.add(incident.id)

    for id in sorted(incident_ids):
        params: dict[str, str | int] = {
            "incident_id": id
        }

        async for attachment in _snapshot_resources(http, log, stream, params):
            yield attachment
