from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import (
    LogCursor,
)
from estuary_cdk.http import HTTPMixin

import source_shopify_native.graphql as gql
from source_shopify_native.models import (
    ShopifyGraphQlResource,
    FullRefreshResource,
    IncrementalResource,
)


async def fetch_incremental(
    http: HTTPMixin,
    window_size: int,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
    model: type[ShopifyGraphQlResource],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + timedelta(days=window_size)
    end = min(max_end, datetime.now(tz=UTC))
    query = model.build_query(log_cursor, end)

    if not isinstance(query, list):
        query = [query]

    for q in query:
        url = await bulk_job_manager.execute(q)

        if url is None:
            log.info(
                f"Bulk query job found no results between {log_cursor} and {end} for this query."
            )
            continue

        _, lines = await http.request_lines(log, url)
        async for record in model.process_result(log, lines()):
            resource = IncrementalResource.model_validate(record)
            yield resource

    yield end


async def fetch_full_refresh(
    http: HTTPMixin,
    start: datetime,
    window_size: int,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
    model: type[ShopifyGraphQlResource],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    end = min(start + timedelta(days=window_size), datetime.now(tz=UTC))
    query = model.build_query(start, end)
    has_results = False

    if not isinstance(query, list):
        query = [query]

    for i, q in enumerate(query):
        url = await bulk_job_manager.execute(q)

        if url is None:
            log.info(
                f"Bulk query job found no results between {start} and {end} for query {i} of {len(query)} for {model.__name__}."
            )
            continue

        _, lines = await http.request_lines(log, url)
        async for record in model.process_result(log, lines()):
            resource = FullRefreshResource.model_validate(record)
            has_results = True
            yield resource

    if not has_results:
        log.info(
            f"Full refresh query found no results between {start} and {end} for {model.__name__}."
        )
        return
