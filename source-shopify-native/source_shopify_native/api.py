from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, TypeVar

from estuary_cdk.capture.common import (
    LogCursor,
)
from estuary_cdk.http import HTTPMixin

import source_shopify_native.graphql as gql
from source_shopify_native.models import (
    ShopifyGraphQlResource,
    FullRefreshResource,
)

T = TypeVar("T", bound=ShopifyGraphQlResource)


async def fetch_incremental(
    http: HTTPMixin,
    window_size: int,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
    model: type[T],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    max_end = log_cursor + timedelta(days=window_size)
    end = min(max_end, datetime.now(tz=UTC))
    query = model.build_query(log_cursor, end)

    url = await bulk_job_manager.execute(query)

    if url is None:
        log.info(
            f"Bulk query job found no results between {log_cursor} and {end} for this query."
        )
        yield end
        return

    _, lines = await http.request_lines(log, url)
    async for record in model.process_result(log, lines()):
        resource = model.model_validate(record)
        yield resource

    yield end


async def fetch_full_refresh(
    http: HTTPMixin,
    start_date: datetime,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
    model: type[ShopifyGraphQlResource],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    end = datetime.now(tz=UTC)
    query = model.build_query(start_date, end)

    url = await bulk_job_manager.execute(query)

    if url is None:
        log.info(
            f"Bulk query job found no results between {start_date} and {end} for {model.__name__}."
        )
        return

    _, lines = await http.request_lines(log, url)
    async for record in model.process_result(log, lines()):
        resource = FullRefreshResource.model_validate(record)
        yield resource
