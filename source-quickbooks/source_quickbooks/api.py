from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic.types import AwareDatetime

from .models import QuickBooksEntity

PAGE_SIZE = 1000
TARGET_FETCH_PAGE_INVOCATION_RUN_TIME = timedelta(minutes=5)

# Incremental queries exclude items updated within this delay of the current
# moment. The exclusive lower bound on incremental queries relies on the
# cursor reflecting a fully-settled state of the source, so we hold the upper
# bound back from "now" to give recent updates time to become visible across
# QuickBooks' backend before we advance past them.
INCREMENTAL_FETCH_DELAY = timedelta(seconds=30)

PROD_API_BASE_URL = "https://quickbooks.api.intuit.com"
SANDBOX_API_BASE_URL = "https://sandbox-quickbooks.api.intuit.com"

T = TypeVar("T", bound=QuickBooksEntity)


async def query_entity(
    model: type[T],
    start_date: datetime,
    end_date: datetime,
    http: HTTPSession,
    base_url: str,
    realm_id: str,
    log: Logger,
) -> AsyncGenerator[T, None]:
    url = f"{base_url}/v3/company/{realm_id}/query"
    query_offset = 0

    while True:
        query_statement = (
            f"SELECT * FROM {model.table_name}"
            f" WHERE Metadata.LastUpdatedTime > '{dt_to_ts(start_date)}'"
            f" AND Metadata.LastUpdatedTime < '{dt_to_ts(end_date)}'"
            f" ORDER BY Metadata.LastUpdatedTime ASC"
            f" STARTPOSITION {query_offset}"
            f" MAXRESULTS {PAGE_SIZE}"
        )

        _, body = await http.request_stream(
            log,
            url,
            headers={"Accept": "application/json"},
            params={"query": query_statement, "minorversion": 75},
        )

        processor = IncrementalJsonProcessor(
            body(),
            f"QueryResponse.{model.table_name}.item",
            model,
        )

        yielded_items = 0
        async for item in processor:
            yielded_items += 1
            yield item

        if yielded_items < PAGE_SIZE:
            break

        query_offset += PAGE_SIZE


async def backfill_entity(
    model: type[T],
    http: HTTPSession,
    base_url: str,
    realm_id: str,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[T | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    page_ts = datetime.fromisoformat(page)
    timeout = datetime.now(tz=UTC) + TARGET_FETCH_PAGE_INVOCATION_RUN_TIME
    timeout_checkpoint = None

    log.info(
        "Initiating backfill fetch",
        {"page_ts": page_ts, "cutoff": cutoff, "timeout": timeout},
    )

    if page_ts >= cutoff:
        return

    async for item in query_entity(
        model, page_ts, cutoff, http, base_url, realm_id, log
    ):
        if (
            timeout_checkpoint is not None
            and item.MetaData.LastUpdatedTime > timeout_checkpoint
        ):
            yield timeout_checkpoint.isoformat(timespec="seconds")
            return

        if datetime.now(tz=UTC) > timeout and timeout_checkpoint is None:
            # We want to make sure the second being worked on is fully covered
            # before checkpointing
            timeout_checkpoint = item.MetaData.LastUpdatedTime

        yield item

    return


async def fetch_entity(
    model: type[T],
    http: HTTPSession,
    base_url: str,
    realm_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[T | LogCursor, None]:
    # QuickBooks does offer a CDC endpoint
    # (https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/change-data-capture)
    # which would cut down our request count from a best case scenario of
    # 1 * number of enabled streams to just 1.
    #
    # However, its results are capped to 1000 items with no pagination.
    #
    # We've had to implement more traditional SQL-like per-entity queries for
    # backfills, so we opted to keep it simple and reuse the same logic for
    # incremental replications.
    #
    # The query's exclusive lower bound guarantees that any returned item has
    # LastUpdatedTime strictly greater than the prior cursor, which both
    # ensures forward progress and avoids re-fetching boundary items each
    # poll. The upper bound is held back from "now" by INCREMENTAL_FETCH_DELAY
    # so that the cursor (which we source from item metadata to avoid
    # trusting our local wall clock against QuickBooks') reflects a state
    # we've confidently observed in full.
    assert isinstance(log_cursor, datetime)

    upper_bound = datetime.now(tz=UTC).replace(microsecond=0) - INCREMENTAL_FETCH_DELAY

    if upper_bound <= log_cursor:
        return

    log.info(
        "Starting incremental fetch",
        {"log_cursor": log_cursor, "upper_bound": upper_bound},
    )

    new_log_cursor = log_cursor
    async for item in query_entity(
        model, log_cursor, upper_bound, http, base_url, realm_id, log
    ):
        new_log_cursor = max(item.MetaData.LastUpdatedTime, new_log_cursor)
        yield item

    if new_log_cursor != log_cursor:
        yield new_log_cursor


def dt_to_ts(dt: AwareDatetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")
