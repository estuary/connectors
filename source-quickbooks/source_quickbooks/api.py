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

T = TypeVar("T", bound=QuickBooksEntity)


async def query_entity(
    model: type[T],
    start_date: datetime,
    end_date: datetime,
    http: HTTPSession,
    realm_id: str,
    log: Logger,
) -> AsyncGenerator[T, None]:
    url = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/query"
    query_offset = 0

    while True:
        query_statement = (
            f"SELECT * FROM {model.table_name}"
            f" WHERE Metadata.LastUpdatedTime >= '{dt_to_ts(start_date)}'"
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

    async for item in query_entity(model, page_ts, cutoff, http, realm_id, log):
        if (
            timeout_checkpoint is not None
            and item.MetaData.LastUpdatedTime >= timeout_checkpoint
        ):
            yield timeout_checkpoint.isoformat(timespec="seconds")
            return

        if datetime.now(tz=UTC) > timeout and timeout_checkpoint is None:
            # We want to make sure the second being worked on is fully covered
            # before checkpointing
            timeout_checkpoint = item.MetaData.LastUpdatedTime + timedelta(seconds=1)

        yield item

    return


async def fetch_entity(
    model: type[T],
    http: HTTPSession,
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
    assert isinstance(log_cursor, datetime)

    truncated_log_cursor = log_cursor.replace(microsecond=0)
    now = datetime.now(tz=UTC).replace(microsecond=0)

    if now == truncated_log_cursor:
        return

    log.info("Starting incremental fetch", {"log_cursor": log_cursor})

    new_log_cursor = log_cursor
    async for item in query_entity(model, log_cursor, now, http, realm_id, log):
        new_log_cursor = max(item.MetaData.LastUpdatedTime, new_log_cursor)
        yield item

    if new_log_cursor != log_cursor:
        yield new_log_cursor


def dt_to_ts(dt: AwareDatetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")
