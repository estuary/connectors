from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from braintree import BraintreeGateway
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .transactions import (
    determine_next_transaction_window_end,
    fetch_transactions_created_between,
    fetch_transactions_disbursed_between,
    fetch_transactions_updated_between,
    TRANSACTION_PATH_COMPONENT,
    TRANSACTION_SEARCH_LIMIT,
)
from .searchable_incremental_resource import (
    determine_next_searchable_resource_window_end_by_field,
    fetch_searchable_resources_created_between,
)
from .disputes import fetch_disputes_received_between
from ..models import IncrementalResource, SearchResponse, IncrementalResourceBraintreeClass


# Disputes are searched using the received_date field, which is a date not a datetime. Using a window size
# smaller than 24 hours (1 day) would make the stream slower than necessary & fetch duplicate results from
# Braintree, so we enforce that dispute date windows must be at least 24 hours wide.
MIN_DISPUTES_WINDOW_SIZE = 24

# We've observed updates in Braintree sometimes take a few seconds to appear
# in the API, and the connector has missed updated transactions due to this.
# This could be due to minor distributed clock or eventual consistency issues.
# To address both, we can ensure the incremental tasks never fetch data more
# recent than 15 minutes in the past. This caps how "real-time" transactions are,
# but it's a simple fix that's easily rolled back if we come up with a better solution.
# The default resource interval is already 5 minutes, so the connector isn't really
# "real-time" anyway.
LAG = timedelta(minutes=15)

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def _are_same_day(start: datetime, end: datetime) -> bool:
    return start.date() == end.date()


async def fetch_transactions(
    http: HTTPSession,
    base_url: str,
    braintree_gateway: BraintreeGateway,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    end = await determine_next_transaction_window_end(
        http=http,
        base_url=base_url,
        start=log_cursor,
        initial_end=min(
            log_cursor + timedelta(hours=window_size),
            datetime.now(tz=UTC) - LAG,
        ),
        log=log,
    )

    if log_cursor >= end:
        return

    # When a transaction's disbursement_details/disbursed_date is updated, none of the TRANSACTION_SEARCH_FIELDS
    # or the non-searchable updated_at field are updated. Meaning, only incrementally replicating based off
    # the TRANSACTION_SEARCH_FIELDS will cause the connector to miss updates to transactions' disbursement dates.
    # To capture these elusive updates, we search the previous day(s) for disbursed transactions and yield them
    # when the date window spans more than a single day.
    if not _are_same_day(log_cursor, end):
        async for doc in fetch_transactions_disbursed_between(
            http=http,
            base_url=base_url,
            start=log_cursor,
            end=(end - timedelta(days=1)),
            gateway=braintree_gateway,
            log=log,
        ):
            yield doc

    async for doc in fetch_transactions_updated_between(
        http,
        base_url,
        start=log_cursor,
        end=end,
        gateway=braintree_gateway,
        log=log,
    ):
        yield doc

    yield end


async def backfill_transactions(
    http: HTTPSession,
    base_url: str,
    braintree_gateway: BraintreeGateway,
    window_size: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = _str_to_dt(page)

    if start >= cutoff:
        return

    end = await determine_next_searchable_resource_window_end_by_field(
        http=http,
        base_url=base_url,
        path=TRANSACTION_PATH_COMPONENT,
        field_name="created_at",
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        log=log,
        search_limit=TRANSACTION_SEARCH_LIMIT,
    )

    async for doc in fetch_transactions_created_between(
        http,
        base_url,
        start,
        end,
        braintree_gateway,
        log,
    ):
        # Yield the document if it has been updated before the cutoff (i.e. the incremental task won't capture it).
        if doc.updated_at < cutoff:
            yield doc

    yield dt_to_str(end)


async def fetch_incremental_resources(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    braintree_gateway: BraintreeGateway,
    braintree_class: IncrementalResourceBraintreeClass,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    end = await determine_next_searchable_resource_window_end_by_field(
        http=http,
        base_url=base_url,
        path=path,
        field_name="created_at",
        start=log_cursor,
        initial_end=min(
            log_cursor + timedelta(hours=window_size),
            datetime.now(tz=UTC) - LAG,
        ),
        log=log,
    )

    if log_cursor >= end:
        return

    async for doc in fetch_searchable_resources_created_between(
        http,
        base_url,
        path,
        response_model,
        log_cursor,
        end,
        braintree_gateway,
        braintree_class,
        log,
    ):
        if doc.created_at > log_cursor:
            yield doc

        if doc.created_at > most_recent_created_at:
            most_recent_created_at = doc.created_at

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_incremental_resources(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    braintree_gateway: BraintreeGateway,
    braintree_class: IncrementalResourceBraintreeClass,
    window_size: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor
) -> AsyncGenerator[IncrementalResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = _str_to_dt(page)

    if start >= cutoff:
        return

    end = await determine_next_searchable_resource_window_end_by_field(
        http=http,
        base_url=base_url,
        path=path,
        field_name="created_at",
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        log=log,
    )

    async for doc in fetch_searchable_resources_created_between(
        http,
        base_url,
        path,
        response_model,
        start,
        end,
        braintree_gateway,
        braintree_class,
        log,
    ):
        if doc.created_at < cutoff:
            yield doc

    yield dt_to_str(end)


async def fetch_disputes(
        http: HTTPSession,
        base_url: str,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    window_end = log_cursor + timedelta(hours=max(window_size, MIN_DISPUTES_WINDOW_SIZE))
    end = min(
        window_end,
        datetime.now(tz=UTC) - LAG,
    ).replace(microsecond=0)

    if log_cursor >= end:
        return

    # Braintree does not let us search disputes based on the created_at field, and the received_date field is
    # the best alternative that Braintree exposes for searching. Since received_date can be earlier than
    # created_at, it's possible to miss records with a small enough window size when the stream is caught up to the present.
    # Ex: {'id': 'dispute_1', 'received_date': '2025-01-10', 'created_at': '2025-01-11T00:50:00Z'} could be missed with
    # a window size of 1 hour. To avoid missing these type of results, we move the start of the received_date search back one day.
    start = log_cursor - timedelta(days=1) if _are_same_day(log_cursor, end) else log_cursor

    async for doc in fetch_disputes_received_between(
        http,
        base_url,
        start,
        end,
        log,
    ):
        if doc.created_at > log_cursor:
            yield doc

        if doc.created_at > most_recent_created_at:
            most_recent_created_at = doc.created_at

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_disputes(
    http: HTTPSession,
    base_url: str,
    window_size: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = _str_to_dt(page)

    if start >= cutoff:
        return

    window_end = start + timedelta(hours=max(window_size, MIN_DISPUTES_WINDOW_SIZE))
    end = min(window_end, cutoff)

    # Due to the potential day difference between received_date and created_at,
    # always search the previous day for results created in this date window as well.
    search_start = start - timedelta(days=1)

    async for doc in fetch_disputes_received_between(
        http,
        base_url,
        search_start,
        end,
        log,
    ):
        if start < doc.created_at <= end:
            yield doc

    yield dt_to_str(end)
