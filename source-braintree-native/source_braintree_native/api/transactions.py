import asyncio
from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Awaitable, Callable

import braintree
from braintree import BraintreeGateway

from estuary_cdk.http import HTTPSession

from .common import (
    HEADERS,
    TRANSACTION_SEARCH_LIMIT,
    braintree_xml_to_dict,
    reduce_window_end,
)
from ..models import (
    IncrementalResource,
    IdSearchResponse,
    TransactionSearchResponse,
)
from .searchable_incremental_resource import (
    determine_next_searchable_resource_window_end_by_field,
    fetch_searchable_resource_ids_by_field_between,
    fetch_by_ids,
)


TRANSACTION_SEARCH_FIELDS = [
    'authorization_expired_at',
    'authorized_at',
    'created_at',
    'failed_at',
    'gateway_rejected_at',
    'processor_declined_at',
    'settled_at',
    'submitted_for_settlement_at',
    'voided_at',
]
TRANSACTION_PATH_COMPONENT = "transactions"


async def determine_next_transaction_window_end(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    initial_end: datetime,
    log: Logger,
) -> datetime:
    end = initial_end

    # Any of the transaction search fields could hit the TRANSACTION_SEARCH_LIMIT,
    # so we continue reassigning & reducing the end datetime until we know none
    # of the search fields will return TRANSACTION_SEARCH_LIMIT records.
    for field in TRANSACTION_SEARCH_FIELDS:
        end = await determine_next_searchable_resource_window_end_by_field(
            http,
            base_url,
            TRANSACTION_PATH_COMPONENT,
            field,
            start,
            end,
            log,
            search_limit=TRANSACTION_SEARCH_LIMIT,
        )

    return end


async def _fetch_unique_updated_transaction_ids(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    log: Logger,
) -> list[str]:
    task_results = await asyncio.gather(
        *(
            fetch_searchable_resource_ids_by_field_between(
                http,
                base_url,
                TRANSACTION_PATH_COMPONENT,
                field,
                start,
                end,
                log,
                search_limit=TRANSACTION_SEARCH_LIMIT,
                enforce_limit=True,
            ) for field in TRANSACTION_SEARCH_FIELDS
        )
    )

    id_set: set[str] = set()
    for task in task_results:
        for id in task:
            id_set.add(id)

    return list(id_set)


async def fetch_transactions_updated_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    gateway: BraintreeGateway,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    ids = await _fetch_unique_updated_transaction_ids(
        http,
        base_url,
        start,
        end,
        log,
    )

    async for doc in fetch_by_ids(
        http,
        base_url,
        TRANSACTION_PATH_COMPONENT,
        TransactionSearchResponse,
        ids,
        gateway,
        braintree.Transaction,
        log,
    ):
        yield doc


async def fetch_transactions_created_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    gateway: BraintreeGateway,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    ids = await fetch_searchable_resource_ids_by_field_between(
        http,
        base_url,
        TRANSACTION_PATH_COMPONENT,
        "created_at",
        start,
        end,
        log,
        search_limit=TRANSACTION_SEARCH_LIMIT,
        enforce_limit=True
    )

    async for doc in fetch_by_ids(
        http,
        base_url,
        TRANSACTION_PATH_COMPONENT,
        TransactionSearchResponse,
        ids,
        gateway,
        braintree.Transaction,
        log,
    ):
        yield doc


# Type for fetch ID functions used by disbursed/disputed transaction searches
FetchIdsFunc = Callable[
    [datetime | None, datetime],
    Awaitable[list[str]]
]


async def _fetch_transaction_ids_with_created_at_windowing(
    fetch_ids: FetchIdsFunc,
    end_date: datetime,
) -> AsyncGenerator[tuple[list[str], datetime | None, datetime], None]:
    """
    Braintree's search API caps results at 50,000 and may omit records arbitrarily when
    hitting this limit - even records whose created_at falls within the returned range.
    Uncapped results (< 50,000) appear to be complete and consistent.

    This function uses binary search on created_at to narrow the window until results
    are uncapped, then shifts the window to cover older transactions until all are fetched.

    Args:
        fetch_ids: Async function(min_created_at, max_created_at) -> list of transaction IDs.
                   The primary search criteria (e.g., disbursement_date range) should be
                   baked into this callable.
        end_date: Used to initialize max_created_at.

    Yields:
        (ids, min_created_at, max_created_at) tuples for each uncapped result set.
    """
    max_created_at = end_date + timedelta(days=1)

    while True:
        min_created_at: datetime | None = None

        while True:
            ids = await fetch_ids(min_created_at, max_created_at)

            # Capped results (>= 50,000) may have gaps. Braintree can omit records
            # even within the returned created_at range. It appears that uncapped
            # results are complete and consistent.
            if len(ids) < TRANSACTION_SEARCH_LIMIT:
                break

            if min_created_at is None:
                min_created_at = max_created_at - timedelta(days=1)
            else:
                min_created_at = reduce_window_end(min_created_at, max_created_at)

        yield (ids, min_created_at, max_created_at)

        # An uncapped result with no min_created_at means we've captured all
        # transactions with created_at <= max_created_at.
        if min_created_at is None:
            return

        max_created_at = min_created_at


# Disbursed transactions
async def _fetch_transaction_ids_disbursed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    min_created_at: datetime | None,
    max_created_at: datetime,
    log: Logger,
) -> list[str]:
    url = f"{base_url}/transactions/advanced_search_ids"
    created_at_filter: dict[str, str] = {
        "max": max_created_at.isoformat(),
    }
    if min_created_at is not None:
        created_at_filter["min"] = min_created_at.isoformat()

    body = {
        "search": {
            "created_at": created_at_filter,
            "disbursement_date": {
                "min": start.isoformat(),
                "max": end.isoformat(),
            }
        }
    }

    response = IdSearchResponse.model_validate(
        braintree_xml_to_dict(
            await http.request(log, url, "POST", json=body, headers=HEADERS)
        )
    )

    return response.search_results.ids


async def fetch_transactions_disbursed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    gateway: BraintreeGateway,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    log.info(f"Fetching disbursed transactions.", {
        "start_date": start_date,
        "end_date": end_date,
    })

    async def fetch_ids(min_created_at: datetime | None, max_created_at: datetime) -> list[str]:
        return await _fetch_transaction_ids_disbursed_between(
            http, base_url, start_date, end_date, min_created_at, max_created_at, log, 
        )

    async for ids, min_created_at, max_created_at in _fetch_transaction_ids_with_created_at_windowing(fetch_ids, end_date):
        log.info(f"Found {len(ids)} disbursed transactions.", {
            "start_date": start_date,
            "end_date": end_date,
            "min_created_at": min_created_at,
            "max_created_at": max_created_at,
        })

        async for doc in fetch_by_ids(
            http,
            base_url,
            TRANSACTION_PATH_COMPONENT,
            TransactionSearchResponse,
            ids,
            gateway,
            braintree.Transaction,
            log,
        ):
            yield doc


# Disputed transactions
async def _fetch_transaction_ids_disputed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    min_created_at: datetime | None,
    max_created_at: datetime,
    log: Logger,
) -> list[str]:
    url = f"{base_url}/transactions/advanced_search_ids"
    created_at_filter: dict[str, str] = {
        "max": max_created_at.isoformat(),
    }
    if min_created_at is not None:
        created_at_filter["min"] = min_created_at.isoformat()

    body = {
        "search": {
            "created_at": created_at_filter,
            "dispute_date": {
                "min": start.isoformat(),
                "max": end.isoformat(),
            }
        }
    }

    response = IdSearchResponse.model_validate(
        braintree_xml_to_dict(
            await http.request(log, url, "POST", json=body, headers=HEADERS)
        )
    )

    return response.search_results.ids


async def fetch_transactions_disputed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    gateway: BraintreeGateway,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    log.info(f"Fetching disputed transactions.", {
        "start_date": start_date,
        "end_date": end_date,
    })

    async def fetch_ids(min_created_at: datetime | None, max_created_at: datetime) -> list[str]:
        return await _fetch_transaction_ids_disputed_between(
            http, base_url, start_date, end_date, min_created_at, max_created_at, log
        )

    async for ids, min_created_at, max_created_at in _fetch_transaction_ids_with_created_at_windowing(fetch_ids, end_date):
        log.info(f"Found {len(ids)} disputed transactions.", {
            "start_date": start_date,
            "end_date": end_date,
            "min_created_at": min_created_at,
            "max_created_at": max_created_at,
        })

        async for doc in fetch_by_ids(
            http,
            base_url,
            TRANSACTION_PATH_COMPONENT,
            TransactionSearchResponse,
            ids,
            gateway,
            braintree.Transaction,
            log,
        ):
            yield doc
