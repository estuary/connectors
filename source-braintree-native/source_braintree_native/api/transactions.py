import asyncio
from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

import braintree
from braintree import BraintreeGateway

from estuary_cdk.http import HTTPSession

from .common import (
    HEADERS,
    TRANSACTION_SEARCH_LIMIT,
    braintree_xml_to_dict,
)
from ..models import (
    Transaction,
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
) -> AsyncGenerator[Transaction, None]:
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
        Transaction,
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
) -> AsyncGenerator[Transaction, None]:
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
        Transaction,
        ids,
        gateway,
        braintree.Transaction,
        log,
    ):
        yield doc


# Disbursed transactions
async def _fetch_transaction_ids_disbursed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    earliest_created_at: datetime,
    log: Logger,
) -> list[str]:
    url = f"{base_url}/transactions/advanced_search_ids"
    body = {
        "search": {
            "created_at": {
                "max": earliest_created_at.isoformat(),
            },
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
) -> AsyncGenerator[Transaction, None]:
    start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    log.info(f"Fetching disbursed transactions.", {
        "start": start_date,
        "end": end_date,
    })

    earliest_created_at = end_date + timedelta(days=1)
    while True:
        ids = await _fetch_transaction_ids_disbursed_between(
            http,
            base_url,
            start,
            end,
            earliest_created_at,
            log,
        )

        async for doc in fetch_by_ids(
            http,
            base_url,
            TRANSACTION_PATH_COMPONENT,
            TransactionSearchResponse,
            Transaction,
            ids,
            gateway,
            braintree.Transaction,
            log,
        ):
            yield doc

            if doc.created_at < earliest_created_at:
                earliest_created_at = doc.created_at

        if len(ids) < TRANSACTION_SEARCH_LIMIT:
            return


# Disputed transactions
async def _fetch_transaction_ids_disputed_between(
    http: HTTPSession,
    base_url: str,
    start: datetime,
    end: datetime,
    earliest_created_at: datetime,
    log: Logger,
) -> list[str]:
    url = f"{base_url}/transactions/advanced_search_ids"
    body = {
        "search": {
            "created_at": {
                "max": earliest_created_at.isoformat(),
            },
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
) -> AsyncGenerator[Transaction, None]:
    start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    log.info(f"Fetching disputed transactions.", {
        "start": start_date,
        "end": end_date,
    })

    earliest_created_at = end_date + timedelta(days=1)
    while True:
        ids = await _fetch_transaction_ids_disputed_between(
            http,
            base_url,
            start,
            end,
            earliest_created_at,
            log,
        )

        async for doc in fetch_by_ids(
            http,
            base_url,
            TRANSACTION_PATH_COMPONENT,
            TransactionSearchResponse,
            Transaction,
            ids,
            gateway,
            braintree.Transaction,
            log,
        ):
            yield doc

            if doc.created_at < earliest_created_at:
                earliest_created_at = doc.created_at

        if len(ids) < TRANSACTION_SEARCH_LIMIT:
            return
