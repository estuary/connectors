import asyncio
from braintree import (
    BraintreeGateway,
    AddOnGateway,
    Configuration,
    TransactionSearch,
    CustomerSearch,
    CreditCardVerificationSearch,
    DisputeSearch,
    SubscriptionSearch,
    ResourceCollection,
    )
from braintree.attribute_getter import AttributeGetter
from braintree.paginated_collection import PaginatedCollection
from braintree.search import Search
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator
from estuary_cdk.capture.common import LogCursor

from .models import (
    FullRefreshResource,
    IncrementalResource,
)


# Searches return at most 10,000 results (50,000 for transaction searches). If we hit this limit,
# the connector could have missed data and we'll need to use smaller date windows.
SEARCH_LIMIT = 10_000
TRANSACTION_SEARCH_LIMIT = 50_000

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

CONVENIENCE_OBJECTS = [
    'gateway'
]


def _search_limit_error_message(count: int, name: str) -> str:
    msg = (
        f"{count} {name} returned in a single search which is "
        f"greater than or equal to Braintree's documented maximum for a single {name} search. "
        "Reduce the window size and backfill this stream."
    )

    return msg


def _braintree_object_to_dict(braintree_object):
        """
        Recursively convert a Braintree object and its nested objects to a dictionary.
        Convenience objects intended to make subsequent Braintree requests easier are ommitted.
        """
        if isinstance(braintree_object, (Configuration, AddOnGateway, BraintreeGateway)):
            return None
        data = braintree_object.__dict__.copy()
        # Remove convenience objects (like BraintreeGateways for making more requests).
        for key in CONVENIENCE_OBJECTS:
            data.pop(key, None)

        for key, value in data.items():
            if isinstance(value, AttributeGetter):
                data[key] = _braintree_object_to_dict(value)
            elif isinstance(value, datetime):
                data[key] =  value.replace(tzinfo=UTC)
            elif hasattr(value, "__dict__"):
                data[key] = _braintree_object_to_dict(value)
            elif isinstance(value, list):
                new_value = []
                for item in value:
                    if hasattr(item, "__dict__"):
                        new_value.append(_braintree_object_to_dict(item))
                    elif isinstance(item, datetime):
                        new_value.append(item.replace(tzinfo=UTC))
                    else:
                        new_value.append(item)

                data[key] = new_value

        # Remove private attributes.
        data.pop('_setattrs', None)
        return data


# Braintree's SDK makes synchronous API requests, which prevents multiple streams from
# sending concurrent API requests. asyncio.to_thread is used as a wrapper to run these
# synchronous API calls in a separate thread and avoid blocking the main thread's event loop.
async def _async_iterator_wrapper(collection: ResourceCollection | PaginatedCollection):
    def _braintree_iterator(collection: ResourceCollection | PaginatedCollection):
        for object in collection.items:
            yield object

    it = await asyncio.to_thread(_braintree_iterator, collection)

    for object in it:
        yield object


async def snapshot_resources(
        braintree_gateway: BraintreeGateway,
        gateway_property: str,
        gateway_response_field: str | None,
        log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    resources = await asyncio.to_thread(
        getattr(braintree_gateway, gateway_property).all
    )

    iterator = getattr(resources, gateway_response_field) if gateway_response_field else resources
    for object in iterator:
        yield FullRefreshResource.model_validate(_braintree_object_to_dict(object))


async def _fetch_transaction_ids_for_single_field(
    field: str,
    braintree_gateway: BraintreeGateway,
    start: datetime,
    end: datetime,
    log: Logger,
) -> list[str]:

    search_criteria: Search.RangeNodeBuilder = getattr(TransactionSearch, field)

    collection: ResourceCollection = await asyncio.to_thread(
        braintree_gateway.transaction.search,
        search_criteria.between(start, end),
    )

    if collection.maximum_size >= TRANSACTION_SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(collection.maximum_size, "transactions"))

    return collection.ids


async def _fetch_unique_updated_transaction_ids(
    braintree_gateway: BraintreeGateway,
    start: datetime,
    end: datetime,
    log: Logger,
) -> list[str]:
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(_fetch_transaction_ids_for_single_field(
            field,
            braintree_gateway,
            start,
            end,
            log
        )) for field in TRANSACTION_SEARCH_FIELDS]

    id_set: set[str] = set()
    for task in tasks:
        for id in task.result():
            id_set.add(id)

    return list(id_set)


async def _fetch_transaction_batch(
        braintree_gateway: BraintreeGateway,
        ids: list[str],
        queue: asyncio.Queue,
        log: Logger,
):
    collection: ResourceCollection = await asyncio.to_thread(
        braintree_gateway.transaction.search,
        TransactionSearch.ids.in_list(ids),
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))
        await queue.put(doc)


async def fetch_transactions(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    window_end = log_cursor + timedelta(hours=window_size)
    end = min(window_end, datetime.now(tz=UTC))

    ids = await _fetch_unique_updated_transaction_ids(
        braintree_gateway,
        log_cursor,
        end,
        log,
    )

    queue = asyncio.Queue(maxsize=10)

    async with asyncio.TaskGroup() as tg:
        for index in range(0, len(ids), TRANSACTION_SEARCH_LIMIT):
            tg.create_task(_fetch_transaction_batch(
                braintree_gateway,
                ids[index:index+TRANSACTION_SEARCH_LIMIT],
                queue,
                log,
            ))

        for _ in range(len(ids)):
            doc: IncrementalResource = await queue.get()
            yield doc
            queue.task_done()

    yield end


async def fetch_customers(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    window_end = log_cursor + timedelta(hours=window_size)
    end = min(window_end, datetime.now(tz=UTC))

    collection: ResourceCollection = await asyncio.to_thread(
        braintree_gateway.customer.search,
        CustomerSearch.created_at.between(log_cursor, end),
    )

    if collection.maximum_size >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(collection.maximum_size, "customers"))

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc
            most_recent_created_at = doc.created_at

    if end == window_end:
        yield window_end
    elif most_recent_created_at > log_cursor:
        yield most_recent_created_at


async def fetch_credit_card_verifications(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    window_end = log_cursor + timedelta(hours=window_size)
    end = min(window_end, datetime.now(tz=UTC))

    collection: ResourceCollection = await asyncio.to_thread(
        braintree_gateway.verification.search,
        CreditCardVerificationSearch.created_at.between(log_cursor, end),
    )

    if collection.maximum_size >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(collection.maximum_size, "credit card verifications"))

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc
            most_recent_created_at = doc.created_at

    if end == window_end:
        yield window_end
    elif most_recent_created_at > log_cursor:
        yield most_recent_created_at

async def fetch_subscriptions(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    window_end = log_cursor + timedelta(hours=window_size)
    end = min(window_end, datetime.now(tz=UTC))

    collection: ResourceCollection = await asyncio.to_thread(
        braintree_gateway.subscription.search,
        SubscriptionSearch.created_at.between(log_cursor, end),
    )

    if collection.maximum_size >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(collection.maximum_size, "subscriptions"))

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc
            most_recent_created_at = doc.created_at

    if end == window_end:
        yield window_end
    elif most_recent_created_at > log_cursor:
        yield most_recent_created_at


async def fetch_disputes(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    # The start date must be shifted back 1 day since we have to query Braintree using the received_date field,
    # which is less granular than the created_at cursor field (date vs. datetime).
    start = log_cursor - timedelta(days=1)
    window_end = log_cursor + timedelta(hours=window_size)
    end = min(window_end, datetime.now(tz=UTC))

    search_result = await asyncio.to_thread(
        braintree_gateway.dispute.search,
        DisputeSearch.received_date.between(start, end),
    )

    count = 0

    async for object in _async_iterator_wrapper(search_result.disputes):
        count += 1
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc
            most_recent_created_at = doc.created_at

    if count >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(count, "disputes"))

    if end == window_end:
        yield window_end
    elif most_recent_created_at > log_cursor:
        yield most_recent_created_at
