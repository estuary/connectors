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
from estuary_cdk.capture.common import LogCursor, PageCursor

from ..models import (
    FullRefreshResource,
    IncrementalResource,
    Transaction,
)


# Searches return at most 10,000 results (50,000 for transaction searches). If we hit this limit,
# the connector could have missed data and we'll need to use smaller date windows.
SEARCH_LIMIT = 10_000
TRANSACTION_SEARCH_LIMIT = 50_000

# Disputes are searched using the received_date field, which is a date not a datetime. Using a window size
# smaller than 24 hours (1 day) would make the stream slower than necessary & fetch duplicate results from
# Braintree, so we enforce that dispute date windows must be at least 24 hours wide.
MIN_DISPUTES_WINDOW_SIZE = 24

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

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


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


# If more than search_limit results match the search criteria, only the most recent search_limit results
# are returned. To make sure we aren't missing results, _determine_window_end reduces the end date until
# fewer than search_limit results are returned & we can be confident no results are missed.
async def _determine_window_end(
        search_method,
        search_range_node_builder: Search.RangeNodeBuilder,
        start: datetime,
        initial_end: datetime,
        search_limit: int,
        log: Logger,
) -> tuple[datetime, ResourceCollection]:
    end = initial_end

    while True:
        collection: ResourceCollection = await asyncio.to_thread(
            search_method,
            search_range_node_builder.between(start, end),
        )

        if collection.maximum_size < search_limit:
            break

        window_size = (end - start) / 2

        # Braintree's datetimes have a resolution of seconds, so we remove microseconds from the window size.
        window_size = window_size - timedelta(microseconds=window_size.microseconds)

        log.debug(f"Number of results exceeds search limit. Reducing window size.", {
            "start": start,
            "end": end,
            "reduced end": start + window_size,
            "search limit": search_limit,
        })

        # It's unlikely a user will have enough data in Braintree that the connector will reduce the window size below 1 second,
        # but if it does happen the connector should raise an error since that stream will be stuck.
        if window_size < timedelta(seconds=1):
            raise RuntimeError("Window size is smaller than Braintree's datetime resolution of 1 second. Contact Estuary support for help addressing this error.")

        end = start + window_size

    return (end, collection)


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
    task_results = await asyncio.gather(
        *(
            _fetch_transaction_ids_for_single_field(field, braintree_gateway, start, end, log)
            for field in TRANSACTION_SEARCH_FIELDS
        )
    )

    id_set: set[str] = set()
    for task in task_results:
        for id in task:
            id_set.add(id)

    return list(id_set)


async def _determine_next_transaction_window_end(
    braintree_gateway: BraintreeGateway,
    start: datetime,
    latest_end: datetime,
    log: Logger,
) -> datetime:
    end = latest_end

    # Any of the transaction search fields could hit the TRANSACTION_SEARCH_LIMIT,
    # so we continue reassigning & reducing the end datetime until we know none
    # of the search fields will return TRANSACTION_SEARCH_LIMIT records.
    for field in TRANSACTION_SEARCH_FIELDS:
        end, _ = await _determine_window_end(
            search_method=braintree_gateway.transaction.search,
            search_range_node_builder=getattr(TransactionSearch, field),
            start = start,
            initial_end=end,
            search_limit=TRANSACTION_SEARCH_LIMIT,
            log=log,
        )

    return end


async def _fetch_transactions_disbursed_between(
    braintree_gateway: BraintreeGateway,
    start: datetime,
    end: datetime,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    # Both start_date and end_date are kept as datetimes to retain timezone information (e.g. UTC)
    # when searching. If timezone information is not included (like with dates), Braintree
    # defaults to using the Braintree account's timezone instead of UTC.
    start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    log.info(f"Fetching disbursed transactions.", {
        "start": start_date,
        "end": end_date,
    })

    earliest_created_at = end_date + timedelta(days=1)

    # Results from Braintree are always returned in descending order of the created_at field.
    # To make sure we get all transactions disbursed on the date, we iteratively reduce 
    # earliest_created_at and perform more searches until all results have been fetched.
    while True:
        collection: ResourceCollection = await asyncio.to_thread(
            braintree_gateway.transaction.search,
            TransactionSearch.disbursement_date.between(start_date, end_date),
            TransactionSearch.created_at.less_than_or_equal_to(earliest_created_at)
        )

        async for object in _async_iterator_wrapper(collection):
            doc = Transaction.model_validate(_braintree_object_to_dict(object))
            yield doc

            if doc.created_at < earliest_created_at:
                earliest_created_at = doc.created_at

        if collection.maximum_size < TRANSACTION_SEARCH_LIMIT:
            return


async def fetch_transactions(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    end = await _determine_next_transaction_window_end(
        braintree_gateway=braintree_gateway,
        log=log,
        start=log_cursor,
        latest_end=min(log_cursor + timedelta(hours=window_size), datetime.now(tz=UTC))
    )

    # When a transaction's disbursement_details/disbursed_date is updated, none of the TRANSACTION_SEARCH_FIELDS
    # or the non-searchable updated_at field are updated. Meaning, only incrementally replicating based off
    # the TRANSACTION_SEARCH_FIELDS will cause the connector to miss updates to transactions' disbursement dates.
    # To capture these elusive updates, we search the previous day(s) for disbursed transactions and yield them
    # when the date window spans more than a single day.
    if not _are_same_day(log_cursor, end):
        async for doc in _fetch_transactions_disbursed_between(
            braintree_gateway=braintree_gateway,
            start=log_cursor,
            end=end - timedelta(days=1),
            log=log
        ):
            yield doc

    ids = await _fetch_unique_updated_transaction_ids(
        braintree_gateway,
        log_cursor,
        end,
        log,
    )

    for index in range(0, len(ids), TRANSACTION_SEARCH_LIMIT):
        ids_chunk = ids[index:index+TRANSACTION_SEARCH_LIMIT]
        collection: ResourceCollection = await asyncio.to_thread(
            braintree_gateway.transaction.search,
            TransactionSearch.ids.in_list(ids_chunk),
        )

        async for object in _async_iterator_wrapper(collection):
            doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))
            yield doc

    yield end


async def backfill_transactions(
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

    end, collection = await _determine_window_end(
        search_method=braintree_gateway.transaction.search,
        search_range_node_builder=TransactionSearch.created_at,
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        search_limit=TRANSACTION_SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = Transaction.model_validate(_braintree_object_to_dict(object))

        # Yield the document if it has been updated before the cutoff (i.e. the incremental task won't capture it).
        if doc.updated_at < cutoff:
            yield doc

    yield _dt_to_str(end)


async def fetch_customers(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    end, collection = await _determine_window_end(
        search_method=braintree_gateway.customer.search,
        search_range_node_builder=CustomerSearch.created_at,
        start=log_cursor,
        initial_end=min(log_cursor + timedelta(hours=window_size), datetime.now(tz=UTC)),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc

            if doc.created_at > most_recent_created_at:
                most_recent_created_at = doc.created_at

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_customers(
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

    end, collection = await _determine_window_end(
        search_method=braintree_gateway.customer.search,
        search_range_node_builder=CustomerSearch.created_at,
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at < cutoff:
            yield doc

    yield _dt_to_str(end)


async def fetch_credit_card_verifications(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    end, collection = await _determine_window_end(
        search_method=braintree_gateway.verification.search,
        search_range_node_builder=CreditCardVerificationSearch.created_at,
        start=log_cursor,
        initial_end=min(log_cursor + timedelta(hours=window_size), datetime.now(tz=UTC)),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc

            if doc.created_at > most_recent_created_at:
                most_recent_created_at = doc.created_at

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_credit_card_verifications(
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

    end, collection = await _determine_window_end(
        search_method=braintree_gateway.verification.search,
        search_range_node_builder=CreditCardVerificationSearch.created_at,
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at < cutoff:
            yield doc

    yield _dt_to_str(end)


async def fetch_subscriptions(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    end, collection = await _determine_window_end(
        search_method=braintree_gateway.subscription.search,
        search_range_node_builder=SubscriptionSearch.created_at,
        start=log_cursor,
        initial_end=min(log_cursor + timedelta(hours=window_size), datetime.now(tz=UTC)),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at > log_cursor:
            yield doc

            if doc.created_at > most_recent_created_at:
                most_recent_created_at = doc.created_at

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_subscriptions(
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

    end, collection = await _determine_window_end(
        search_method=braintree_gateway.subscription.search,
        search_range_node_builder=SubscriptionSearch.created_at,
        start=start,
        initial_end=min(start + timedelta(hours=window_size), cutoff),
        search_limit=SEARCH_LIMIT,
        log=log
    )

    async for object in _async_iterator_wrapper(collection):
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if doc.created_at < cutoff:
            yield doc

    yield _dt_to_str(end)


def _are_same_day(start: datetime, end: datetime) -> bool:
    return start.date() == end.date()


async def fetch_disputes(
        braintree_gateway: BraintreeGateway,
        window_size: int,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    most_recent_created_at = log_cursor
    window_end = log_cursor + timedelta(hours=max(window_size, MIN_DISPUTES_WINDOW_SIZE))
    end = min(window_end, datetime.now(tz=UTC)).replace(microsecond=0)

    # Braintree does not let us search disputes based on the created_at field, and the received_date field is
    # the best alternative that Braintree exposes for searching. Since received_date can be earlier than
    # created_at, it's possible to miss records with a small enough window size when the stream is caught up to the present.
    # Ex: {'id': 'dispute_1', 'received_date': '2025-01-10', 'created_at': '2025-01-11T00:50:00Z'} could be missed with
    # a window size of 1 hour. To avoid missing these type of results, we move the start of the received_date search back one day.
    start = log_cursor - timedelta(days=1) if _are_same_day(log_cursor, end) else log_cursor

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

            if doc.created_at > most_recent_created_at:
                most_recent_created_at = doc.created_at

    if count >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(count, "disputes"))

    if most_recent_created_at > log_cursor:
        yield most_recent_created_at
    else:
        yield end


async def backfill_disputes(
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

    window_end = start + timedelta(hours=max(window_size, MIN_DISPUTES_WINDOW_SIZE))
    end = min(window_end, cutoff)

    # Due to the potential day difference between received_date and created_at,
    # always search the previous day for results created in this date window as well.
    search_start = start - timedelta(days=1)

    search_result = await asyncio.to_thread(
        braintree_gateway.dispute.search,
        DisputeSearch.received_date.between(search_start, end),
    )

    count = 0

    async for object in _async_iterator_wrapper(search_result.disputes):
        count += 1
        doc = IncrementalResource.model_validate(_braintree_object_to_dict(object))

        if start < doc.created_at <= end:
            yield doc

    if count >= SEARCH_LIMIT:
        raise RuntimeError(_search_limit_error_message(count, "disputes"))

    yield _dt_to_str(end)
