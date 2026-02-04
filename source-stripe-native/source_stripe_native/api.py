from datetime import datetime, timedelta, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import AsyncGenerator, Literal
import re

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)


from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    Events,
    EventResult,
    BackfillResult,
    ListResult,
    StripeChildObject,
    StripeObjectNoEvents,
    StripeObjectWithEvents,
    SubscriptionItems,
    CONNECTED_ACCOUNT_EXEMPT_STREAMS,
)


API = "https://api.stripe.com/v1"
MAX_PAGE_LIMIT = 100

MISSING_RESOURCE_REGEX = r"resource_missing.+No such.+"
MISSING_LINE_ITEMS_FOR_CHECKOUT_SESSION_REGEX = r"Could not find line items for this Checkout Session. They may be missing for old testmode Checkout Sessions."
MISSING_RESOURCE_REGEXES = [
    MISSING_RESOURCE_REGEX,
    MISSING_LINE_ITEMS_FOR_CHECKOUT_SESSION_REGEX,
]
NOT_ON_LEGACY_BILLING_REGEX = r"Cannot list usage record summaries for.+because it is not on the legacy metered billing system"
DO_NOT_HAVE_PLATFORM_CONTROLS_REGEX = r"You cannot perform this request as you do not have Platform Controls for the Stripe Dashboard on the account"
EVENT_NO_LONGER_RETAINED = r"is no longer available because it's aged out of our retention policy"


# We have reason to believe the Stripe's Events API is eventually consistent. Examples of how to handle this
# are written in the following article: https://blog.sequin.io/finding-and-fixing-eventual-consistency-with-stripe-events.
# The approach in the article aims is to ensure the incremental tasks never fetch data more recent than 5 seconds in the past.
# This is a good approach, however, we err on the side of caution and set the lag to 1 minute to ensure we don't miss any events.
# This caps how "real-time" the streams can be but it's a simple fix that easily rolled back if we come up with a
# better solution. The default interval is already 5 minutes, so the connector is not really "real-time" anyway.
LAG = timedelta(minutes=1)
MIN_INCREMENTAL_INTERVAL = timedelta(minutes=1)


def add_event_types(
    params: dict[str, str | int], event_types: dict[str, Literal["c", "u", "d"]]
):
    """
    Adds the event types (i.e. keys) of the passed in `event_types` dict to a
    dict of query parameters.
    """
    for index, type in enumerate(event_types.keys()):
        params.update(
            {
                f"types[{index}]": type,
            }
        )

    return params


async def fetch_incremental(
    cls: type[StripeObjectWithEvents],
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[StripeObjectWithEvents | LogCursor, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental works by accessing stripe's Events API
    Each Resource that contains a valid Event Type is passed here.
    It works by calling the Events API, parsing the Events result
    and validating the incoming model class.
    If the document was created after the last log_cursor,
    yield this document and later yield the newest log_cursor.
    """
    assert isinstance(log_cursor, datetime)

    # We need to keep track of the IDs of documents we've seen and emitted
    # to avoid emitting duplicate documents in the same fetch_incremental call.
    # This is because Stripe's Events API will return the events in reverse-chronological order,
    # and we may see the same document multiple times if it has been updated causing us
    # to emit the updated document before the original document.
    seen_ids = set()
    iterating = True

    url = f"{API}/events"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    parameters = add_event_types(parameters, cls.EVENT_TYPES)
    end = datetime.now(tz=UTC) - LAG

    if end < log_cursor or (end - log_cursor < MIN_INCREMENTAL_INTERVAL):
        # Return early and sleep if the end date is before the log cursor
        # or the difference between the end date and log cursor is less than
        # the minimum incremental interval.
        return

    parameters["created[lte]"] = int(end.timestamp())
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id
    max_ts = log_cursor

    while iterating:
        _, body = await http.request_stream(log, url, params=parameters, headers=headers)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            EventResult.Data,
            EventResult,
        )

        last_event: EventResult.Data | None = None

        async for event in processor:
            last_event = event
            event_ts = _s_to_dt(event.created)

            # Update the most recent timestamp seen.
            if event_ts > max_ts:
                max_ts = event_ts

            # Emit documents if we haven't seen them.
            if event_ts >= log_cursor:
                if cls == Events:
                    doc = cls.model_validate(event.model_dump())
                    doc.meta_ = cls.Meta(op="c")
                else:
                    doc = cls.model_validate(event.data.object)
                    doc.meta_ = cls.Meta(op=cls.EVENT_TYPES[event.type])

                if account_id:
                    doc.account_id = account_id

                # ExternalAccountCards and ExternalBankAccount share the same events even though the
                # returned documents are different. We skip document if they aren't
                # for the current stream.
                if cls.NAME == "ExternalAccountCards" and doc.object != "card":
                    continue
                elif cls.NAME == "ExternalBankAccount" and doc.object != "bank_account":
                    continue

                if doc.id in seen_ids:
                    log.debug(
                        f"Skipping duplicate document with ID {doc.id} for stream {cls.NAME}."
                    )
                    continue
                yield doc
                seen_ids.add(doc.id)
            elif event_ts < log_cursor:
                iterating = False
                break

        async for _ in processor:
            # Consume any remaining items in the processor to ensure we don't leave any
            # unprocessed items in the stream.
            pass

        remainder = processor.get_remainder()

        if remainder.has_more is True and last_event:
            parameters["starting_after"] = last_event.id
        else:
            break

    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)  # startTimestamp is inclusive.
    elif connected_account_id and end > log_cursor:
        # If there were no events and we are capturing a connected account, move the cursor
        # forward. This is necessary since we can't process all connected accounts in a single
        # connector invocation, and we try to fairly rotated which connected accounts are processed
        # based on how old their incremental cursors are.
        yield end


async def fetch_backfill(
    cls: type[StripeObjectWithEvents],
    start_date: datetime,
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[StripeObjectWithEvents | str, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill works by accessing the stream own data API
    provided by stripe. These API's endpoints works by using pagination,
    with the response schema being the same on most cases (some edge-cases
    added extra fields, so BackfillResult model allows for extras).
    It works by calling each individual stream endpoint and parsing each result,
    created before the cutoff, with its model.

    If a document is equal-to or older-than the start_date, this means we've reached the limit
    set by the user, and the stream halts.
    """
    assert isinstance(cutoff, datetime)

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    if page:
        parameters["starting_after"] = page

    if cls.NAME == "Subscriptions" or cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"

    try:
        _, body = await http.request_stream(log, url, params=parameters, headers=headers)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            cls,
            BackfillResult[cls]
        )

        last_doc: StripeObjectWithEvents | None = None
        async for doc in processor:
            last_doc = doc
            # Sometimes API results don't have a created field. These may have been added by some legacy Stripe system
            # that didn't add the created field. Since we can't determine when these results were created, treat them
            # like other results within the backfill window and yield them.
            doc_ts = _s_to_dt(doc.created) if doc.created is not None else None

            if account_id:
                doc.account_id = account_id

            if doc_ts == start_date:
                # Yield final document for reference
                yield doc
                return
            elif doc_ts is not None and doc_ts < start_date:
                return
            elif doc_ts is None or doc_ts < cutoff:
                yield doc

    except HTTPError as err:
        # Once we reach the very last event when backfilling Events, it's possible for the event
        # we checkpointed as the page token to age out of Stripe's retention window. This
        # means we've reached the end of the events stream & the backfill is complete.
        if err.code == 400 and EVENT_NO_LONGER_RETAINED in err.message and cls == Events:
            return
        else:
            raise

    async for _ in processor:
        # Consume any remaining items in the processor to ensure we don't leave any
        # unprocessed items in the stream.
        pass

    remainder = processor.get_remainder()
    if remainder.has_more and last_doc:
        yield last_doc.id
    else:
        return


async def fetch_incremental_substreams(
    cls: type[StripeObjectWithEvents],
    cls_child: type[StripeChildObject],
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[StripeChildObject | LogCursor, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_substreams works very similar to
    fetch_incremental method. The only variation is that
    the resulting data serves has the search object for the next
    stream (child stream). With that, a new iteration happens inside
    this method exclusively for the child stream.
    """
    assert isinstance(log_cursor, datetime)

    # We need to keep track of the IDs of documents we've seen and emitted
    # to avoid emitting duplicate documents in the same fetch_incremental call.
    # This is because Stripe's Events API will return the events in reverse-chronological order,
    # and we may see the same document multiple times if it has been updated causing us
    # to emit the updated document before the original document.
    seen_ids = set()
    iterating = True

    url = f"{API}/events"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    parameters = add_event_types(parameters, cls_child.EVENT_TYPES)
    end = datetime.now(tz=UTC) - LAG

    if end < log_cursor or (end - log_cursor < MIN_INCREMENTAL_INTERVAL):
        # Return early and sleep if the end date is before the log cursor
        # or the difference between the end date and log cursor is less than
        # the minimum incremental interval.
        return

    parameters["created[lte]"] = int(end.timestamp())
    max_ts = log_cursor
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    while iterating:
        events = EventResult.model_validate_json(
            await http.request(
                log, url, method="GET", params=parameters, headers=headers
            )
        )

        for event in events.data:
            event_ts = _s_to_dt(event.created)

            # Update the most recent timestamp seen.
            if event_ts > max_ts:
                max_ts = event_ts

            # Emit documents if we haven't seen them.
            if event_ts >= log_cursor:
                parent_data = cls.model_validate(event.data.object)
                search_name = cls.SEARCH_NAME
                id = parent_data.id
                child_data = _capture_substreams(
                    cls_child, search_name, id, parent_data, account_id, http, log
                )

                if child_data is None:
                    pass  # move to next customer
                async for doc in child_data:
                    doc.meta_ = cls_child.Meta(op=cls.EVENT_TYPES[event.type])
                    if account_id:
                        doc.account_id = account_id
                    if doc.id in seen_ids:
                        log.debug(
                            f"Skipping duplicate document with ID {doc.id} for stream {cls_child.NAME}."
                        )
                        continue
                    yield doc
                    seen_ids.add(doc.id)

            elif event_ts < log_cursor:
                iterating = False
                break
        if events.has_more is True:
            parameters["starting_after"] = events.data[-1].id
        else:
            break
    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)  # startTimestamp is inclusive.
    elif connected_account_id and end > log_cursor:
        # If there were no events and we are capturing a connected account, move the cursor
        # forward. This is necessary since we can't process all connected accounts in a single
        # connector invocation, and we try to fairly rotated which connected accounts are processed
        # based on how old their incremental cursors are.
        yield end


async def fetch_backfill_substreams(
    cls: type[StripeObjectWithEvents],
    cls_child: type[StripeChildObject],
    start_date: datetime,
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[StripeChildObject | str, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill_substreams works similar to fetch_backfill. The only variation is that
    the resulting data serves has the search object for the next
    stream (child stream). With that, a new iteration happens inside
    this method exclusively for the child stream.
    """
    assert isinstance(cutoff, datetime)

    search_name = cls.SEARCH_NAME
    url = f"{API}/{search_name}"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    if page:
        parameters["starting_after"] = page

    if cls.NAME == "Subscriptions" or cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"

    result = BackfillResult[cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters, headers=headers)
    )

    for doc in result.data:
        # Sometimes API results don't have a created field. These may have been added by some legacy Stripe system
        # that didn't add the created field. Since we can't determine when these results were created, treat them
        # like other results within the backfill window and yield them.
        doc_ts = _s_to_dt(doc.created) if doc.created is not None else None

        if doc_ts == start_date:
            parent_data = doc
            id = parent_data.id
            child_data = _capture_substreams(
                cls_child, search_name, id, parent_data, account_id, http, log
            )

            if child_data is None:
                return
            async for doc in child_data:
                doc.meta_ = cls_child.Meta(op="u")
                if account_id:
                    doc.account_id = account_id
                yield doc
            return

        elif doc_ts is not None and doc_ts < start_date:
            return

        elif doc_ts is None or doc_ts < cutoff:
            parent_data = doc
            id = parent_data.id
            child_data = _capture_substreams(
                cls_child, search_name, id, parent_data, account_id, http, log
            )

            if child_data is None:
                pass  # move to next customer
            async for doc in child_data:
                doc.meta_ = cls_child.Meta(op="u")
                if account_id:
                    doc.account_id = account_id
                yield doc

    if result.has_more:
        yield result.data[-1].id
    else:
        return


async def _capture_substreams(
    cls_child: type[StripeChildObject],
    search_name: str,
    id: str,
    parent_data,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
):
    """
    _capture_substreams works by handling the child_stream query and pagination.
    It requires the parent stream data, along with the item id and the parent search name.
    """

    child_url = f"{API}/{search_name}/{id}/{cls_child.SEARCH_NAME}"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id
        and cls_child.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    # Use stream specific URLs and query parameters.
    match cls_child.NAME:
        case "SetupAttempts":
            parameters.update({"setup_intent": id})
            child_url = f"{API}/{cls_child.SEARCH_NAME}"
        case "UsageRecords":
            child_url = f"{API}/subscription_items/{id}/{cls_child.SEARCH_NAME}"
        case "ExternalAccountCards":
            parameters.update({"object": "card"})
        case "ExternalBankAccount":
            parameters.update({"object": "bank_account"})

    # Fetch child records
    while True:
        # Per the Stripe docs, Persons can't be accessed for accounts where `account.controller.requirement_collection` is "stripe". It also
        # appears that when `account.controller.type` is "account", then Persons can't be accessed. This handling may need refined further
        # if we encounter further access issues and our understanding of the Stripe API deepens.
        # Docs reference: https://docs.stripe.com/api/persons
        if (
            cls_child.NAME == "Persons" and parent_data.controller["type"] == "account"
        ) or (
            cls_child.NAME == "Persons"
            and "requirement_collection" in parent_data.controller
            and parent_data.controller["requirement_collection"] is not None
            and parent_data.controller["requirement_collection"] == "stripe"
        ):
            break

        try:
            _, body = await http.request_stream(log, child_url, params=parameters, headers=headers)
            processor = IncrementalJsonProcessor(
                body(),
                "data.item",
                cls_child,
                ListResult[cls_child],
            )
            last_doc: StripeChildObject | None = None
            async for doc in processor:
                last_doc = doc
                if account_id:
                    doc.account_id = account_id
                yield doc
        except HTTPError as err:
            # It's possible for us to process events for deleted parent resources, making
            # the requests for the associated child resources fail. Stripe returns a 404
            # error & a message containing "resource_missing" and "No such" when this happens.
            if err.code == 404 and bool(
                re.search(regex, err.message, re.DOTALL)
                for regex in MISSING_RESOURCE_REGEX
            ):
                log.warning(
                    f"Missing resource error for URL {child_url}. Skipping to the next resource.",
                    {"code": err.code, "message": err.message},
                )
                break
            # If we request usage record summaries for subscription items that do not use the
            # legacy billing system, Stripe returns a 400 error & a pretty specific message.
            elif err.code == 400 and bool(
                re.search(NOT_ON_LEGACY_BILLING_REGEX, err.message, re.DOTALL)
            ):
                log.warning(
                    f"Cannot retrieve usage record summaries for {id} since it is not on Stripe's legacy billing system. Skipping to the next resource.",
                    {"code": err.code, "message": err.message},
                )
                break
            # Stripe has a "Stripe Connect" feature that lets external accounts be connected to a Stripe account, but
            # the Stripe account may not have appropriate permissions to read data for connected external accounts.
            elif err.code == 403 and bool(
                re.search(DO_NOT_HAVE_PLATFORM_CONTROLS_REGEX, err.message, re.DOTALL)
            ):
                log.warning(
                    f"Cannot retrieve {cls_child.NAME} for account {id}. Skipping to the next resource.",
                    {
                        "code": err.code,
                        "message": err.message,
                    },
                )
                break
            # Propagate all other errors.
            else:
                raise err

        remainder = processor.get_remainder()

        if remainder.has_more and last_doc:
            parameters["starting_after"] = last_doc.id
        else:
            break


async def fetch_incremental_no_events(
    cls: type[StripeObjectNoEvents],
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[StripeObjectNoEvents | LogCursor, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_no_events works very similar to fetch_backfilling. This method
    handles streams that do not have valid Event Types.
    It works by calling each individual stream endpoint and parsing each result,
    created after the last log_cursor, with its model.
    """
    assert isinstance(log_cursor, datetime)

    iterating = True

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    max_ts = log_cursor
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    end = datetime.now(tz=UTC) - LAG

    while iterating:
        _, body = await http.request_stream(log, url, params=parameters, headers=headers)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            cls,
            ListResult[cls],
        )

        last_resource: StripeObjectNoEvents | None = None
        async for resource in processor:
            last_resource = resource
            resource_ts = _s_to_dt(resource.created)

            # Update the most recent timestamp seen.
            if resource_ts > max_ts:
                max_ts = resource_ts

            # Emit documents if we haven't seen them yet.
            if resource_ts >= log_cursor:
                doc = cls.model_validate(resource)
                if account_id:
                    doc.account_id = account_id
                yield doc
            elif resource_ts < log_cursor:
                iterating = False
                break

        async for _ in processor:
            # Consume any remaining items in the processor to ensure we don't leave any
            # unprocessed items in the stream.
            pass

        remainder = processor.get_remainder()
        if remainder.has_more is True and last_resource:
            parameters["starting_after"] = last_resource.id
        else:
            break

    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)  # startTimestamp is inclusive.
    elif connected_account_id and end > log_cursor:
        # If there were no events and we are capturing a connected account, move the cursor
        # forward. This is necessary since we can't process all connected accounts in a single
        # connector invocation, and we try to fairly rotated which connected accounts are processed
        # based on how old their incremental cursors are.
        yield end


async def fetch_incremental_usage_records(
    cls: SubscriptionItems,
    cls_child: type[StripeChildObject],
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[StripeChildObject | LogCursor, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_usage_records works similar to fetch_incremental_substreams.
    The only variation is that the resulting data from the child stream
    needs aditional processing
    """
    assert isinstance(log_cursor, datetime)

    # We need to keep track of the IDs of documents we've seen and emitted
    # to avoid emitting duplicate documents in the same fetch_incremental call.
    # This is because Stripe's Events API will return the events in reverse-chronological order,
    # and we may see the same document multiple times if it has been updated causing us
    # to emit the updated document before the original document.
    seen_ids = set()
    iterating = True

    url = f"{API}/events"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    parameters = add_event_types(parameters, cls_child.EVENT_TYPES)
    end = datetime.now(tz=UTC) - LAG

    if end < log_cursor or (end - log_cursor < MIN_INCREMENTAL_INTERVAL):
        # Return early and sleep if the end date is before the log cursor
        # or the difference between the end date and log cursor is less than
        # the minimum incremental interval.
        return

    parameters["created[lte]"] = int(end.timestamp())
    max_ts = log_cursor
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id and cls.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    while iterating:
        events = EventResult.model_validate_json(
            await http.request(
                log, url, method="GET", params=parameters, headers=headers
            )
        )

        for event in events.data:
            event_ts = _s_to_dt(event.created)

            # Update the most recent timestamp seen.
            if event_ts > max_ts:
                max_ts = event_ts

            # Emit documents if we haven't seen them.
            if event_ts >= log_cursor:
                parent_data = cls.model_validate(event.data.object)
                search_name = cls.SEARCH_NAME
                for item in parent_data.items.data:
                    id = item.id
                    child_data = _capture_substreams(
                        cls_child, search_name, id, parent_data, account_id, http, log
                    )

                    if child_data is None:
                        pass  # move to next item
                    async for doc in child_data:
                        doc.meta_ = cls_child.Meta(op=cls.EVENT_TYPES[event.type])
                        if doc.id in seen_ids:
                            log.debug(
                                f"Skipping duplicate document with ID {doc.id} for stream {cls_child.NAME}."
                            )
                            continue
                        yield doc
                        seen_ids.add(doc.id)

            elif event_ts < log_cursor:
                iterating = False
                break

        if events.has_more is True:
            parameters["starting_after"] = events.data[-1].id
        else:
            break

    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1)  # startTimestamp is inclusive.
    elif connected_account_id and end > log_cursor:
        # If there were no events and we are capturing a connected account, move the cursor
        # forward. This is necessary since we can't process all connected accounts in a single
        # connector invocation, and we try to fairly rotated which connected accounts are processed
        # based on how old their incremental cursors are.
        yield end


async def fetch_backfill_usage_records(
    cls: SubscriptionItems,
    cls_child: type[StripeChildObject],
    start_date: datetime,
    platform_account_id: str | None,
    connected_account_id: str | None,
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[StripeChildObject | str, None]:
    """
    Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill_usage_records works similar to fetch_backfill_substreams.
    The only variation is that the resulting data from the child stream
    needs aditional processing
    """
    assert isinstance(cutoff, datetime)

    search_name = cls.SEARCH_NAME
    url = f"{API}/{search_name}"
    parameters: dict[str, str | int] = {"limit": MAX_PAGE_LIMIT}
    headers = {}
    account_id = (
        connected_account_id
        if connected_account_id
        and cls_child.NAME not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
        else platform_account_id
    )
    if account_id:
        headers["Stripe-Account"] = account_id

    if page:
        parameters["starting_after"] = page

    if cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"

    result = BackfillResult[cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters, headers=headers)
    )

    for doc in result.data:
        doc_ts = _s_to_dt(doc.created)
        if doc_ts == start_date:
            parent_data = doc
            for item in parent_data.items.data:
                id = item.id
                child_data = _capture_substreams(
                    cls_child, search_name, id, parent_data, account_id, http, log
                )

                if child_data is None:
                    pass
                async for doc in child_data:
                    doc.meta_ = cls_child.Meta(op="u")
                    yield doc
            return

        elif doc_ts < start_date:
            return

        elif doc_ts < cutoff:
            parent_data = doc
            for item in parent_data.items.data:
                id = item.id
                child_data = _capture_substreams(
                    cls_child, search_name, id, parent_data, account_id, http, log
                )

                if child_data is None:
                    pass  # move to next item
                async for doc in child_data:
                    doc.meta_ = cls_child.Meta(op="u")
                    yield doc

    if result.has_more:
        yield result.data[-1].id
    else:
        return


def _s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s, tz=UTC)
