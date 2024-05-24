from datetime import datetime, timedelta, UTC
from decimal import Decimal
from estuary_cdk.http import HTTPSession
from logging import Logger
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator
import json

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)


from .models import EventResult, BackfillResult, ListResult


API = "https://api.stripe.com/v1"

async def fetch_incremental(
    cls,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:
    """ Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental works by accessing stripe's Events API
    Each Resource that contains a valid Event Type is passed here.
    It works by calling the Events API, parsing the Events result
    and validating the incoming model class.
    If the document was created after the last log_cursor, 
    yield this document and later yield the newest log_cursor.
    """
    stop = True

    url = f"{API}/events"
    parameters = {"type": cls.TYPES, "limit": 100}
    recent = []


    _cls: Any = cls  # Silence mypy false-positive

    while stop:
        result = EventResult.model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )
        for results in result.data:
            if _s_to_dt(results.created) > log_cursor:
                recent.append(_s_to_dt(results.created))
                doc = _cls.model_validate(results.data.object)
                doc.meta_ = _cls.Meta(op="u")
                yield doc
        
            elif _s_to_dt(results.created) < log_cursor:
                stop = False
                break
        if result.has_more is True:
            parameters["starting_after"] = result.data[-1].id
        else:
            break
    if recent:
        recent.sort()
        yield recent[-1]

async def fetch_backfill(
    cls,
    stop_date,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:
    """ Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill works by accessing the stream own data API
    provided by stripe. These API's endpoints works by using pagination,
    with the response schema being the same on most cases (some edge-cases
    added extra fields, so BackfillResult model allows for extras).
    It works by calling each individual stream endpoint and parsing each result,
    created before the cutoff, with its model.

    If a document is equal-to or older-than the stop_date, this means we've reached the limit
    set by the user, and the stream halts. 

    """

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters = {"limit": 100}

    if page:
        parameters["starting_after"] = page
    
    if cls.NAME == "Subscriptions" or cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"

    _cls: Any = cls  # Silence mypy false-positive
    result = BackfillResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )

    for doc in result.data:
        if _s_to_dt(doc.created) == stop_date:
            # Yield final document for reference
            doc.meta_ = _cls.Meta(op="u")
            yield doc
            return
        elif _s_to_dt(doc.created) < stop_date:
            return
        elif _s_to_dt(doc.created) < cutoff:
            doc.meta_ = _cls.Meta(op="u")
            yield doc

    if result.has_more:
        yield result.data[-1].id
    else:
        return

async def fetch_incremental_substreams(
    cls,
    cls_child,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

    """Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_substreams works very similar to
    fetch_incremental method. The only variation is that 
    the resulting data serves has the search object for the next
    stream (child stream). With that, a new iteration happens inside
    this method exclusively for the child stream.
    """

    stop = True

    url = f"{API}/events"
    parameters = {"type": cls.TYPES, "limit": 100}
    recent = []


    _cls: Any = cls  # Silence mypy false-positive

    while stop:
        result = EventResult.model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )
        for results in result.data:
            if _s_to_dt(results.created) > log_cursor:
                recent.append(_s_to_dt(results.created))

                parent_data = _cls.model_validate(results.data.object)
                search_name = _cls.SEARCH_NAME
                id = parent_data.id
                child_data = _capture_substreams(
                                    cls_child,
                                    search_name,
                                    id,
                                    parent_data,
                                    http,
                                    log
                                )

                if child_data is None:
                    pass # move to next customer
                async for doc in child_data:
                    doc.parent_id = parent_data.id
                    doc.meta_ = cls_child.Meta(op="u")
                    yield doc 
        
            elif _s_to_dt(results.created) < log_cursor:
                stop = False
                break
        if result.has_more is True:
            parameters["starting_after"] = result.data[-1].id
        else:
            break
    if recent:
        recent.sort()
        yield recent[-1]

async def fetch_backfill_substreams(
    cls,
    cls_child,
    stop_date,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:
    """Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill_substreams works similar to fetch_backfill. The only variation is that 
    the resulting data serves has the search object for the next
    stream (child stream). With that, a new iteration happens inside
    this method exclusively for the child stream.
    """

    _cls: Any = cls  # Silence mypy false-positive

    url = f"{API}/{_cls.SEARCH_NAME}"
    parameters = {"limit": 100}

    search_name = _cls.SEARCH_NAME


    if page:
        parameters["starting_after"] = page
    
    if _cls.NAME == "Subscriptions" or cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"


    
    result = BackfillResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )

    for doc in result.data:
        log.debug(f"{_s_to_dt(doc.created)}")
        if _s_to_dt(doc.created) == stop_date:
            parent_data = doc
            id = parent_data.id
            child_data = _capture_substreams(
                            cls_child,
                            search_name,
                            id,
                            parent_data,
                            http,
                            log
                        )

            if child_data is None:
                return
            async for doc in child_data:
                doc.parent_id = parent_data.id
                doc.meta_ = cls_child.Meta(op="u")
                yield doc 
            return

        elif _s_to_dt(doc.created) < stop_date:
            return

        elif _s_to_dt(doc.created) < cutoff:
            parent_data = doc
            id = parent_data.id
            child_data = _capture_substreams(
                            cls_child,
                            search_name,
                            id,
                            parent_data,
                            http,
                            log
                        )

            if child_data is None:
                pass # move to next customer
            async for doc in child_data:
                doc.parent_id = parent_data.id
                doc.meta_ = cls_child.Meta(op="u")
                yield doc 

    if result.has_more:
        yield result.data[-1].id
    else:
        return

async def _capture_substreams(
    cls_child,
    search_name,
    id,
    parent_data,
    http,
    log
):
    """_capture_substreams works by handling the child_stream query and pagination.
    it requires the parent stream data, along with the item id and the parent search name.
    """

    child_url = f"{API}/{search_name}/{id}/{cls_child.SEARCH_NAME}"
    parameters = cls_child.PARAMETERS
    if "subscription" in parameters.keys():
        parameters["subscription"] = id
        child_url = f"{API}/{cls_child.SEARCH_NAME}"
    elif "setup_intent" in parameters.keys():
        parameters["setup_intent"] = id
        child_url = f"{API}/{cls_child.SEARCH_NAME}"
    elif cls_child.NAME == "UsageRecords":
        child_url = f"{API}/subscription_items/{id}/{cls_child.SEARCH_NAME}"

    while True:
        if cls_child.NAME == "Persons" and parent_data.controller["requirement_collection"] == "stripe" :
            break
        result_child = ListResult[cls_child].model_validate_json(
        await http.request(log, child_url, method="GET", params=parameters)
    )

        for doc in result_child.data:
            yield doc

        if result_child.has_more is True:
            parameters["starting_after"] = result_child.data[-1].id
        elif result_child.has_more is False:
            break


async def fetch_incremental_no_events(
    cls,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:
    """ Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_no_events works very similar to fetch_backfilling. This method
    handles streams that do not have valid Event Types.
    It works by calling each individual stream endpoint and parsing each result,
    created after the last log_cursor, with its model.
    """

    stop = True

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters = {"limit": 100}
    recent = []


    _cls: Any = cls  # Silence mypy false-positive

    while stop:
        result = ListResult.model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )
        for results in result.data:
            if _s_to_dt(results["created"]) > log_cursor:
                recent.append(_s_to_dt(results["created"]))
                doc = _cls.model_validate(results)
                doc.meta_ = _cls.Meta(op="u")
                yield doc
        
            elif _s_to_dt(results["created"]) < log_cursor:
                stop = False
                break
        if result.has_more is True:
            parameters["starting_after"] = result.data[-1]["id"]
        else:
            break
    if recent:
        recent.sort()
        yield recent[-1]

async def fetch_incremental_usage_records(
    cls,
    cls_child,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

    """Note: Stripe's data is always served in reverse-chronological order.
    fetch_incremental_usage_records works similar to fetch_incremental_substreams. 
    The only variation is that the resulting data from the child stream
    needs aditional processing
    """

    stop = True

    url = f"{API}/events"
    parameters = {"type": cls.TYPES, "limit": 100}
    recent = []


    _cls: Any = cls  # Silence mypy false-positive

    while stop:
        result = EventResult.model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )
        for results in result.data:
            if _s_to_dt(results.created) > log_cursor:
                recent.append(_s_to_dt(results.created))

                parent_data = _cls.model_validate(results.data.object)
                search_name = _cls.SEARCH_NAME
                for item in parent_data.items.data:
                    id = item.id
                    child_data = _capture_substreams(
                                    cls_child,
                                    search_name,
                                    id,
                                    parent_data,
                                    http,
                                    log
                                )

                    if child_data is None:
                        pass # move to next item
                    async for doc in child_data:
                        doc.parent_id = parent_data.id
                        doc.meta_ = cls_child.Meta(op="u")
                        yield doc 

        
            elif _s_to_dt(results.created) < log_cursor:
                stop = False
                break
        if result.has_more is True:
            parameters["starting_after"] = result.data[-1].id
        else:
            break
    if recent:
        recent.sort()
        yield recent[-1]

async def fetch_backfill_usage_records(
    cls,
    cls_child,
    stop_date,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:

    """Note: Stripe's data is always served in reverse-chronological order.
    fetch_backfill_usage_records works similar to fetch_backfill_substreams. 
    The only variation is that the resulting data from the child stream
    needs aditional processing
    """

    _cls: Any = cls  # Silence mypy false-positive

    url = f"{API}/{_cls.SEARCH_NAME}"
    parameters = {"limit": 100}

    search_name = _cls.SEARCH_NAME

    if page:
        parameters["starting_after"] = page
    
    if _cls.NAME == "SubscriptionItems":
        parameters["status"] = "all"

    result = BackfillResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )

    for doc in result.data:
        if _s_to_dt(doc.created) == stop_date:
            parent_data = doc
            for item in parent_data.items.data:
                id = item.id
                child_data = _capture_substreams(
                                cls_child,
                                search_name,
                                id,
                                parent_data,
                                http,
                                log
                            )

                if child_data is None:
                    pass
                async for doc in child_data:
                    doc.parent_id = parent_data.id
                    doc.meta_ = cls_child.Meta(op="u")
                    yield doc 
            return

        elif _s_to_dt(doc.created) < stop_date:
            return

        elif _s_to_dt(doc.created) < cutoff:
            parent_data = doc
            for item in parent_data.items.data:
                id = item.id
                child_data = _capture_substreams(
                                cls_child,
                                search_name,
                                id,
                                parent_data,
                                http,
                                log
                            )

                if child_data is None:
                    pass # move to next item
                async for doc in child_data:
                    doc.parent_id = parent_data.id
                    doc.meta_ = cls_child.Meta(op="u")
                    yield doc 

    if result.has_more:
        yield result.data[-1].id
    else:
        return


def _s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s,tz=UTC)