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
#TODO add stop date to backfilling

async def fetch_incremental(
    cls,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

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

async def fetch_incremental_substreams(
    cls,
    cls_child,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

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
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters = {"limit": 100}

    if page:
        parameters["starting_after"] = page

    _cls: Any = cls  # Silence mypy false-positive
    result = BackfillResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )

    for doc in result.data:
        if _s_to_dt(doc.created) < cutoff:
            parent_data = doc
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

    child_url = f"{API}/{search_name}/{id}/{cls_child.SEARCH_NAME}"
    parameters = {"limit": 100}

    while True:
        result_child = ListResult[cls_child].model_validate_json(
        await http.request(log, child_url, method="GET", params=parameters)
    )

        for doc in result_child.data:
            yield doc

        if result_child.has_more is True:
            parameters["starting_after"] = result_child.data[-1].id
        elif result_child.has_more is False:
            break



async def fetch_backfill(
    cls,
    http: HTTPSession,
    log: Logger,
    page: str | None,
    cutoff: datetime,
)-> AsyncGenerator:

    url = f"{API}/{cls.SEARCH_NAME}"
    parameters = {"limit": 100}

    if page:
        parameters["starting_after"] = page

    _cls: Any = cls  # Silence mypy false-positive
    result = BackfillResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=parameters)
    )
    log.debug(f"{result.model_dump()}")

    for doc in result.data:
        if _s_to_dt(doc.created) < cutoff:
            doc.meta_ = _cls.Meta(op="u")
            log.debug(f"{doc}")
            yield doc

    if result.has_more:
        yield result.data[-1].id
    else:
        return

async def fetch_incremental_no_events(
    cls,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator:

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

def _s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s,tz=UTC)