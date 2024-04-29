import asyncio
import itertools
import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from .models import (
    EMAIL_EVENT_HORIZON_DELTA,
    Association,
    BatchResult,
    CRMObject,
    EmailEvent,
    EmailEventsResponse,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    PageResult,
    Properties,
)

HUB = "https://api.hubapi.com"


async def fetch_properties(
    log: Logger, cls: type[CRMObject], http: HTTPSession
) -> Properties:
    if p := getattr(cls, "CACHED_PROPERTIES", None):
        return p

    url = f"{HUB}/crm/v3/properties/{cls.NAME}"
    cls.CACHED_PROPERTIES = Properties.model_validate_json(await http.request(log, url))
    for p in cls.CACHED_PROPERTIES.results:
        p.hubspotObject = cls.NAME

    return cls.CACHED_PROPERTIES


async def fetch_page(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    http: HTTPSession,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[CRMObject | str, None]:

    url = f"{HUB}/crm/v3/objects/{cls.NAME}"
    properties = await fetch_properties(log, cls, http)
    property_names = ",".join(p.name for p in properties.results if not p.calculated)

    input = {
        "associations": ",".join(cls.ASSOCIATED_ENTITIES),
        "limit": 50, # Maximum when requesting history.
        "properties": property_names,
        "propertiesWithHistory": property_names,
    }
    if page:
        input["after"] = page

    _cls: Any = cls  # Silence mypy false-positive.
    result: PageResult[CRMObject] = PageResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=input)
    )

    for doc in result.results:
        if doc.updatedAt < cutoff:
            yield doc

    if result.paging:
        yield result.paging.next.after


async def fetch_batch(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    ids: Iterable[str],
) -> BatchResult[CRMObject]:

    url = f"{HUB}/crm/v3/objects/{cls.NAME}/batch/read"
    properties = await fetch_properties(log, cls, http)
    property_names = [p.name for p in properties.results if not p.calculated]

    input = {
        "inputs": [{"id": id} for id in ids],
        "properties": property_names,
        "propertiesWithHistory": property_names,
    }

    _cls: Any = cls  # Silence mypy false-positive.
    return BatchResult[_cls].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )


async def fetch_association(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    ids: Iterable[str],
    associated_entity: str,
) -> BatchResult[Association]:
    url = f"{HUB}/crm/v4/associations/{cls.NAME}/{associated_entity}/batch/read"
    input = {"inputs": [{"id": id} for id in ids]}

    return BatchResult[Association].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )


async def fetch_batch_with_associations(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    ids: list[str],
) -> BatchResult[CRMObject]:

    batch, all_associated = await asyncio.gather(
        fetch_batch(log, cls, http, ids),
        asyncio.gather(
            *(
                fetch_association(log, cls, http, ids, e)
                for e in cls.ASSOCIATED_ENTITIES
            )
        ),
    )
    # Index CRM records so we can attach associations.
    index = {r.id: r for r in batch.results}

    for associated_entity, associated in zip(cls.ASSOCIATED_ENTITIES, all_associated):
        for result in associated.results:
            setattr(
                index[result.from_.id],
                associated_entity,
                [to.toObjectId for to in result.to],
            )

    return batch


FetchRecentFn = Callable[
    [Logger, HTTPSession, datetime, PageCursor],
    Awaitable[tuple[Iterable[tuple[datetime, str]], PageCursor]],
]
"""
FetchRecentFn is the common signature of all fetch_recent_$thing functions below.
They return a page of (updatedAt, id) tuples for entities that have recently
changed, starting from PageCursor and optionally using the provided datetime to
lower-bound the look back. If there are additional pages, a PageCursor is returned.

Pages may return IDs in any order, but paging will stop only upon seeing an entry
that's as-old or older than the datetime cursor.
"""


async def fetch_changes(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    fetch_recent: FetchRecentFn,
    http: HTTPSession,
    # Remainder is common.FetchChangesFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CRMObject | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # Walk pages of recent IDs until we see one which is as-old
    # as `log_cursor`, or no pages remain.
    recent: list[tuple[datetime, str]] = []
    next_page: PageCursor = None

    while True:
        iter, next_page = await fetch_recent(log, http, log_cursor, next_page)

        for ts, id in iter:
            if ts > log_cursor:
                recent.append((ts, id))
            else:
                next_page = None

        if not next_page:
            break

    recent.sort()  # Oldest updates first.

    for batch_it in itertools.batched(recent, 100):
        batch = list(batch_it)

        documents: BatchResult[CRMObject] = await fetch_batch_with_associations(
            log, cls, http, [id for _, id in batch]
        )
        for doc in documents.results:
            yield doc

    if recent:
        yield recent[-1][0]


async def fetch_recent_companies(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/companies/v2/companies/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentCompanies.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.companyId))
        for r in result.results
    ), result.hasMore and result.offset


async def fetch_recent_contacts(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/contacts/v1/lists/recently_updated/contacts/recent"
    params = {"count": 100, "timeOffset": page} if page else {"count": 1}

    result = OldRecentContacts.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(int(r.properties.lastmodifieddate.value)), str(r.vid))
        for r in result.contacts
    ), result.has_more and result.time_offset


async def fetch_recent_deals(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/deals/v1/deal/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentDeals.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.dealId))
        for r in result.results
    ), result.hasMore and result.offset


async def fetch_recent_engagements(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/engagements/v1/engagements/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentEngagements.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(r.engagement.lastUpdated), str(r.engagement.id))
        for r in result.results
    ), result.hasMore and result.offset


async def fetch_recent_tickets(
    log: Logger, http: HTTPSession, since: datetime, cursor: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/crm-objects/v1/change-log/tickets"
    params = {"timestamp": int(since.timestamp() * 1000) - 1}

    result = TypeAdapter(list[OldRecentTicket]).validate_json(
        await http.request(log, url, params=params)
    )
    return ((_ms_to_dt(r.timestamp), str(r.objectId)) for r in result), None


async def fetch_email_events_changes(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[EmailEvent | LogCursor, None]:

    assert isinstance(log_cursor, datetime)

    url = f"{HUB}/email/public/v1/events"

    horizon = int((datetime.now(tz=UTC) - EMAIL_EVENT_HORIZON_DELTA).timestamp() * 1000)

    input: Dict[str, Any] = {
        "startTimestamp": _dt_to_ms(log_cursor),
        "endTimestamp": horizon,
        "limit": 1000,
    }

    max_ts = log_cursor

    # Email events are in reverse-chronological order. Page through them and yield them along the
    # way, which will not produce documents ordered chronologically, but it isn't practical to load
    # them all into memory and sort.
    while True:
        result = EmailEventsResponse.model_validate_json(
            await http.request(log, url, params=input)
        )

        for event in result.events:
            if event.created > max_ts:
                max_ts = event.created

            yield event

        if not result.hasMore:
            break

        input["offset"] = result.offset

    if max_ts != log_cursor:
        yield max_ts + timedelta(milliseconds=1) # startTimestamp is inclusive.


async def fetch_email_events_page(
    # Closed over via functools.partial:
    http: HTTPSession,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[EmailEvent | PageCursor, None]:
    
    assert isinstance(cutoff, datetime)
    
    url = f"{HUB}/email/public/v1/events"

    input: Dict[str, Any] = {
        "endTimestamp": _dt_to_ms(cutoff) - 1, # endTimestamp is inclusive.
        "limit": 1000,
    }
    if page:
        input["offset"] = page

    result = EmailEventsResponse.model_validate_json(
        await http.request(log, url, params=input)
    )

    for event in result.events:
        yield event

    if result.hasMore:
        yield result.offset


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)

def _dt_to_ms(dt: datetime) -> int:
    v = dt.timestamp() * 1000
    if v % 1 != 0:
        raise ValueError("Only millisecond precision is supported")
    return int(v)