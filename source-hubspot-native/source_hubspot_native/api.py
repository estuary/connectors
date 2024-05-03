import asyncio
import itertools
import json
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
)

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from .models import (
    Association,
    BatchResult,
    CRMObject,
    CustomObjectSchema,
    CustomObjectSearchResult,
    EmailEvent,
    EmailEventsResponse,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    PageResult,
    Properties,
    SearchPageResult,
)

HUB = "https://api.hubapi.com"

# Various HubSpot APIs (namely, v3 search and email events) are eventually consistent, and may not
# produce monotonic ordering for very recent updates with respect to their updated timestamps. The
# best we can do is hold back reading very recent documents, and assume that slightly older
# documents are monotonic.  The value chosen here is largely arbitrary as there is no HubSpot
# documentation regarding this eventual consistency at the moment.
CONSISTENCY_HORIZON_DELTA: timedelta = timedelta(minutes=5)


properties_cache: dict[str, Properties]= {}

async def fetch_properties(
    log: Logger, http: HTTPSession, object_name: str
) -> Properties:
    if object_name in properties_cache:
        return properties_cache[object_name]

    url = f"{HUB}/crm/v3/properties/{object_name}"
    properties_cache[object_name] = Properties.model_validate_json(await http.request(log, url))
    for p in properties_cache[object_name].results:
        p.hubspotObject = object_name

    return properties_cache[object_name]


async def fetch_page(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    http: HTTPSession,
    object_name: str,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[CRMObject | str, None]:
    
    assert isinstance(cutoff, datetime)

    url = f"{HUB}/crm/v3/objects/{object_name}"

    output: list[CRMObject] = []

    properties = await fetch_properties(log, http, object_name)

    # There is a limit on how large a URL can be when making a GET request to HubSpot. Exactly what
    # this limit is is a bit mysterious to me. Empirical testing indicates that a single property
    # with an alphanumeric name ~32k characters long will push it over the limit. However, this ~32k
    # length limit assumption does not seem to hold when considering huge amounts of shorter
    # property names, which is more common. The chunk size here is a best guess then based on
    # something that works for actual rea;-world use cases. Note that the length is effectively
    # doubled by the fact that we request both properties and properties with history.
    #
    # If this calculation results in more than one chunk of properties to retrieve based on the
    # cumulative byte lengths, we will issue multiple requests for different sets of properties and
    # combine the results together in the output documents.
    chunked_properties = _chunk_props(
        [p.name for p in properties.results if not p.calculated],
        5 * 1024,
    )

    for props in chunked_properties:
        property_names = ",".join(props)

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

        for idx, doc in enumerate(result.results):
            if idx == len(output):
                # Populate the document at idx the first time around.
                output.append(doc)
            else:
                # When fetching values for additional chunks of properties, we require that
                # documents are received in the same order.
                assert output[idx].id == doc.id

                # An additional requirement is that if a document gets updated while we are fetching
                # a separate chunk of properties, that its `updatedAt` value will be increased to
                # the point that it will be beyond the cutoff for the backfill and the updated
                # document will be captured via the incremental stream. This will prevent any
                # inconsistencies arising from a document being updated in the midst of us fetching
                # its properties.
                if output[idx].updatedAt != doc.updatedAt:
                    assert doc.updatedAt >= cutoff
                    output[idx].updatedAt = doc.updatedAt # We'll discard this document per the check a little further down.

                output[idx].properties.update(doc.properties)
                output[idx].propertiesWithHistory.update(doc.propertiesWithHistory)

    for doc in output:
        if doc.updatedAt < cutoff:
            yield doc

    if result.paging:
        yield result.paging.next.after


async def fetch_batch(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    object_name: str,
    ids: Iterable[str],
) -> BatchResult[CRMObject]:

    url = f"{HUB}/crm/v3/objects/{object_name}/batch/read"
    properties = await fetch_properties(log, http, object_name)
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
    object_name: str,
    ids: Iterable[str],
    associated_entity: str,
) -> BatchResult[Association]:
    url = f"{HUB}/crm/v4/associations/{object_name}/{associated_entity}/batch/read"
    input = {"inputs": [{"id": id} for id in ids]}

    return BatchResult[Association].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )


async def fetch_batch_with_associations(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    object_name: str,
    ids: list[str],
) -> BatchResult[CRMObject]:

    batch, all_associated = await asyncio.gather(
        fetch_batch(log, cls, http, object_name, ids),
        asyncio.gather(
            *(
                fetch_association(log, cls, http, object_name, ids, e)
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
    object_name: str,
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

    for batch_it in itertools.batched(recent, 50):
        batch = list(batch_it)

        documents: BatchResult[CRMObject] = await fetch_batch_with_associations(
            log, cls, http, object_name, [id for _, id in batch]
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


# TODO(whb): As we add more object types that might need to use the search API for getting recent
# changes, this function will probably make sense to generalize.
async def fetch_recent_custom_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    since: datetime,
    cursor: PageCursor,
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    
    url = f"{HUB}/crm/v3/objects/{object_name}/search"

    # The search API has known inconsistencies with very recent data.
    horizon = datetime.now(tz=UTC) - CONSISTENCY_HORIZON_DELTA

    if horizon <= since:
        return [], None

    input = {
        "filters": [
            {
                "propertyName": "hs_lastmodifieddate",
                "operator": "BETWEEN",
                "highValue": horizon.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "value": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        ],
        # Sort newest to oldest since paging stops when an item as old or older than `since` is
        # encountered per the handling in `fetch_changes`.
        "sorts": [
            {
                "propertyName": "hs_lastmodifieddate",
                "direction": "DESCENDING"
            }
        ],
        "limit": 100,
    }

    if cursor:
        input["after"] = cursor

    result: SearchPageResult[CustomObjectSearchResult] = SearchPageResult[CustomObjectSearchResult].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )

    newCursor = result.paging.next.after if result.paging else None

    return ((r.properties.hs_lastmodifieddate, str(r.id)) for r in result.results), newCursor


async def list_custom_objects(
    log: Logger,
    http: HTTPSession,
) -> list[str]:
    
    url = f"{HUB}/crm/v3/schemas"
    
    # Note: The schemas endpoint always returns all items in a single call, so there's never
    # pagination.
    result = PageResult[CustomObjectSchema].model_validate_json(
        await http.request(log, url, method="GET")
    )

    return [r.name for r in result.results if not r.archived]


async def fetch_email_events_page(
    http: HTTPSession,
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


async def fetch_recent_email_events(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[EmailEvent | LogCursor, None]:

    assert isinstance(log_cursor, datetime)

    url = f"{HUB}/email/public/v1/events"

    # The email events API has known inconsistencies with very recent data.
    horizon = datetime.now(tz=UTC) - CONSISTENCY_HORIZON_DELTA

    if horizon <= log_cursor:
        return

    input: Dict[str, Any] = {
        "startTimestamp": _dt_to_ms(log_cursor),
        "endTimestamp": _dt_to_ms(horizon),
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


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)

def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _chunk_props(props: list[str], max_bytes: int) -> list[list[str]]:
    result: list[list[str]] = []

    current_chunk: list[str] = []
    current_size = 0

    for p in props:
        sz = len(p.encode('utf-8'))

        if current_size + sz > max_bytes:
            result.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(p)
        current_size += sz

    if current_chunk:
        result.append(current_chunk)

    return result
