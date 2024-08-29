import asyncio
import functools
import itertools
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
    Company,
    Contact,
    CustomObject,
    CustomObjectSchema,
    CustomObjectSearchResult,
    Deal,
    EmailEvent,
    EmailEventsResponse,
    Engagement,
    Names,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    PageResult,
    Properties,
    SearchPageResult,
    Ticket,
)

import source_hubspot_native.emitted_changes_cache as cache

HUB = "https://api.hubapi.com"

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


async def fetch_page_with_assocations(
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


async def _fetch_batch(
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
        _fetch_batch(log, cls, http, object_name, ids),
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
    [Logger, HTTPSession, datetime],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
'''
Returns a stream of (timestamp, key, document) tuples that represent a
potentially incomplete stream of very recent documents. The timestamp is used to
checkpoint the next log cursor. The key is used for updating the emitted changes
cache with the timestamp of the document.

Documents may be returned in any order, but iteration will be stopped upon
seeing an entry that's as-old or older than the datetime cursor.
'''

FetchDelayedFn = Callable[
    [Logger, HTTPSession, datetime, datetime],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
'''
Returns a stream of (timestamp, key, document) tuples that represent a complete
stream of not-so-recent documents. The key is used for seeing if a more recent
change event document has already been emitted by the FetchRecentFn.

Similar to FetchRecentFn, documents may be returned in any order and iteration
stops when seeing an entry that's as-old or older than the "since" datetime
cursor, which is the first datetime parameter. The second datetime parameter
represents an "until" value, and documents more recent than this are discarded
and should usually not even be retrieved if possible.
'''

# In-memory cache of how far along the "delayed" change stream is.
last_delayed_fetch_end: dict[str, datetime] = {}

delayed_offset = timedelta(hours=1)
delayed_fetch_minimum_window = timedelta(minutes=5)


async def process_changes(
    object_name: str,
    fetch_recent: FetchRecentFn,
    fetch_delayed: FetchDelayedFn,
    http: HTTPSession,
    # Remainder is common.FetchChangesFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Any | LogCursor, None]:
    '''
    High-level processing of changes event documents, where there is both a
    stream of very recent documents that is potentially incomplete, and a
    trailing stream that is delayed but 100% complete.

    Every time this function is invoked, the FetchRecentFn is called and any
    recent documents are emitted. If a sufficient amount of time has passed
    since the last time the delayed stream was polled, the FetchDelayedFn is
    called and those documents are emitted to ensure all documents are captured.
    This process will inevitably result in some duplicate documents being
    emitted, but some duplicates are better than missing data completely.

    The LogCursor is based on the position of the recent stream in time. To keep
    track of the progress of both the recent and delayed streams with a single
    LogCursor, there's a few of important details to note:

    - The delayed stream will only ever read documents as recent as
      `delayed_offset` (1 hour).
    - The delayed stream will always be polled up to the LogCursor timestamp
      minus `delayed_offset` if the LogCursor to be emitted would represent a
      delayed stream "window" of at least `delayed_fetch_minimum_window` (5
      minutes).
    - On connector initialization, the delayed stream is assumed to have last
      polled up to the persisted LogCursor, less `delayed_offset` _and_
      `delayed_fetch_minimum_window`. This ensures that when the connector
      restarts the delayed stream does not resume any later than where it last
      left off, and will in fact always resume from just prior to the oldest
      point it may have left off.
    - While the connector is running, the delayed stream progress is kept in
      memory.
    '''
    assert isinstance(log_cursor, datetime)

    fetch_recent_start_time = datetime.now(UTC)
    max_ts: datetime = log_cursor
    had_recent_docs = False
    async for ts, key, obj in fetch_recent(log, http, log_cursor):
        if ts > log_cursor:
            max_ts = max(max_ts, ts)
            cache.add_recent(object_name, key, ts)
            had_recent_docs = True
            yield obj
        else:
            break
    
    if not had_recent_docs:
        # No recent docs, but the delayed stream may still be advanced if enough
        # time has passed since it last was. A synthetic timestamp will be used
        # as the updated cursor in this case, and this increases the likelihood
        # that recent documents will be "missed" from the recent APIs. This
        # cursor will only ever be emitted if there periods of time where there
        # are no recent documents but there are some delayed documents, and we
        # are guaranteed to eventually get any documents that are missed by the
        # recent stream via this whole convoluted strategy anyway.
        if fetch_recent_start_time <= max_ts:
            # Unlikely clock drift edge case; bail out.
            return
        max_ts = fetch_recent_start_time

    delayed_fetch_next_start = last_delayed_fetch_end.setdefault(
        object_name, log_cursor - delayed_offset - delayed_fetch_minimum_window
    )
    delayed_fetch_next_end = max_ts - delayed_offset

    cache_hits = 0
    delayed_emitted = 0
    if delayed_fetch_next_end - delayed_fetch_next_start > delayed_fetch_minimum_window:
        # Poll the delayed stream for documents if we need to.
        async for ts, key, obj in fetch_delayed(log, http, delayed_fetch_next_start, delayed_fetch_next_end):
            if ts > delayed_fetch_next_end:
                # In case the FetchDelayedFn is unable to filter based on
                # `delayed_fetch_next_end`.
                continue
            elif ts > delayed_fetch_next_start:
                if cache.has_as_recent_as(object_name, key, ts):
                    cache_hits += 1
                    continue

                delayed_emitted += 1
                yield obj
            else:
                break

        last_delayed_fetch_end[object_name] = delayed_fetch_next_end
        evicted = cache.cleanup(object_name, delayed_fetch_next_end)

        log.info(
            "fetched delayed events for stream",
            {"object_name": object_name, "emitted": delayed_emitted, "cache_hits": cache_hits, "evicted": evicted, "new_size": cache.count_for(object_name)}
        )

    if had_recent_docs or delayed_emitted:
        yield max_ts


_FetchIdsFn = Callable[
    [PageCursor, int],
    Awaitable[tuple[Iterable[tuple[datetime, str]], PageCursor]],
]
'''
Returns a stream of object IDs that can be used to fetch the full object details
along with its associations. Used in `fetch_changes_with_associations`.

IDs may be returned in any order, but iteration will be stopped upon seeing an
entry that's as-old or older than the datetime cursor. Entries newer than the
until datetime will be discarded.
'''

async def fetch_changes_with_associations(
    object_name: str,
    cls: type[CRMObject],
    fetcher: _FetchIdsFn,
    log: Logger,
    http: HTTPSession,
    since: datetime,
    until: datetime | None = None
) -> AsyncGenerator[tuple[datetime, str, CRMObject], None]:
    
    # Walk pages of recent IDs until we see one which is as-old
    # as `since`, or no pages remain.
    recent: list[tuple[datetime, str]] = []
    next_page: PageCursor = None

    while True:
        iter, next_page = await fetcher(next_page, len(recent))

        for ts, id in iter:
            if until and ts > until:
                continue
            elif ts > since:
                # TODO(whb): It may be worth consulting the emitted changes
                # cache here to see if we have already emitted a more recent
                # change event before we do all the associations fetching work.
                # Before implementing that I'd like to make sure that the
                # top-level filtering works in production. Since the delayed
                # changes stream only runs every 5 minutes or so it shouldn't be
                # a huge load on the connector.
                recent.append((ts, id))
            else:
                next_page = None

        if not next_page:
            break

    recent.sort()  # Oldest updates first.

    for batch_it in itertools.batched(recent, 50):
        batch = list(batch_it)

        # Enable lookup of datetimes for IDs from the result batch.
        dts = {r[1]: r[0] for r in batch}

        documents: BatchResult[CRMObject] = await fetch_batch_with_associations(
            log, cls, http, object_name, [id for _, id in batch]
        )
        for doc in documents.results:
            id = str(doc.id)
            yield dts[id], id, doc


async def fetch_search_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    since: datetime,
    until: datetime | None,
    cursor: PageCursor,
    last_modified_property_name: str = "hs_lastmodifieddate"
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    
    url = f"{HUB}/crm/v3/objects/{object_name}/search"

    filter = {
        "propertyName": last_modified_property_name,
        "operator": "BETWEEN",
        "value": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "highValue": until.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    } if until else {
        "propertyName": last_modified_property_name,
        "operator": "GTE",
        "value": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }

    input = {
        "filters": [filter],
        # Sort newest to oldest since paging stops when an item as old or older than `since` is
        # encountered per the handling in `fetch_changes`.
        "sorts": [
            {
                "propertyName": last_modified_property_name,
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


def fetch_recent_custom_objects(
    object_name: str, log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, None, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, since,
    )


def fetch_delayed_custom_objects(
    object_name: str, log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, until, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, since,
    )

def fetch_recent_companies(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            log.warn("limit of 9,900 recent companies reached")
            return [], None

        url = f"{HUB}/companies/v2/companies/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentCompanies.model_validate_json(
            await http.request(log, url, params=params)
        )
        return (
            (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.companyId))
            for r in result.results
        ), result.hasMore and result.offset


    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, since,
    )


def fetch_delayed_companies(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.companies, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, since
    )


def fetch_recent_contacts(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        # There is no documented limit on the number of contacts that can be
        # returned by this API, other than that it goes back a maximum of 30 days.

        url = f"{HUB}/contacts/v1/lists/recently_updated/contacts/recent"
        params = {"count": 100, "timeOffset": page} if page else {"count": 1}

        result = OldRecentContacts.model_validate_json(
            await http.request(log, url, params=params)
        )
        return (
            (_ms_to_dt(int(r.properties.lastmodifieddate.value)), str(r.vid))
            for r in result.contacts
        ), result.has_more and result.time_offset


    return fetch_changes_with_associations(
        Names.contacts, Contact, do_fetch, log, http, since,
    )


def fetch_delayed_contacts(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.contacts, log, http, since, until, page, "lastmodifieddate")

    return fetch_changes_with_associations(
        Names.contacts, Contact, do_fetch, log, http, since
    )


def fetch_recent_deals(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            log.warn("limit of 9,900 recent deals reached")
            return [], None

        url = f"{HUB}/deals/v1/deal/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentDeals.model_validate_json(
            await http.request(log, url, params=params)
        )

        return (
            (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.dealId))
            for r in result.results
        ), result.hasMore and result.offset


    return fetch_changes_with_associations(
        Names.deals, Deal, do_fetch, log, http, since,
    )


def fetch_delayed_deals(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.deals, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.deals, Deal, do_fetch, log, http, since
    )


async def _fetch_engagements(
    log: Logger, http:  HTTPSession, page: PageCursor, count: int
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    if count >= 9_900:
        # TODO(whb): Consider implementing some kind of automatic re-backfill
        # functionality to better support this. "Engagements" as we are
        # capturing them has a 10k limit on how many items the API can return,
        # and there is no other API that can be used to get them within a
        # certain time window. The only option here is to re-backfill.
        raise Exception("Recent engagements limit of 9,900 items reached. This binding must be re-backfilled for the capture to continue.")

    url = f"{HUB}/engagements/v1/engagements/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentEngagements.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(r.engagement.lastUpdated), str(r.engagement.id))
        for r in result.results
    ), result.hasMore and result.offset


def fetch_recent_engagements(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements, log, http),
        log,
        http,
        since,
    )


def fetch_delayed_engagements(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    # There is no way to fetch engagements other than starting with the most
    # recent and reading backward, so this is the same process as fetching
    # "recent" engagements. It relies on the filtering in
    # `fetch_changes_with_associations` to ignore the more recent items.
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements, log, http),
        log,
        http,
        since,
        until,
    )


def fetch_recent_tickets(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        # This API will return a maximum of 1000 tickets, and does not appear to
        # ever return an error. It just ends at the 1000 most recently modified
        # tickets.
        url = f"{HUB}/crm-objects/v1/change-log/tickets"
        params = {"timestamp": int(since.timestamp() * 1000) - 1}

        result = TypeAdapter(list[OldRecentTicket]).validate_json(
            await http.request(log, url, params=params)
        )
        return ((_ms_to_dt(r.timestamp), str(r.objectId)) for r in result), None


    return fetch_changes_with_associations(
        Names.tickets, Ticket, do_fetch, log, http, since,
    )


def fetch_delayed_tickets(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.tickets, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.tickets, Ticket, do_fetch, log, http, since
    )


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


async def _fetch_email_events(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:
    url = f"{HUB}/email/public/v1/events"

    input: Dict[str, Any] = {
        "startTimestamp": _dt_to_ms(since),
        "limit": 1000,
    }
    if until:
        input["endTimestamp"] = _dt_to_ms(until)

    while True:
        result = EmailEventsResponse.model_validate_json(
            await http.request(log, url, params=input)
        )

        for event in result.events:
            yield event.created, event.id, event

        if not result.hasMore:
            break

        input["offset"] = result.offset


def fetch_recent_email_events(
    log: Logger, http: HTTPSession, since: datetime
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:

    return _fetch_email_events(log, http, since + timedelta(milliseconds=1), None)

def fetch_delayed_email_events(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:

    return _fetch_email_events(log, http, since, until)


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
