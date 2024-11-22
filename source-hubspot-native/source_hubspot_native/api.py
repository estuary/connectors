import asyncio
import functools
import itertools
from datetime import UTC, datetime, timedelta
import json
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
    Triggers,
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
    DealPipelines,
    EmailEvent,
    EmailEventsResponse,
    Engagement,
    Names,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    Owner,
    PageResult,
    Product,
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


async def fetch_deal_pipelines(
    log: Logger, http: HTTPSession
) -> DealPipelines:
    url = f"{HUB}/crm-pipelines/v1/pipelines/deals"

    return DealPipelines.model_validate_json(await http.request(log, url))


async def fetch_owners(
    log: Logger, http: HTTPSession
) -> list[Owner]:
    url = f"{HUB}/owners/v2/owners/"

    return TypeAdapter(list[Owner]).validate_json(await http.request(log, url))


async def fetch_page_with_associations(
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
            "limit": 50, # Maximum when requesting history.
            "properties": property_names,
            "propertiesWithHistory": property_names,
        }
        if len(cls.ASSOCIATED_ENTITIES) > 0:
            input["associations"] = ",".join(cls.ASSOCIATED_ENTITIES)
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
    [Logger, HTTPSession, datetime, datetime | None],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
'''
Returns a stream of (timestamp, key, document) tuples that represent a
potentially incomplete stream of very recent documents. The timestamp is used to
checkpoint the next log cursor. The key is used for updating the emitted changes
cache with the timestamp of the document.

Documents may be returned in any order, but iteration will be stopped upon
seeing an entry that's as-old or older than the datetime cursor.

The first datetime parameter represents the "since" value, which is the oldest
documents that are required. The second datetime parameter is an "until" value,
which if not None is a hint that more recent documents are not needed.
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

# How far back the delayed stream will search up until. This needs to be far
# back enough that the HubSpot APIs return consistent data.
delayed_offset = timedelta(hours=1)

# The minimum amount of time must be available for the delay stream to be
# called. It can't be called every time `process_changes` is invoked since
# FetchDelayedFn is usually using a slower API than FetchRecentFn, and calling
# FetchDelayedFn every time would negate the benefit of having a faster
# FetchRecentFn.
delayed_fetch_minimum_window = timedelta(minutes=5)

# Limit the maximum time window we will ask FetchRecentFn to get changes for.
# This is to prevent the time window from becoming excessively large if there
# are relatively brief outages in the connector (on the order of a day or two),
# which otherwise would require a checkpoint containing all of the documents
# from the last log_cursor up until the present time. This allows for a form of
# incremental progress to be made, which is helpful if there are ~millions of
# documents to catch up on. The HubSpot APIs are pretty flaky and will
# occasionally return non-retryable errors for no reason and a successful
# checkpoint requires that to never happen, so really large checkpoints can
# become effectively impossible to complete.
#
# Generally moving the cursor forward in these fixed windows is a bad idea when
# trying to cover time windows on the scales of ~years (as in for a backfill),
# but incremental bindings shouldn't fall more than ~days behind in the worst of
# cases so requesting an hour at a time won't take too long.
max_fetch_recent_window = timedelta(hours=1)


class MustBackfillBinding(Exception):
    pass


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

    now_time = datetime.now(UTC)
    fetch_recent_end_time: datetime | None = None
    if now_time - log_cursor > max_fetch_recent_window:
        # Limit the maximum requested window for FetchRecentFn if it's been a
        # while since the LogCursor was updated.
        fetch_recent_end_time = log_cursor + max_fetch_recent_window

    max_ts: datetime = log_cursor
    try: 
        async for ts, key, obj in fetch_recent(log, http, log_cursor, fetch_recent_end_time):
            if fetch_recent_end_time and ts > fetch_recent_end_time:
                continue
            elif ts > log_cursor:
                max_ts = max(max_ts, ts)
                cache.add_recent(object_name, key, ts)
                yield obj
            else:
                break
    except MustBackfillBinding:
        log.info("triggering automatic backfill for %s", object_name)
        yield Triggers.BACKFILL

    if fetch_recent_end_time:
        # Assume all recent documents up until fetch_recent_end_time were
        # returned by FetchRecentFn. It's fine if this is not strictly true
        # since FetchDelayedFn will eventually fill in any missed documents.
        # This also ensures that the cursor is always kept moving forward even
        # if there are no new recent documents for a long time, which is
        # important since it moves the delayed stream forward in that case.
        max_ts = fetch_recent_end_time

    delayed_fetch_next_start = last_delayed_fetch_end.setdefault(
        object_name, log_cursor - delayed_offset - delayed_fetch_minimum_window
    )
    delayed_fetch_next_end = max_ts - delayed_offset

    cache_hits = 0
    delayed_emitted = 0
    if delayed_fetch_next_end - delayed_fetch_next_start > delayed_fetch_minimum_window:
        # Poll the delayed stream for documents if we need to.
        try:
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
        except MustBackfillBinding:
            log.info("triggering automatic backfill for %s", object_name)
            yield Triggers.BACKFILL

        last_delayed_fetch_end[object_name] = delayed_fetch_next_end
        evicted = cache.cleanup(object_name, delayed_fetch_next_end)

        log.info(
            "fetched delayed events for stream",
            {"object_name": object_name, "since": delayed_fetch_next_start, "until": delayed_fetch_next_end, "emitted": delayed_emitted, "cache_hits": cache_hits, "evicted": evicted, "new_size": cache.count_for(object_name)}
        )

    if max_ts != log_cursor:
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
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, CRMObject], None]:
    
    # Walk pages of recent IDs until we see one which is as-old
    # as `since`, or no pages remain.
    recent: list[tuple[datetime, str]] = []
    next_page: PageCursor = None
    count = 0

    while True:
        iter, next_page = await fetcher(next_page, count)

        for ts, id in iter:
            count += 1
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
        dts = {id: dt for dt, id in batch}

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
    _: PageCursor,
    last_modified_property_name: str = "hs_lastmodifieddate"
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    '''
    Retrieve all records between 'since' and 'until' (or the present time).

    The HubSpot Search API has an undocumented maximum offset of 10,000 items,
    so the logic here will completely enumerate all available records, kicking
    off new searches if needed to work around that limit. The returned
    PageCursor is always None.

    Multiple records can have the same "last modified" property value, and
    indeed large runs of these may have the same value. So a "new" search will
    inevitably grab some records again, and deduplication is handled here via
    the set.
    '''
    
    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    limit = 200
    output_items: set[tuple[datetime, str]] = set()
    cursor: int | None = None
    max_updated: datetime = since
    original_since = since
    original_total: int | None = None

    while True:
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
            "sorts": [
                {
                    "propertyName": last_modified_property_name,
                    "direction": "ASCENDING"
                }
            ],
            "limit": limit,
        }
        if cursor:
            input["after"] = cursor

        result: SearchPageResult[CustomObjectSearchResult] = SearchPageResult[CustomObjectSearchResult].model_validate_json(
            await http.request(log, url, method="POST", json=input)
        )

        if not original_total:
            # Record the total from the original entire request range for
            # logging.
            original_total = result.total

        for r in result.results:
            this_mod_time = r.properties.hs_lastmodifieddate
            if this_mod_time < max_updated:
                # This should never happen since results are requested in
                # ASCENDING order by the last modified time.
                raise Exception(f"last modified date {this_mod_time} is before {max_updated} for {r.id}")
            
            max_updated = this_mod_time
            output_items.add((this_mod_time, str(r.id)))

        if not result.paging:
            if since is not original_since:
                log.info(
                    "finished multi-request fetch",
                    {
                        "final_count": len(output_items),
                        "total": original_total,
                    }
                )
            break

        cursor = int(result.paging.next.after)
        if cursor + limit > 10_000:
            # Further attempts to read this search result will not accepted by
            # the search API, so a new search must be initiated to complete the
            # request.
            cursor = None

            if since == max_updated:
                log.info(
                    "cycle detected for lastmodifieddate, fetching all ids for records modified at that instant",
                    {"object_name": object_name, "instant": since},
                )
                output_items.update(await fetch_search_objects_modified_at(
                    object_name, log, http, max_updated, last_modified_property_name
                ))

                # HubSpot APIs use millisecond resolution, so move the time
                # cursor forward by that minimum amount now that we know we have
                # all of the records modified at the common `max_updated` time.
                max_updated = max_updated + timedelta(milliseconds=1)

                if until and max_updated > until:
                    # Unlikely edge case, but this would otherwise result in an
                    # error from the API.
                    break

            since = max_updated
            log.info(
                "initiating a new search to satisfy requested time range",
                {
                    "object_name": object_name,
                    "since": since,
                    "until": until,
                    "original_since": original_since,
                    "count": len(output_items),
                    "total": original_total,
                },
            )

    # Sort newest to oldest to match the convention of most of "fetch ID"
    # functions.
    return sorted(list(output_items), reverse=True), None


async def fetch_search_objects_modified_at(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    modified: datetime,
    last_modified_property_name: str = "hs_lastmodifieddate"
) -> set[tuple[datetime, str]]:
    '''
    Fetch all of the ids of the given object that were modified at the given
    time. Used exclusively for breaking out of cycles in the search API
    resulting from more than 10,000 records being modified at the same time,
    which is unfortunately a thing that can happen.

    To simplify the pagination strategy, the actual `paging` result isn't used
    at all other than to see when we have reached the end, and the search query
    just always asks for ids larger than what it had previously seen.
    '''

    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    limit = 200
    output_items: set[tuple[datetime, str]] = set()
    id_cursor: int | None = None
    round = 0

    while True:
        filters: list[dict[str, Any]] = [{
            "propertyName": last_modified_property_name,
            "operator": "EQ",
            "value": modified.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }]

        if id_cursor:
            filters.append({
                "propertyName": "hs_object_id",
                "operator": "GT",
                "value": id_cursor,
            })

        input = {
            "filters": filters,
            "sorts": [
                {
                    "propertyName": "hs_object_id",
                    "direction": "ASCENDING"
                }
            ],
            "limit": limit,
        }

        result: SearchPageResult[CustomObjectSearchResult] = SearchPageResult[CustomObjectSearchResult].model_validate_json(
            await http.request(log, url, method="POST", json=input)
        )

        for r in result.results:
            if id_cursor and r.id <= id_cursor:
                # This should _really_ never happen, but HubSpot is weird so if
                # it does I want to know about it and will come back later and
                # figure it out.
                raise Exception(f"unexpected id order: {r.id} <= {id_cursor}")
            id_cursor = r.id
            output_items.add((r.properties.hs_lastmodifieddate, str(r.id)))

        # Log every 10,000 returned records, since there are 200 per page.
        if round % 50 == 0:
            log.info(
                "fetching ids for records modified at instant",
                {
                    "object_name": object_name,
                    "instant": modified,
                    "count": len(output_items),
                    "remaining": result.total,
                }
            )

        if not result.paging:
            break

        round += 1

    return output_items


def fetch_recent_custom_objects(
    object_name: str, log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, until, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, since, until
    )


def fetch_delayed_custom_objects(
    object_name: str, log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, until, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, since, until
    )

def fetch_recent_companies(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None,
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
        Names.companies, Company, do_fetch, log, http, since, until
    )


def fetch_delayed_companies(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.companies, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, since, until
    )


def fetch_recent_contacts(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:
    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            # There is actually no documented limit on the number of contacts
            # that can be returned by this API, other than that it goes back a
            # maximum of 30 days. But since there is no way to filter the
            # response by `until`, we impose the same limit on the number of
            # recent IDs that will be fetched here as other ID fetchers to
            # prevent cases of trying to cycle through huge numbers of results
            # if the LogCursor hasn't been updated in a long time.
            log.warn("limit of 9,900 recent contacts reached")
            return [], None

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
        Names.contacts, Contact, do_fetch, log, http, since, until
    )


def fetch_delayed_contacts(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.contacts, log, http, since, until, page, "lastmodifieddate")

    return fetch_changes_with_associations(
        Names.contacts, Contact, do_fetch, log, http, since, until
    )


def fetch_recent_deals(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
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
        Names.deals, Deal, do_fetch, log, http, since, until
    )


def fetch_delayed_deals(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.deals, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.deals, Deal, do_fetch, log, http, since, until
    )


async def _fetch_engagements(
    log: Logger, http:  HTTPSession, page: PageCursor, count: int
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    if count >= 9_900:
        # "Engagements" as we are capturing them has a 10k limit on how many
        # items the API can return, and there is no other API that can be used
        # to get them within a certain time window. The only option here is to
        # re-backfill.
        log.warn("limit of 9,900 recent engagements reached")
        raise MustBackfillBinding

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
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements, log, http),
        log,
        http,
        since,
        until,
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
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
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
        Names.tickets, Ticket, do_fetch, log, http, since, until
    )


def fetch_delayed_tickets(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.tickets, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.tickets, Ticket, do_fetch, log, http, since, until
    )


def fetch_recent_products(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, Product], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.products, Product, do_fetch, log, http, since, until
    )


def fetch_delayed_products(
    log: Logger, http: HTTPSession, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Product], None]:

    async def do_fetch(page: PageCursor, count: int) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.contacts, Product, do_fetch, log, http, since, until
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
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:

    return _fetch_email_events(log, http, since + timedelta(milliseconds=1), until)

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
