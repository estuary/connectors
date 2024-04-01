from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import json
import pytz
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator, Dict
import asyncio
import itertools
from copy import deepcopy

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)

from .models import (
    Association,
    BatchResult,
    CRMObject,
    WorkflowResult,
    V1CustomCRMObject,
    V1CustomCRMObject2,
    V1CRMObject,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    OldRecentEngagementsCalls,
    OldRecentEngagementsEmails,
    OldRecentEngagementsMeetings,
    OldRecentEngagementsNotes,
    OldRecentEngagementsTasks,
    OldRecentMarketingEmails,
    OldRecentMarketingForms,
    OldRecentOwners,
    OldRecentLineItems,
    OldRecentProducts,
    OldRecentWorkflows,
    OldRecentGoals,
    OldRecentFeedbackSubmissions,
    OldRecentEmailSubscriptions,
    PageResult,
    Properties,
)

HUB = "https://api.hubapi.com"


async def fetch_properties(
    log: Logger, cls: type[CRMObject], http: HTTPSession
) -> Properties:
    if p := getattr(cls, "CACHED_PROPERTIES", None):
        return p

    url = f"{HUB}/crm/v3/properties/{cls.PROPERTY_SEARCH_NAME}"
    cls.CACHED_PROPERTIES = Properties.model_validate_json(await http.request(log, url))
    for p in cls.CACHED_PROPERTIES.results:
        p.hubspotObject = cls.NAME

    return cls.CACHED_PROPERTIES

async def fetch_custom_objects(
    log: Logger, http: HTTPSession
) -> Dict:

    url = f"{HUB}/crm/v3/schemas"
    result = await http.request(log, url)

    return json.loads(result)


async def fetch_page(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    http: HTTPSession,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[CRMObject | str, None]:

    url = f"{HUB}/crm/v3/objects/{cls.PROPERTY_SEARCH_NAME}"

    if cls.IGNORE_PROPERTY_SEARCH is True:
        input = {
        "associations": ",".join(cls.ASSOCIATED_ENTITIES),
        "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
        }
        if len(cls.ASSOCIATED_ENTITIES) == 0:
            del input['associations']
    else: 
        properties = await fetch_properties(log, cls, http)
        property_names = ",".join(p.name for p in properties.results if not p.calculated)

        input = {
            "associations": ",".join(cls.ASSOCIATED_ENTITIES),
            "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
            "properties": property_names,
            "propertiesWithHistory": property_names,
        }
        if len(cls.ASSOCIATED_ENTITIES) == 0:
            del input['associations']
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


async def fetch_page_custom(
    # Closed over via functools.partial:
    cls: type[V1CRMObject],
    http: HTTPSession,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[CRMObject | str, None]:

    url = f"{HUB}{cls.ENFORCE_URL}"

    if cls.IGNORE_PROPERTY_SEARCH is True:
        input = {
        "associations": ",".join(cls.ASSOCIATED_ENTITIES),
        "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
        }
        if len(cls.ASSOCIATED_ENTITIES) == 0:
            del input['associations'] 
    else: 
        properties = await fetch_properties(log, cls, http)
        property_names = ",".join(p.name for p in properties.results if not p.calculated)

        input = {
            "associations": ",".join(cls.ASSOCIATED_ENTITIES),
            "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
            "properties": property_names,
            "propertiesWithHistory": property_names,
        }
    if page:
        input["after"] = page


    data = json.loads(await http.request(log, url, method="GET", params=input))
    if data.get('total') is not None:
        del data['total']
    _cls: Any = cls  # Silence mypy false-positive.
    result: PageResult[V1CRMObject] = PageResult[_cls].model_validate_json(
        json.dumps(data)
    )


    for doc in result.results:
        if doc.updatedAt < cutoff:
            yield doc

    if result.paging:
        yield result.paging.next.after

async def fetch_page_workflow(
    # Closed over via functools.partial:
    cls: type[V1CRMObject],
    http: HTTPSession,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[CRMObject | str, None]:

    url = f"{HUB}{cls.ENFORCE_URL}"

    if cls.IGNORE_PROPERTY_SEARCH is True:
        input = {
        "associations": ",".join(cls.ASSOCIATED_ENTITIES),
        "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
        }
        if len(cls.ASSOCIATED_ENTITIES) == 0:
            del input['associations'] 
    else: 
        properties = await fetch_properties(log, cls, http)
        property_names = ",".join(p.name for p in properties.results if not p.calculated)

        input = {
            "associations": ",".join(cls.ASSOCIATED_ENTITIES),
            "limit": 2,  # 50, # Maximum when requesting history. TODO(johnny).
            "properties": property_names,
            "propertiesWithHistory": property_names,
        }
    if page:
        input["after"] = page

    _cls: Any = cls  # Silence mypy false-positive.
    result: WorkflowResult[V1CRMObject] = WorkflowResult[_cls].model_validate_json(
        await http.request(log, url, method="GET", params=input)
    )

    for doc in result.workflows:
        if doc.updatedAt < cutoff:
            yield doc


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
    else:
        return


async def fetch_changes_no_batch(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    fetch_recent: FetchRecentFn,
    http: HTTPSession,
    # Remainder is common.FetchChangesFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CRMObject | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    recent: list[tuple[datetime, str]] = []
    next_page: PageCursor = None
    _cls = cls

    while True:
        iter, next_page = await fetch_recent(log, http, log_cursor, next_page)
        
        for ts, id, result in iter:
            recent.append((ts, id)) # Used later to update log_cursor value
            yield V1CRMObject.model_validate_json(json.dumps(result)) 


        if not next_page:
            break

    if recent:
        recent.sort() # Oldest updates first.
        yield recent[-1][0] # Updates log_cursor value
    else:
        return


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

    url = f"{HUB}/crm/v3/objects/tickets"
    
    result = OldRecentTicket.model_validate_json(
        await http.request(log, url, params=None)
    )

    return (
        (r.updatedAt, str(r.id))
        for r in result.results
    ), None and None

async def fetch_recent_contacts_lists(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/contacts/v1/lists"
    params = {"count": 100, "offset": page} if page else {"count": 250}

    data = await http.request(log, url, params=params)


    result = V1CustomCRMObject2.model_validate_json(
        data
    )

    for item in result.lists:
        item['listId'] = str(item['listId'])

    if result.hasMore is False:
        return (
            (_ms_to_dt(r['updatedAt']), str(r['listId']), r)
            for r in result.lists if _ms_to_dt(r['updatedAt']) > since
        ), None
    else: 
        return (
            (_ms_to_dt(r['updatedAt']), str(r['listId']), r)
            for r in result.lists if _ms_to_dt(r['updatedAt']) > since
        ), result.offset

#same as the ticket_pipeline
async def fetch_recent_deal_pipelines(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/crm-pipelines/v1/pipelines/deals"

    result = V1CRMObject.model_validate_json(
        await http.request(log, url, params=None)
    )

    # we want new deals
    # plus updatedAt can be null
    # and can compromise selection
    return (
        (_ms_to_dt(r['createdAt']), str(r["pipelineId"]), r)
        for r in result.results if _ms_to_dt(r['createdAt']) > since
    ), None

async def fetch_recent_ticket_pipelines(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/crm-pipelines/v1/pipelines/tickets"

    result = V1CRMObject.model_validate_json(
        await http.request(log, url, params=None)
    )

    # we want new tickets
    # plus updatedAt can be null
    # and can compromise selection
    return (
        (_ms_to_dt(r['createdAt']), str(r["pipelineId"]), r) 
        for r in result.results if _ms_to_dt(r['createdAt']) > since
    ), None

async def fetch_recent_email_events(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/email/public/v1/events"
    params = {"offset": page} if page else None

    result = V1CRMObject.model_validate_json(
        await http.request(log, url, params=params)
    )
    
    if result.hasMore is False:
        return (
            (_ms_to_dt(r["created"]), str(r["id"]), r) 
            for r in result.events if _ms_to_dt(r["created"]) > since), None

    else:
        return (
            (_ms_to_dt(r['created']), str(r['id']), r)
            for r in result.events if _ms_to_dt(r["created"]) > since), result.offset

async def fetch_recent_marketing_emails(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/marketing/v3/emails/"

    result = OldRecentMarketingEmails.model_validate_json(
        await http.request(log, url, params=None)
    )
    
    return (
        ((r.updatedAt), str(r.id))
        for r in result.results
    ), None and None # This endpoint does not have pagination.

async def fetch_recent_subscription_changes(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/email/public/v1/subscriptions/timeline"
    params = {"limit": 100, "offset": page} if page else {"limit": 1}

    data = await http.request(log, url, params=params)

    result = V1CRMObject.model_validate_json(
        data
    )

    for item in result.timeline:
        item["portalId"] = str(item["portalId"])

    if result.hasMore is False:
        return (
            (_ms_to_dt(r["timestamp"]), str(r["portalId"]), r)
            for r in result.timeline if _ms_to_dt(r["timestamp"]) > since
    ), None

    else:
        return (
            (_ms_to_dt(r["timestamp"]), str(r["portalId"]), r)
            for r in result.timeline if _ms_to_dt(r["timestamp"]) > since
        ), result.offset

async def fetch_email_subscriptions(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:

    url = f"{HUB}/communication-preferences/v3/definitions"
    

    result = OldRecentEmailSubscriptions.model_validate_json(
        await http.request(log, url, params=None)
    )

    return (
        ((r.updatedAt), str(r.id))
        for r in result.subscriptionDefinitions
    ), None and None # This endpoint does not have pagination.

async def fetch_contacts_lists_subscription(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/contacts/v1/lists/all/contacts/all"
    params = {"count": 100, "offset": page,"showListMemberships": "true"} if page else {"count": 1, "showListMemberships": "true"}

    data = await http.request(log, url, params=params)

    result = V1CustomCRMObject.model_validate_json(
        data
    )

    for item in result.contacts:
        item["vid"] = str(item["vid"])


    if result.hasMore is False:
        return (
        (_ms_to_dt(int(r["properties"]["lastmodifieddate"]["value"])), str(r["vid"]), r)
        for r in result.contacts if _ms_to_dt(int(r["properties"]["lastmodifieddate"]["value"])) > since
    ), None
    
    else:
        return (
            (_ms_to_dt(int(r["properties"]["lastmodifieddate"]["value"])), str(r["vid"]), r)
            for r in result.contacts if _ms_to_dt(int(r["properties"]["lastmodifieddate"]["value"])) > since
        ), result.offset

async def fetch_campaigns(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
    ) -> tuple[Iterable[tuple[datetime, str, Any]], PageCursor]:

    url = f"{HUB}/email/public/v1/campaigns"
    params = {"offset": page} if page else None

    data = await http.request(log, url, params=params)

    result = V1CRMObject.model_validate_json(
        data
    )

    if result.hasMore is False:
        return (
        (_ms_to_dt(int(r["lastUpdatedTime"])), str(r["id"]), r)
        for r in result.campaigns if _ms_to_dt(int(r["lastUpdatedTime"])) > since
    ), None
    
    else:
        return (
            (_ms_to_dt(int(r["lastUpdatedTime"])), str(r["id"]), r)
            for r in result.campaigns if _ms_to_dt(int(r["lastUpdatedTime"])) > since
        ), result.offset

def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


## V3

async def fetch_marketing_forms(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    
    url = f"{HUB}/marketing/v3/forms"

    result = OldRecentMarketingForms.model_validate_json(
         await http.request(log, url, params=None)
    )
    return (
        (r.updatedAt, str(r.id))
        for r in result.results
    ), None and None

async def fetch_owners(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    
    url = f"{HUB}/crm/v3/owners"

    result = OldRecentOwners.model_validate_json(
         await http.request(log, url, params=None)
    )
    return (
        (r.updatedAt, str(r.id))
        for r in result.results
    ), None and None

async def fetch_workflows(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    
    url = f"{HUB}/automation/v3/workflows"

    result = OldRecentWorkflows.model_validate_json(
         await http.request(log, url, params=None)
    )
    return (
        (r.updatedAt, str(r.id))
        for r in result.workflows
    ), None and None
