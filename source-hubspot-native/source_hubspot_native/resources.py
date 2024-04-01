from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable, Dict
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
#from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding
from estuary_cdk.capture import common
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .models import (
    BaseCRMObject,
    CRMObject,
    V1CRMObject,
    CustomObject,
    Company,
    Contact,
    Deal,
    EndpointConfig,
    Engagement,
    Names,
    OAUTH2_SPEC,
    Property,
    ResourceConfig,
    ResourceState,
    Ticket,
    ContactLists,
    EngagementCalls,
    EngagementEmails,
    EngagementMeetings,
    EngagementNotes,
    EngagementTasks,
    TicketPipelines,
    DealPipelines,
    EmailEvents,
    SubscriptionChanges,
    MarketingEmails,
    MarketingForms,
    Owners,
    LineItems,
    Campaigns,
    Products,
    Workflows,
    Goals,
    FeedbackSubmissions,
    EmailSubscriptions,
    ContactSubscription,
)
from .api import (
    FetchRecentFn,
    fetch_page,
    fetch_page_custom,
    fetch_page_workflow,
    fetch_properties,
    fetch_changes,
    fetch_changes_no_batch,
    fetch_recent_companies,
    fetch_recent_contacts,
    fetch_recent_deals,
    fetch_recent_engagements,
    fetch_recent_contacts_lists,
    fetch_recent_ticket_pipelines,
    fetch_recent_deal_pipelines,
    fetch_recent_email_events,
    fetch_recent_subscription_changes,
    fetch_recent_marketing_emails,
    fetch_campaigns,
    fetch_marketing_forms,
    fetch_owners,
    fetch_workflows,
    fetch_email_subscriptions,
    fetch_contacts_lists_subscription,
    fetch_custom_objects,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    resources_list = [
        crm_object_streamed(Company, http, fetch_recent_companies),
        crm_object_streamed(Contact, http, fetch_recent_contacts),
        crm_object_streamed(Deal, http, fetch_recent_deals),
        crm_object_streamed(Engagement, http, fetch_recent_engagements),
        crm_object_streamed_no_batch(ContactLists, http, fetch_recent_contacts_lists),
        crm_object_streamed_no_batch(ContactSubscription, http, fetch_contacts_lists_subscription),
        crm_object_streamed_no_batch(Campaigns, http, fetch_campaigns),
        crm_object_streamed_no_batch(SubscriptionChanges, http, fetch_recent_subscription_changes),
        crm_object_streamed_no_batch(EmailEvents, http, fetch_recent_email_events),
        crm_object_streamed_no_batch(TicketPipelines, http, fetch_recent_ticket_pipelines),
        crm_object_streamed_no_batch(DealPipelines, http, fetch_recent_deal_pipelines),
        crm_object_paginated(EngagementCalls, http),
        crm_object_paginated(EngagementEmails, http),
        crm_object_paginated(EngagementMeetings, http),
        crm_object_paginated(EngagementNotes, http),
        crm_object_paginated(EngagementTasks, http),
        crm_object_paginated(Goals, http),
        crm_object_paginated(FeedbackSubmissions, http),
        crm_object_paginated(LineItems, http),
        crm_object_paginated(Products, http),
        crm_object_paginated(Ticket, http),
        crm_object_custom(MarketingEmails, http, fetch_recent_marketing_emails),
        crm_object_custom(EmailSubscriptions, http, fetch_email_subscriptions),
        crm_object_custom(MarketingForms, http, fetch_marketing_forms),
        crm_object_custom(Owners, http, fetch_owners),
        workflow_object(Workflows, http, fetch_workflows),
        properties(http),
    ]

    all_custom_objects = await fetch_custom_objects(log, http)
    if len(all_custom_objects["results"]) != 0:
        for objects in all_custom_objects["results"]:
            BaseCRMObject.NAME = objects["labels"]["plural"].lower()
            BaseCRMObject.PRIMARY_KEY = ["/id"]
            BaseCRMObject.PROPERTY_SEARCH_NAME = objects["labels"]["plural"].lower()
            BaseCRMObject.ASSOCIATED_ENTITIES = []

            resources_list.append(custom_objects(BaseCRMObject, http))

    return resources_list


def crm_object_paginated(
    cls: type[CRMObject], http: HTTPSession
) -> common.Resource:
    """Base Resource to run V3 API objects using pagination

    Args:
        cls (type[CRMObject]): _description_
        http (HTTPSession): _description_
        fetch_recent (FetchRecentFn): _description_

    Returns:
        common.Resource: _description_
    """
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page, cls, http),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=True,
    )

def crm_object_custom(
    cls: type[V1CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
) -> common.Resource:
    """Custom Resource to run V3 objects using pagination
    This endpoint allows for different URL objects from the V3 API
    and handle some data formatting. It works very similar to 
    'crm_object_paginated'

    Args:
        cls (type[V1CRMObject]): _description_
        http (HTTPSession): _description_
        fetch_recent (FetchRecentFn): _description_

    Returns:
        common.Resource: _description_
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page_custom, cls, http),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=True,
    )


def crm_object_streamed(
    cls: type[CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
) -> common.Resource:
    """Base Resource to run V1 API objects using stream
    This resource uses an batch endpoint from Hubspot. It works
    by getting the Ids and last_updated data from fetch_recent 
    and passing it to the batch endpoint later on.

    Args:
        cls (type[CRMObject]): _description_
        http (HTTPSession): _description_
        fetch_recent (FetchRecentFn): _description_

    Returns:
        common.Resource: _description_
    """
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes, cls, fetch_recent, http),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=True,
    )



def crm_object_streamed_no_batch(
    cls: type[V1CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
) -> common.Resource:
    """Custom Resource to run V1 API objects using stream
    This resource does not use the batch function from "crm_object_streamed"
    Allowing for a more broad usage.

    Args:
        cls (type[V1CRMObject]): _description_
        http (HTTPSession): _description_
        fetch_recent (FetchRecentFn): _description_

    Returns:
        common.Resource: _description_
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes_no_batch, cls, fetch_recent, http),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=True,
    )

def workflow_object(
    cls: type[V1CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
) -> common.Resource:

    """Custom Resource to run specifically workflow stream objects
    Workflow objects do not have pagination neither a usual schema, 
    so this resource was created to handle this specific case.
    """
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page_workflow, cls, http),
        )

    started_at = datetime.now(tz=UTC)

    return common.Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=True,
    )




def properties(http: HTTPSession) -> common.Resource:

    async def snapshot(log: Logger) -> AsyncGenerator[Property, None]:
        classes: list[type[BaseCRMObject]] = [
            Company,
            Contact,
            Engagement,
            Deal,
            Ticket,
        ]
        for cls in classes:
            properties = await fetch_properties(log, cls, http)
            for prop in properties.results:
                yield prop

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot,
            tombstone=Property(_meta=Property.Meta(op="d")),
        )

    return common.Resource(
        name=Names.properties,
        key=["/_meta/row_id"],
        model=Property,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=Names.properties, interval=timedelta(minutes=7)
        ),
        schema_inference=True,
    )

def custom_objects(
    cls: type[CRMObject], http: HTTPSession
) -> common.Resource:
    
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page, cls, http),
        )
    

    started_at = datetime.now(tz=UTC)
    return common.Resource(
        name=cls.NAME,
        key=cls.PRIMARY_KEY,
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=7)),
        schema_inference=False,
    )
