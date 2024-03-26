from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
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
)
from .api import (
    FetchRecentFn,
    fetch_page,
    fetch_properties,
    fetch_changes,
    fetch_recent_companies,
    fetch_recent_contacts,
    fetch_recent_deals,
    fetch_recent_engagements,
    fetch_recent_tickets,
    fetch_recent_contacts_lists,
    fetch_engagements_calls,
    fetch_engagements_emails,
    fetch_engagements_notes,
    fetch_engagements_tasks,
    fetch_engagements_meetings,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    return [
        crm_object_no_page(Company, http, fetch_recent_companies),
        crm_object_no_page(Contact, http, fetch_recent_contacts),
        crm_object_no_page(Deal, http, fetch_recent_deals),
        crm_object_no_page(Engagement, http, fetch_recent_engagements),
        crm_object_no_page(Ticket, http, fetch_recent_tickets),
        crm_object_no_changes(EngagementCalls, http, fetch_engagements_calls),
        crm_object_no_changes(EngagementEmails, http, fetch_engagements_emails),
        crm_object_no_changes(EngagementMeetings, http, fetch_engagements_meetings),
        crm_object_no_changes(EngagementNotes, http, fetch_engagements_notes),
        crm_object_no_changes(EngagementTasks, http, fetch_engagements_tasks),
        crm_object_no_page(ContactLists, http, fetch_recent_contacts_lists),
        properties(http),
    ]


def crm_object_no_changes(
    cls: type[CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
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
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )

def crm_object_no_page(
    cls: type[CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
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
            fetch_changes=functools.partial(fetch_changes, cls, fetch_recent, http),
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
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
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
            name=Names.properties, interval=timedelta(days=1)
        ),
        schema_inference=True,
    )
