import functools
import itertools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Iterable

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import LogCursor, PageCursor, Resource, open_binding
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, HTTPSession, TokenSource

from .api import (
    FetchRecentFn,
    fetch_changes,
    fetch_email_events_page,
    fetch_page,
    fetch_properties,
    fetch_recent_companies,
    fetch_recent_contacts,
    fetch_recent_custom_objects,
    fetch_recent_deals,
    fetch_recent_email_events,
    fetch_recent_engagements,
    fetch_recent_tickets,
    list_custom_objects,
)
from .models import (
    OAUTH2_SPEC,
    Company,
    Contact,
    CRMObject,
    CustomObject,
    Deal,
    EmailEvent,
    EndpointConfig,
    Engagement,
    Names,
    Property,
    ResourceConfig,
    ResourceState,
    Ticket,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    standard_object_names: list[str] = [
        Names.companies,
        Names.contacts,
        Names.deals,
        Names.engagements,
        Names.tickets,
    ]

    custom_object_names = await list_custom_objects(log, http)

    custom_object_resources = [
        crm_object(
            CustomObject, n, http, functools.partial(fetch_recent_custom_objects, n)
        )
        for n in custom_object_names
    ]

    return [
        crm_object(Company, Names.companies, http, fetch_recent_companies),
        crm_object(Contact, Names.contacts, http, fetch_recent_contacts),
        crm_object(Deal, Names.deals, http, fetch_recent_deals),
        crm_object(Engagement, Names.engagements, http, fetch_recent_engagements),
        crm_object(Ticket, Names.tickets, http, fetch_recent_tickets),
        properties(http, itertools.chain(standard_object_names, custom_object_names)),
        email_events(http),
        *custom_object_resources,
    ]


def crm_object(
    cls: type[CRMObject], object_name, http: HTTPSession, fetch_recent: FetchRecentFn
) -> Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes, cls, fetch_recent, http, object_name),
            fetch_page=functools.partial(fetch_page, cls, http, object_name),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=object_name,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=object_name),
        schema_inference=True,
    )


def properties(http: HTTPSession, object_names: Iterable[str]) -> Resource:

    async def snapshot(log: Logger) -> AsyncGenerator[Property, None]:
        for obj in object_names:
            properties = await fetch_properties(log, http, obj)
            for prop in properties.results:
                yield prop

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot,
            tombstone=Property(_meta=Property.Meta(op="d")),
        )

    return Resource(
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

def email_events(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_recent_email_events, http),
            fetch_page=functools.partial(fetch_email_events_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.email_events,
        key=["/id"],
        model=EmailEvent,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=Names.email_events),
        schema_inference=True,
    )
