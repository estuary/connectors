import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Awaitable, Iterable

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import LogCursor, PageCursor, Resource, open_binding
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, HTTPSession, TokenSource

from .api import (
    FetchRecentFn,
    fetch_changes,
    fetch_email_events_changes,
    fetch_email_events_page,
    fetch_page,
    fetch_properties,
    fetch_recent_companies,
    fetch_recent_contacts,
    fetch_recent_deals,
    fetch_recent_engagements,
    fetch_recent_tickets,
)
from .models import (
    EMAIL_EVENT_HORIZON_DELTA,
    OAUTH2_SPEC,
    BaseCRMObject,
    Company,
    Contact,
    CRMObject,
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
    return [
        crm_object(Company, http, fetch_recent_companies),
        crm_object(Contact, http, fetch_recent_contacts),
        crm_object(Deal, http, fetch_recent_deals),
        crm_object(Engagement, http, fetch_recent_engagements),
        crm_object(Ticket, http, fetch_recent_tickets),
        properties(http),
        email_events(http),
    ]


def crm_object(
    cls: type[CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
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
            fetch_changes=functools.partial(fetch_changes, cls, fetch_recent, http),
            fetch_page=functools.partial(fetch_page, cls, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )


def properties(http: HTTPSession) -> Resource:

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
            fetch_changes=functools.partial(fetch_email_events_changes, http),
            fetch_page=functools.partial(fetch_email_events_page, http),
        )

    # started_at is set in the past per the horizon delta, otherwise we might miss events happening
    # simultaneously with the stream's initialization. It is set with millisecond precision, since
    # the API doesn't support anything beyond milliseconds.
    started_at = datetime.now(tz=UTC) - EMAIL_EVENT_HORIZON_DELTA
    started_at = started_at.replace(microsecond=started_at.microsecond // 1000 * 1000)

    return Resource(
        name=Names.email_events,
        key=["/id"],
        model=EmailEvent,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(
            name=Names.email_events, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )
