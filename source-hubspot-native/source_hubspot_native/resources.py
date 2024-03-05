from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding
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
    ]


def crm_object(
    cls: type[CRMObject], http: HTTPSession, fetch_recent: FetchRecentFn
) -> Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
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
