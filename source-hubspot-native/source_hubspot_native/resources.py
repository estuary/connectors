import functools
import itertools
from datetime import UTC, datetime, timedelta
from logging import Logger
import re
from typing import AsyncGenerator, Iterable

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding, ReductionStrategy, ResourceConfig
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    FetchDelayedFn,
    FetchRecentFn,
    check_campaigns_access,
    fetch_campaigns,
    fetch_campaigns_page,
    fetch_contact_list_memberships,
    fetch_contact_lists_page,
    check_contact_list_memberships_access,
    check_contact_lists_access,
    fetch_contact_list_memberships_page,
    fetch_contact_lists,
    fetch_deal_pipelines,
    fetch_delayed_companies,
    fetch_delayed_contacts,
    fetch_delayed_custom_objects,
    fetch_delayed_deals,
    fetch_delayed_email_events,
    fetch_delayed_engagements,
    fetch_delayed_feedback_submissions,
    fetch_delayed_goals,
    fetch_delayed_line_items,
    fetch_delayed_marketing_emails,
    fetch_delayed_orders,
    fetch_delayed_products,
    fetch_delayed_tickets,
    fetch_delayed_workflows,
    fetch_email_events_page,
    fetch_form_submissions,
    fetch_forms,
    fetch_marketing_emails_page,
    fetch_owners,
    fetch_page_with_associations,
    fetch_properties,
    fetch_recent_companies,
    fetch_recent_contacts,
    fetch_recent_custom_objects,
    fetch_recent_deals,
    fetch_recent_email_events,
    fetch_recent_engagements,
    fetch_recent_feedback_submissions,
    fetch_recent_goals,
    fetch_recent_line_items,
    fetch_recent_marketing_emails,
    fetch_recent_orders,
    fetch_recent_products,
    fetch_recent_tickets,
    fetch_recent_workflows,
    fetch_workflows_page,
    list_custom_objects,
    process_changes,
)
from .models import (
    OAUTH2_SPEC,
    Campaign,
    Company,
    Contact,
    CRMObject,
    ContactList,
    ContactListMembership,
    CustomObject,
    Deal,
    DealPipeline,
    EmailEvent,
    EndpointConfig,
    Engagement,
    FeedbackSubmission,
    Form,
    FormSubmission,
    Goals,
    LineItem,
    MarketingEmail,
    Names,
    Order,
    Owner,
    Product,
    Property,
    HubspotResourceConfigWithSchedule,
    ResourceState,
    Ticket,
    Workflow,
    ConnectorState,
)


MISSING_SCOPE_REGEX = (
    r"This app hasn't been granted all required scopes to make this call.|"
    r"auth request is missing required '.+' scope|"
    r"does not have proper permissions"
)


async def _can_access_endpoint(
    gen: AsyncGenerator,
) -> bool:
    try:
        async for _ in gen:
            break

        return True
    except HTTPError as err:
        is_missing_scope = err.code == 403 and bool(
            re.search(MISSING_SCOPE_REGEX, err.message)
        )

        if is_missing_scope:
            return False
        else:
            raise


async def _remove_permission_blocked_resources(
    log: Logger, http: HTTPMixin, resources: list[Resource]
) -> list[Resource]:
    # Attempt to access resources' endpoints. If a resource's endpoint is
    # inaccessible, remove that resource from the list of discovered resources.
    PERMISSION_BLOCKED_RESOURCES: list[tuple[Names, AsyncGenerator]] = [
        (
            Names.marketing_emails,
            fetch_recent_marketing_emails(log, http, False, datetime.now(tz=UTC), None),
        ),
        (
            Names.email_events,
            fetch_recent_email_events(log, http, False, datetime.now(tz=UTC), None),
        ),
        (Names.forms, fetch_forms(http, log)),
        (Names.form_submissions, fetch_form_submissions(http, log, 0)),
        (
            Names.feedback_submissions,
            fetch_recent_feedback_submissions(
                log, http, False, datetime.now(tz=UTC), None
            ),
        ),
        (
            Names.contact_lists,
            check_contact_lists_access(http, log),
        ),
        (
            Names.contact_list_memberships,
            check_contact_list_memberships_access(http, log),
        ),
        (
            Names.goals,
            fetch_recent_goals(
                log, http, False, datetime.now(tz=UTC), None,
            ),
        ),
        (
            Names.workflows,
            fetch_recent_workflows(
                log, http, False, datetime.now(tz=UTC), None,
            )
        ),
        (
            Names.orders,
            fetch_recent_orders(
                log,
                http,
                False,
                datetime.now(tz=UTC),
                None,
            ),
        ),
        (
            Names.campaigns,
            check_campaigns_access(http, log),
        ),
    ]

    for resource, gen in PERMISSION_BLOCKED_RESOURCES:
        if not await _can_access_endpoint(gen):
            resources = [r for r in resources if r.name != resource.name]

    return resources


def _resolve_custom_object_resource_name(
    name: str,
    useLegacyNaming: bool,
) -> str:
    return name if useLegacyNaming else f"custom_{name}"


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_check_permissions: bool = False,
) -> list[Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    standard_object_names: list[str] = [
        Names.companies,
        Names.contacts,
        Names.deals,
        Names.engagements,
        Names.tickets,
    ]

    custom_object_names = await list_custom_objects(log, http)
    # Some HubSpot endpoints like /v3/properties/{objectType} do not work for every custom object type.
    # However, these endpoints do work if we prepend a "p_" to the beginning of the custom object name
    # and use that in the path instead.
    # Docs reference: https://developers.hubspot.com/docs/api/crm/crm-custom-objects#retrieve-existing-custom-objects
    custom_object_path_components = [f"p_{n}" for n in custom_object_names]

    with_history = config.capturePropertyHistory

    custom_object_resources = [
        crm_object_with_associations(
            CustomObject,
            _resolve_custom_object_resource_name(n, config.useLegacyNamingForCustomObjects),
            custom_object_path_components[index],
            http,
            with_history,
            functools.partial(
                fetch_recent_custom_objects, custom_object_path_components[index]
            ),
            functools.partial(
                fetch_delayed_custom_objects, custom_object_path_components[index]
            ),
        )
        for index, n in enumerate(custom_object_names)
    ]

    standard_object_resources = [
        crm_object_with_associations(
            Company,
            Names.companies,
            Names.companies,
            http,
            with_history,
            fetch_recent_companies,
            fetch_delayed_companies,
        ),
        crm_object_with_associations(
            Contact,
            Names.contacts,
            Names.contacts,
            http,
            with_history,
            fetch_recent_contacts,
            fetch_delayed_contacts,
        ),
        crm_object_with_associations(
            Deal,
            Names.deals,
            Names.deals,
            http,
            with_history,
            fetch_recent_deals,
            fetch_delayed_deals,
        ),
        crm_object_with_associations(
            Engagement,
            Names.engagements,
            Names.engagements,
            http,
            with_history,
            fetch_recent_engagements,
            fetch_delayed_engagements,
        ),
        crm_object_with_associations(
            Ticket,
            Names.tickets,
            Names.tickets,
            http,
            with_history,
            fetch_recent_tickets,
            fetch_delayed_tickets,
        ),
        crm_object_with_associations(
            Product,
            Names.products,
            Names.products,
            http,
            with_history,
            fetch_recent_products,
            fetch_delayed_products,
        ),
        crm_object_with_associations(
            LineItem,
            Names.line_items,
            Names.line_items,
            http,
            with_history,
            fetch_recent_line_items,
            fetch_delayed_line_items,
        ),
        crm_object_with_associations(
            Goals,
            Names.goals,
            Names.goals,
            http,
            with_history,
            fetch_recent_goals,
            fetch_delayed_goals,
        ),
        properties(
            http, itertools.chain(standard_object_names, custom_object_path_components)
        ),
        deal_pipelines(http),
        owners(http),
        email_events(http),
        forms(http),
        form_submissions(http),
        marketing_emails(http),
        feedback_submissions(http, with_history),
        contact_lists(http),
        contact_list_memberships(http),
        workflows(http),
        campaigns(http),
        orders(http, with_history),
    ]

    if should_check_permissions:
        standard_object_resources = await _remove_permission_blocked_resources(log, http, standard_object_resources)


    # Resource names must be unique in the list of returned resources, but it's possible for a custom object
    # and a standard object to use the same resource name if the useLegacyNamingForCustomObjects flag is set.
    # If that happens, omit the standard object resource from the returned resource list.
    for custom_object_resource in custom_object_resources:
        standard_object_resources = [r for r in standard_object_resources if r.name != custom_object_resource.name]

    return [
        *custom_object_resources,
        *standard_object_resources,
    ]


def crm_object_with_associations(
    cls: type[CRMObject],
    object_name: str,
    path_component: str,
    http: HTTPSession,
    with_history: bool,
    fetch_recent: FetchRecentFn,
    fetch_delayed: FetchDelayedFn,
) -> Resource:

    async def open(
        binding: CaptureBinding[HubspotResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Emit a sourced schema to increase the inferred schema's complexity limit.
        properties = await fetch_properties(task.log, http, path_component)
        task.sourced_schema(binding_index, cls.sourced_schema(properties.results))
        await task.checkpoint(state=ConnectorState())

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                process_changes,
                path_component,
                fetch_recent,
                fetch_delayed,
                http,
                with_history,
            ),
            fetch_page=functools.partial(
                fetch_page_with_associations, cls, http, with_history, path_component
            ),
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
        # Default to performing a calculated properties refresh at 23:55 UTC every day for enabled CRM object bindings.
        initial_config=HubspotResourceConfigWithSchedule(name=object_name, schedule="55 23 * * *"),
        schema_inference=True,
        reduction_strategy=ReductionStrategy.MERGE,
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
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot,
            tombstone=Property(_meta=Property.Meta(op="d"), type=""),
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


def deal_pipelines(http: HTTPSession) -> Resource:

    async def snapshot(log: Logger) -> AsyncGenerator[DealPipeline, None]:
        items = await fetch_deal_pipelines(log, http)
        for item in items.results:
            yield item

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot,
            tombstone=DealPipeline(
                _meta=DealPipeline.Meta(op="d"), createdAt=None, updatedAt=None
            ),
        )

    return Resource(
        name=Names.deal_pipelines,
        key=["/_meta/row_id"],
        model=DealPipeline,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=Names.deal_pipelines, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def owners(http: HTTPSession) -> Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(fetch_owners, http),
            tombstone=Owner(_meta=Owner.Meta(op="d"), createdAt=None, updatedAt=None),
        )

    return Resource(
        name=Names.owners,
        key=["/_meta/row_id"],
        model=Owner,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=Names.owners, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def email_events(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                process_changes,
                Names.email_events,
                fetch_recent_email_events,
                fetch_delayed_email_events,
                http,
                True,  # email events do not include property history
            ),
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


def forms(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(fetch_forms, http),
            tombstone=Form(
                _meta=Form.Meta(op="d"), id="", createdAt=None, updatedAt=None
            ),
        )

    return Resource(
        name=Names.forms,
        key=["/_meta/row_id"],
        model=Form,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=Names.forms, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def form_submissions(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_form_submissions,
                http,
            ),
        )

    return Resource(
        name=Names.form_submissions,
        key=["/formId", "/submittedAt"],
        model=FormSubmission,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=0),
        ),
        initial_config=ResourceConfig(
            name=Names.form_submissions, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def marketing_emails(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                process_changes,
                Names.marketing_emails,
                fetch_recent_marketing_emails,
                fetch_delayed_marketing_emails,
                http,
                False,  # marketing emails do not include property history
            ),
            fetch_page=functools.partial(fetch_marketing_emails_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.marketing_emails,
        key=["/id"],
        model=MarketingEmail,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=Names.marketing_emails),
        schema_inference=True,
    )


def feedback_submissions(http: HTTPSession, with_history: bool) -> Resource:
    return crm_object_with_associations(
        FeedbackSubmission,
        Names.feedback_submissions,
        Names.feedback_submissions,
        http,
        with_history,
        fetch_recent_feedback_submissions,
        fetch_delayed_feedback_submissions,
    )


def contact_lists(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_contact_lists, http),
            fetch_page=functools.partial(fetch_contact_lists_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.contact_lists,
        key=["/listId"],
        model=ContactList,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(
            name=Names.contact_lists, interval=timedelta(hours=3)
        ),
        schema_inference=True,
    )


def contact_list_memberships(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_contact_list_memberships, http),
            fetch_page=functools.partial(fetch_contact_list_memberships_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.contact_list_memberships,
        key=["/listId", "/recordId"],
        model=ContactListMembership,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(
            name=Names.contact_list_memberships, interval=timedelta(hours=3)
        ),
        schema_inference=True,
    )


def workflows(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                process_changes,
                Names.workflows,
                fetch_recent_workflows,
                fetch_delayed_workflows,
                http,
                False,  # workflows do not include property history
            ),
            fetch_page=functools.partial(fetch_workflows_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.workflows,
        key=["/id"],
        model=Workflow,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=Names.workflows),
        schema_inference=True,
    )


def orders(http: HTTPSession, with_history: bool) -> Resource:
    return crm_object_with_associations(
        Order,
        Names.orders,
        Names.orders,
        http,
        with_history,
        fetch_recent_orders,
        fetch_delayed_orders,
    )


def campaigns(http: HTTPSession) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_campaigns, http),
            fetch_page=functools.partial(fetch_campaigns_page, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=Names.campaigns,
        key=["/id"],
        model=Campaign,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=Names.campaigns),
        schema_inference=True,
    )
