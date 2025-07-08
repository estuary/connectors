from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    IntercomResource,
    TimestampedResource,
    ClientSideFilteringResourceFetchChangesFn,
    IncrementalResourceFetchChangesFn,
    CompanyResourceFetchChangesFn,
    IncrementalDateWindowResourceFetchChangesFn,
    OAUTH2_SPEC,
)
from .api import (
    snapshot_resources,
    fetch_contacts,
    fetch_tickets,
    fetch_conversations,
    fetch_conversations_parts,
    fetch_segments,
    fetch_companies,
    fetch_company_segments,
    API,
)


# Full refresh resources.
# Each tuple contains their name, their path, the API response field that contains records, and a model query param (data attributes only).
FULL_REFRESH_RESOURCES: list[tuple[str, str, str, str | None]] = [
    ("admins", "admins", "admins", None),
    ("tags", "tags", "data", None),
    ("teams", "teams", "teams", None),
    ("company_attributes", "data_attributes", "data", "company"),
    ("contact_attributes", "data_attributes", "data", "contact"),
]


# Incremental resources that use date windows.
# Each tuple contains the resource's name and its fetch function.
INCREMENTAL_DATE_WINDOW_RESOURCES: list[tuple[str, IncrementalDateWindowResourceFetchChangesFn]] = [
    ('contacts', fetch_contacts),
]

# Incremental resources that don't use date windows.
# Each tuple contains the resource's name and its fetch function.
INCREMENTAL_RESOURCES: list[tuple[str, IncrementalResourceFetchChangesFn]] = [
    ('conversation_parts', fetch_conversations_parts),
    ('tickets', fetch_tickets),
    ('conversations', fetch_conversations),
]

# Resources that have a timestamp field we can use to perform client side filtering.
# Each tuple contains the resource's name and its fetch function.
CLIENT_SIDE_FILTERED_RESOURCES: list[tuple[str, ClientSideFilteringResourceFetchChangesFn]] = [
    ("segments", fetch_segments),
]

# Company-related resources. These are also filtered on the client side, but require
# an additional config setting to determine which endpoint to use.
COMPANY_RESOURCES: list[tuple[str, CompanyResourceFetchChangesFn]] = [
    ("companies", fetch_companies),
    ("company_segments", fetch_company_segments),
]


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    url = f"{API}/data_attributes"
    params = {"model": "contact"}

    try:
        await http.request(log, url, params=params)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating access token.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            response_field: str,
            query_param: str | None,
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                path,
                response_field,
                query_param,
            ),
            tombstone=IntercomResource(_meta=IntercomResource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=IntercomResource,
            open=functools.partial(open, path, response_field, query_param),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_field, query_param) in FULL_REFRESH_RESOURCES
    ]

    return resources


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        fetch_fn: IncrementalResourceFetchChangesFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_fn,
                http,
                config.advanced.search_page_size,
            )
        )

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, fetch_fn),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, fetch_fn) in INCREMENTAL_RESOURCES
    ]

    return resources


def incremental_date_window_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        fetch_fn: IncrementalDateWindowResourceFetchChangesFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_fn,
                http,
                config.advanced.window_size,
                config.advanced.search_page_size,
            )
        )

    resources = [
        common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, fetch_fn),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, fetch_fn) in INCREMENTAL_DATE_WINDOW_RESOURCES
    ]

    return resources


def client_side_filtered_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        fetch_fn: ClientSideFilteringResourceFetchChangesFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_fn,
                http,
            )
        )

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, fetch_fn),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, fetch_fn) in CLIENT_SIDE_FILTERED_RESOURCES
    ]

    return resources


def company_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        fetch_fn: CompanyResourceFetchChangesFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_fn,
                http,
                config.advanced.use_companies_list_endpoint,
            )
        )

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, fetch_fn),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, fetch_fn) in COMPANY_RESOURCES
    ]

    return resources



async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    return [
        *full_refresh_resources(log, http, config),
        *incremental_date_window_resources(log, http, config),
        *incremental_resources(log, http, config),
        *client_side_filtered_resources(log, http, config),
        *company_resources(log, http, config),
    ]
