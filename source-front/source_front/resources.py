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
    FrontResource,
    Conversation,
    FULL_REFRESH_RESOURCES,
    INCREMENTAL_RESOURCES_WITH_CURSOR_FIELDS,
)
from .api import (
    snapshot_resources,
    fetch_resources_with_cursor_fields,
    fetch_conversations,
    API,
    CONVERSATIONS_LAG
)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    """
    Validates that the provided access token is a valid Front access token.
    """
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{API}/accounts"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid access token. Please confirm the provided access token is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating access token.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
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
            ),
            tombstone=FrontResource(_meta=FrontResource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FrontResource,
            open=functools.partial(open, name),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name) in FULL_REFRESH_RESOURCES
    ]

    return resources


def incremental_resources_with_cursor_fields(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        name: str,
        q_filter: str,
        sort_by: str,
        model : type[FrontResource],
        cursor_field: str,
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
                fetch_resources_with_cursor_fields,
                http,
                name,
                q_filter,
                sort_by,
                model,
                cursor_field,
            )
        )

    resources = [
        common.Resource(
            name=name,
            key=["/id"],
            model=model,
            open=functools.partial(open, name, q_filter, sort_by, model, cursor_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, q_filter, sort_by, model, cursor_field) in INCREMENTAL_RESOURCES_WITH_CURSOR_FIELDS
    ]

    return resources


def conversations(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
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
                fetch_conversations,
                http,
            )
        )

    # Avoid querying for data less than a few minutes old to *hopefully* avoid distributed clock issues.
    start = min(config.start_date, datetime.now(tz=UTC) - timedelta(minutes=CONVERSATIONS_LAG))

    conversation = [
        common.Resource(
            name="conversations",
            key=["/id"],
            model=Conversation,
            open=open,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=start),
            ),
            initial_config=ResourceConfig(
                name="conversations", interval=timedelta(minutes=5 + CONVERSATIONS_LAG)
            ),
            schema_inference=True,
        )
    ]

    return conversation


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        *full_refresh_resources(log, http, config),
        *incremental_resources_with_cursor_fields(log, http, config),
        *conversations(log, http, config),
    ]
