from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from estuary_cdk.capture.common import ResourceConfig

from source_monday.models import (
    EndpointConfig,
    OAUTH2_SPEC,
    ResourceState,
    GraphQLDocument,
)
from source_monday.api import (
    snapshot_boards,
    snapshot_teams,
    snapshot_users,
    snapshot_tags,
    snapshot_items,
)
from source_monday.graphql import API


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """
    Checks if the provided client credentials belong to a valid OAuth app.
    """
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        await http.request(log, API, method="POST", json={"query": "query {me {id}}"})
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def boards(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_boards, http),
            tombstone=GraphQLDocument(_meta=GraphQLDocument.Meta(op="d")),
        )

    return common.Resource(
        name="boards",
        key=["/_meta/row_id"],
        model=GraphQLDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="boards", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def teams(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_teams, http),
            tombstone=GraphQLDocument(_meta=GraphQLDocument.Meta(op="d")),
        )

    return common.Resource(
        name="teams",
        key=["/_meta/row_id"],
        model=GraphQLDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="teams", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def users(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_users, http),
            tombstone=GraphQLDocument(_meta=GraphQLDocument.Meta(op="d")),
        )

    return common.Resource(
        name="users",
        key=["/_meta/row_id"],
        model=GraphQLDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="users", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def tags(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_tags, http),
            tombstone=GraphQLDocument(_meta=GraphQLDocument.Meta(op="d")),
        )

    return common.Resource(
        name="tags",
        key=["/_meta/row_id"],
        model=GraphQLDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="tags", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def items(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_items, http),
            tombstone=GraphQLDocument(_meta=GraphQLDocument.Meta(op="d")),
        )

    return common.Resource(
        name="items",
        key=["/_meta/row_id"],
        model=GraphQLDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="items", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    return [
        boards(log, http, config),
        teams(log, http, config),
        users(log, http, config),
        tags(log, http, config),
        items(log, http, config),
    ]
