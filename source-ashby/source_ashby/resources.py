import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture.common import (
    BaseDocument,
    Resource,
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    Task,
    open_binding,
)
from estuary_cdk.flow import BasicAuth, CaptureBinding
from estuary_cdk.http import HTTPMixin, TokenSource

from .api import (
    fetch_api_key_scopes,
    fetch_entity,
    fetch_incremental_child_entity,
    snapshot_child_entity,
    snapshot_entity,
)
from .models import (
    INCREMENTAL_STREAMS,
    SNAPSHOT_STREAMS,
    AshbyEntity,
    AshbySnapshotEntity,
    ChildEntityMixin,
    EndpointConfig,
)

AshbyResource = Resource[AshbyEntity, ResourceConfig, ResourceState]


async def filter_resources_by_scopes(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    resources: list[AshbyResource],
) -> list[AshbyResource]:
    # This escape hatch value allows us to run discoveries
    # without having a valid set of credentials
    if config.credentials.access_token == "ESTUARY_TEST_ACCESS_TOKEN":
        return resources

    available_scopes = await fetch_api_key_scopes(http, log)

    filtered: list[AshbyResource] = []
    for resource in resources:
        model = resource.model
        assert isinstance(model, type) and issubclass(model, AshbyEntity)
        required_scope = model.required_scope

        if required_scope in available_scopes:
            filtered.append(resource)
        else:
            log.info(f"Skipping {resource.name}: missing scope '{required_scope}'")

    return filtered


async def validate_credentials(
    http: HTTPMixin, config: EndpointConfig, log: Logger
) -> None:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=BasicAuth(
            username=config.credentials.access_token,
            password="",
        ),
    )

    _ = await fetch_api_key_scopes(http, log)


def _create_incremental_resource(
    entity_cls: type[AshbyEntity], http: HTTPMixin
) -> AshbyResource:
    fetch_fn = (
        fetch_incremental_child_entity
        if issubclass(entity_cls, ChildEntityMixin)
        else fetch_entity
    )

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
            # Since incremental fetches couldn't begin until a complete backfill was performed,
            # it is impossible to define two discrete functions — the fetch function does both.
            fetch_changes=functools.partial(fetch_fn, entity_cls, http),
        )

    return Resource(
        name=entity_cls.name,
        key=["/id"],
        model=entity_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=("",)),
        ),
        initial_config=ResourceConfig(
            name=entity_cls.name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


def _create_snapshot_resource(
    entity_cls: type[AshbySnapshotEntity], http: HTTPMixin
) -> AshbyResource:
    snapshot_fn = (
        snapshot_child_entity
        if issubclass(entity_cls, ChildEntityMixin)
        else snapshot_entity
    )

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
            fetch_snapshot=functools.partial(snapshot_fn, entity_cls, http),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d")),
        )

    return SnapshotResource(
        name=entity_cls.name,
        model=entity_cls,
        open=open,
        initial_config=ResourceConfig(
            name=entity_cls.name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[AshbyResource]:
    if http.token_source is None:
        http.token_source = TokenSource(
            oauth_spec=None,
            credentials=BasicAuth(
                username=config.credentials.access_token,
                password="",
            ),
        )

    resources: list[AshbyResource] = [
        _create_incremental_resource(stream_cls, http)
        for stream_cls in INCREMENTAL_STREAMS
    ]
    resources.extend(
        _create_snapshot_resource(stream_cls, http) for stream_cls in SNAPSHOT_STREAMS
    )

    return await filter_resources_by_scopes(log, http, config, resources)
