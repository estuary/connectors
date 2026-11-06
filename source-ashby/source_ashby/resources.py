import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture.common import (
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

_ALL_DECLARED_SCOPES = {
    stream_cls.required_scope
    for stream_cls in (*INCREMENTAL_STREAMS, *SNAPSHOT_STREAMS)
}


async def _fetch_available_scopes(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> set[str]:
    # This escape hatch value allows us to run discoveries
    # without having a valid set of credentials.
    if config.credentials.access_token == "ESTUARY_TEST_ACCESS_TOKEN":
        return _ALL_DECLARED_SCOPES

    return await fetch_api_key_scopes(http, log)


def _has_required_scope(
    log: Logger,
    entity_cls: type[AshbyEntity],
    available_scopes: set[str],
) -> bool:
    if entity_cls.required_scope in available_scopes:
        return True

    log.info(f"Skipping {entity_cls.name}: missing scope '{entity_cls.required_scope}'")
    return False


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
        )

    return SnapshotResource(
        name=entity_cls.name,
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

    available_scopes = await _fetch_available_scopes(log, http, config)

    resources: list[AshbyResource] = [
        _create_incremental_resource(stream_cls, http)
        for stream_cls in INCREMENTAL_STREAMS
        if _has_required_scope(log, stream_cls, available_scopes)
    ]
    resources.extend(
        _create_snapshot_resource(stream_cls, http)
        for stream_cls in SNAPSHOT_STREAMS
        if _has_required_scope(log, stream_cls, available_scopes)
    )

    return resources
