import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    BaseDocument,
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, TokenSource

from .api import (
    backfill_entity,
    fetch_entity,
    snapshot_child_entity,
    snapshot_entity,
    fetch_user_info,
)
from .models import (
    INCREMENTAL_STREAMS,
    SNAPSHOT_STREAMS,
    CalendlyChildEntity,
    CalendlyEntity,
    EndpointConfig,
)

CalendlyResource = Resource[CalendlyEntity, ResourceConfig, ResourceState]


async def validate_credentials(
    http: HTTPMixin, config: EndpointConfig, log: Logger
) -> None:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    _ = await fetch_user_info(http, log)


def _incremental_resources(
    http: HTTPMixin, config: EndpointConfig, org_uri: str
) -> list[CalendlyResource]:
    cutoff = datetime.now(tz=UTC)

    def open(
        entity_cls: type[CalendlyEntity],
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
            fetch_changes=functools.partial(fetch_entity, entity_cls, org_uri, http),
            fetch_page=functools.partial(
                backfill_entity, entity_cls, org_uri, http, config
            ),
        )

    return [
        Resource(
            name=entity_cls.name,
            key=["/uri"],
            model=entity_cls,
            open=functools.partial(open, entity_cls),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(
                name=entity_cls.name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        )
        for entity_cls in INCREMENTAL_STREAMS
    ]


def _snapshot_resources(http: HTTPMixin, org_uri: str) -> list[CalendlyResource]:
    def open(
        entity_cls: type[CalendlyEntity],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        snapshot_fn = (
            snapshot_child_entity
            if issubclass(entity_cls, CalendlyChildEntity)
            else snapshot_entity
        )

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_fn, entity_cls, org_uri, http),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d")),
        )

    return [
        Resource(
            name=entity_cls.name,
            key=["/_meta/row_id"],
            model=entity_cls,
            open=functools.partial(open, entity_cls),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=entity_cls.name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        )
        for entity_cls in SNAPSHOT_STREAMS
    ]


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[CalendlyResource]:
    if http.token_source is None:
        http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    user_info = await fetch_user_info(http, log)
    org_uri = user_info.current_organization

    return [
        *_incremental_resources(http, config, org_uri),
        *_snapshot_resources(http, org_uri),
    ]
