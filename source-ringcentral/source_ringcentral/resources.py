import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Type

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_incremental_changes,
    fetch_incremental_page,
    fetch_snapshot,
)
from .auth import RingCentralJWTTokenSource
from .models import (
    EndpointConfig,
    Extension,
    ExternalContact,
    FullRefreshResource,
    IncrementalResource,
    InternalContact,
    Message,
    UserCallLog,
)

FULL_REFRESH_RESOURCES: list[Type[FullRefreshResource]] = [
    Extension,
    InternalContact,
    ExternalContact,
]

INCREMENTAL_RESOURCES: list[Type[IncrementalResource]] = [
    UserCallLog,
    Message,
]


def full_refresh_resources(
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    def open(
        model: Type[FullRefreshResource],
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
            fetch_snapshot=functools.partial(
                fetch_snapshot,
                http,
                model=model,
            ),
            tombstone=FullRefreshResource(
                _meta=FullRefreshResource.Meta(op="d")
            ),
        )

    return [
        Resource(
            name=model.NAME,
            key=[model.KEY],
            model=model,
            open=functools.partial(open, model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=model.NAME, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for model in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    cutoff: datetime = datetime.now(UTC)

    def open(
        model: Type[IncrementalResource],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        fetch_changes = functools.partial(
            fetch_incremental_changes,
            http,
            model=model,
            lookback_window=config.advanced.lookback_window,
        )
        fetch_page = functools.partial(
            fetch_incremental_page,
            http,
            model=model,
            start_date=config.start_date,
        )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_changes,
            fetch_page=fetch_page,
        )

    return [
        Resource(
            name=model.NAME,
            key=[model.KEY],
            model=model,
            open=functools.partial(open, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff),
            ),
            initial_config=ResourceConfig(
                name=model.NAME, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for model in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    http.token_source = RingCentralJWTTokenSource(config.credentials)

    return [
        *full_refresh_resources(http, config),
        *incremental_resources(http, config),
    ]
