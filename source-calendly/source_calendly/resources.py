import functools
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Callable

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    BaseDocument,
    LogCursor,
    Resource,
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    open_binding,  # pyright: ignore[reportUnknownVariableType]
)
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, HTTPSession, TokenSource

from .api import (
    backfill_entity,
    fetch_entity,
    fetch_event_invitees,
    fetch_scheduled_events,
    fetch_user_info,
    is_teams_account,
    snapshot_child_entity,
    snapshot_entity,
)
from .models import (
    INCREMENTAL_STREAMS,
    SNAPSHOT_STREAMS,
    CalendlyChildEntity,
    CalendlyEntity,
    EndpointConfig,
    EventInvitee,
    ScheduledEvent,
)

CalendlyResource = Resource[CalendlyEntity, ResourceConfig, ResourceState]

CalendlySnapshotResource = SnapshotResource[CalendlyEntity, ResourceConfig]
ScheduledEventFetchFn = Callable[
    [str, int, int, HTTPSession, Logger, LogCursor],
    AsyncGenerator[CalendlyEntity | LogCursor, None],
]


async def validate_credentials(
    http: HTTPMixin, config: EndpointConfig, log: Logger
) -> None:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    _ = await fetch_user_info(http, log)


def _incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
    org_uri: str,
    streams: list[type[CalendlyEntity]],
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
                backfill_entity, entity_cls, org_uri, config.start_date, http
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
        for entity_cls in streams
    ]


def _snapshot_resources(
    http: HTTPMixin,
    org_uri: str,
    streams: list[type[CalendlyEntity]],
) -> list[CalendlyResource]:
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
        CalendlySnapshotResource(
            name=entity_cls.name,
            open=functools.partial(open, entity_cls),
            initial_config=ResourceConfig(
                name=entity_cls.name,
                interval=timedelta(minutes=5),
            ),
        )
        for entity_cls in streams
    ]


def _scheduled_event_resources(
    http: HTTPMixin, config: EndpointConfig, org_uri: str
) -> list[CalendlyResource]:
    def open(
        fetch_fn: ScheduledEventFetchFn,
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
                fetch_fn,
                org_uri,
                config.scheduled_event_lookback_months,
                config.scheduled_event_lookahead_months,
                http,
            ),
        )

    return [
        Resource(
            name=ScheduledEvent.name,
            key=["/uri"],
            model=ScheduledEvent,
            open=functools.partial(open, fetch_scheduled_events),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=ScheduledEvent.name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        ),
        Resource(
            name=EventInvitee.name,
            key=["/uri"],
            model=EventInvitee,
            open=functools.partial(open, fetch_event_invitees),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=EventInvitee.name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        ),
    ]


def _available_streams(
    log: Logger,
    has_teams_access: bool,
    streams: list[type[CalendlyEntity]],
) -> list[type[CalendlyEntity]]:
    if has_teams_access:
        return streams

    available: list[type[CalendlyEntity]] = []
    for entity_cls in streams:
        if entity_cls.teams_only:
            log.info(f"Skipping {entity_cls.name}: requires a teams account")
        else:
            available.append(entity_cls)

    return available


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[CalendlyResource]:
    if http.token_source is None:
        http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    user_info = await fetch_user_info(http, log)
    org_uri = user_info.current_organization
    has_teams_access = await is_teams_account(org_uri, http, log)

    return [
        *_incremental_resources(
            http,
            config,
            org_uri,
            _available_streams(log, has_teams_access, INCREMENTAL_STREAMS),
        ),
        *_scheduled_event_resources(http, config, org_uri),
        *_snapshot_resources(
            http, org_uri, _available_streams(log, has_teams_access, SNAPSHOT_STREAMS)
        ),
    ]
