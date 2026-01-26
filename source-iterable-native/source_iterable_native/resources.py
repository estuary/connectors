import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_campaigns,
    backfill_export_resources,
    fetch_campaign_metrics,
    fetch_campaigns,
    fetch_export_resources,
    snapshot_list_users,
    snapshot_resources,
    snapshot_templates,
)
from .export_job_manager import ExportJobManager
from .models import (
    BaseUsers,
    CampaignMetrics,
    Campaigns,
    Channels,
    EndpointConfig,
    EventTypes,
    Events,
    ExportResource,
    IterableResource,
    ListUsers,
    Lists,
    MessageTypes,
    MetadataTables,
    ProjectType,
    ResourceConfig,
    ResourceState,
    Templates,
    UsersWithEmails,
    UsersWithIds,
)
from .shared import EPOCH, now


AUTHORIZATION_HEADER = "Api-Key"
REALTIME = "realtime"
LOOKBACK = "lookback"

# The lookback window addresses Iterable's eventual consistency, where recent data may not
# immediately appear in export results. We use a dual-cursor strategy: a realtime cursor
# that tracks the current position, and a lookback cursor that trails behind to catch
# late-arriving data.
#
# Iterable's mobile SDKs can queue events when a device is offline, sending them later with
# two timestamps: `createdAt` (when the event was triggered) and `sentAt` (when it was sent
# to Iterable). It is unclear whether Iterable's export API filters by `createdAt` alone or
# by something like `max(createdAt, sentAt)`. If we discover the connector has missed an event,
# it's possible these offline events explain the missing data, and we can investigate how best
# to handle these late arriving offline events at that point.
#
# See: https://support.iterable.com/hc/en-us/articles/360035395671-Tracking-Events-and-Purchases-with-Iterable-s-Mobile-SDKs#offline-events-processing
EVENTUAL_CONSISTENCY_LAG = timedelta(hours=1)

FULL_REFRESH_RESOURCES: list[type[IterableResource]] = [
    Channels,
    MessageTypes,
    MetadataTables,
    Templates,
    Lists,
    ListUsers,
]


def _determine_users_stream(project_type: ProjectType) -> type[BaseUsers]:
    match project_type:
        case ProjectType.EMAIL_BASED:
            return UsersWithEmails
        case ProjectType.USER_ID_BASED | ProjectType.HYBRID:
            return UsersWithIds
        case _:
            raise RuntimeError(f"Unknown Iterable project type: {project_type}")


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)

    try:
        async for _ in snapshot_resources(http, Channels, log):
            break
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:

    def open(
            stream: type[IterableResource],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        if issubclass(stream, Templates):
            snapshot_fn = functools.partial(snapshot_templates, http, stream)
        elif issubclass(stream, ListUsers):
            snapshot_fn = functools.partial(
                snapshot_list_users,
                http,
                stream,
                config.advanced.list_users_timeout,
            )
        else:
            snapshot_fn = functools.partial(snapshot_resources, http, stream)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fn,
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    resources = [
        Resource(
            name=stream.name,
            key=["/_meta/row_id"],
            model=BaseDocument,
            open=functools.partial(open, stream),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=stream.name, interval=stream.interval
            ),
            schema_inference=True,
            disable=stream.disable
        )
        for stream in FULL_REFRESH_RESOURCES
    ]

    return resources


def users(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
        export_job_manager: ExportJobManager,
) -> Resource:
    def open(
            stream: type[BaseUsers],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                REALTIME: functools.partial(
                    fetch_export_resources,
                    http,
                    stream,
                    export_job_manager,
                    stream.data_type,
                    None,
                    config.advanced.window_size,
                ),
                LOOKBACK: functools.partial(
                    fetch_export_resources,
                    http,
                    stream,
                    export_job_manager,
                    stream.data_type,
                    EVENTUAL_CONSISTENCY_LAG,
                    config.advanced.window_size,
                )
            },
            fetch_page=functools.partial(
                backfill_export_resources,
                http,
                stream,
                export_job_manager,
                stream.data_type,
                config.start_date,
                config.advanced.window_size,
            )
        )

    stream = _determine_users_stream(config.project_type)

    cutoff = now()

    return Resource(
        name=stream.name,
        key=[f"/{stream.primary_key}"],
        model=stream,
        open=functools.partial(open, stream),
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=cutoff),
                LOOKBACK: ResourceState.Incremental(cursor=cutoff - EVENTUAL_CONSISTENCY_LAG),
            },
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=ResourceConfig(
            name=stream.name, interval=stream.interval
        ),
        schema_inference=True,
        disable=stream.disable,
    )


def events(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
        export_job_manager: ExportJobManager,
) -> Resource:
    def open(
            stream: type[ExportResource],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                **{
                    f"{REALTIME}_{event_type}": functools.partial(
                        fetch_export_resources,
                        http,
                        stream,
                        export_job_manager,
                        event_type,
                        None,
                        config.advanced.window_size,
                    )
                    for event_type in EventTypes
                },
                **{
                    f"{LOOKBACK}_{event_type}": functools.partial(
                        fetch_export_resources,
                        http,
                        stream,
                        export_job_manager,
                        event_type,
                        EVENTUAL_CONSISTENCY_LAG,
                        config.advanced.window_size,
                    )
                    for event_type in EventTypes
                }
            },
            fetch_page={
                event_type: functools.partial(
                    backfill_export_resources,
                    http,
                    stream,
                    export_job_manager,
                    event_type,
                    config.start_date,
                    config.advanced.window_size,
                )
                for event_type in EventTypes
            },
        )

    stream = Events

    cutoff = now()

    return Resource(
        name=stream.name,
        key=["/_estuary_id", "/eventType"],
        model=stream,
        open=functools.partial(open, stream),
        initial_state=ResourceState(
            inc={
                **{
                    f"{REALTIME}_{event_type}": ResourceState.Incremental(cursor=cutoff)
                    for event_type in EventTypes
                },
                **{
                    f"{LOOKBACK}_{event_type}": ResourceState.Incremental(cursor=cutoff - EVENTUAL_CONSISTENCY_LAG)
                    for event_type in EventTypes
                },
            },
            backfill={
                event_type: ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for event_type in EventTypes
            },
        ),
        initial_config=ResourceConfig(
            name=stream.name, interval=stream.interval
        ),
        schema_inference=True,
        disable=stream.disable,
    )


def campaigns(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
) -> Resource:
    def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_campaigns,
                http,
            ),
            fetch_page=functools.partial(
                backfill_campaigns,
                http,
            ),
        )

    cutoff = now()

    return Resource(
        name=Campaigns.name,
        key=["/id"],
        model=Campaigns,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=ResourceConfig(
            name=Campaigns.name, interval=Campaigns.interval
        ),
        schema_inference=True,
        disable=Campaigns.disable,
    )


def campaign_metrics(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
) -> Resource:
    def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_campaign_metrics,
                http,
            ),
        )

    return Resource(
        name=CampaignMetrics.name,
        key=["/id"],
        model=CampaignMetrics,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=EPOCH),
        ),
        initial_config=ResourceConfig(
            name=CampaignMetrics.name, interval=CampaignMetrics.interval
        ),
        schema_inference=True,
        disable=CampaignMetrics.disable
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_cancel_running_jobs: bool = False,
) -> list[Resource]:
    if http.token_source is None:
        http.token_source = TokenSource(
            oauth_spec=None,
            credentials=config.credentials,
            authorization_header=AUTHORIZATION_HEADER
        )

    export_job_manager = ExportJobManager(http, log)

    if should_cancel_running_jobs:
        await export_job_manager.cancel_all_running_and_enqueued_jobs()

    return [
        *full_refresh_resources(log, http, config),
        users(log, http, config, export_job_manager),
        events(log, http, config, export_job_manager),
        campaigns(log, http, config),
        campaign_metrics(log, http, config),
    ]
