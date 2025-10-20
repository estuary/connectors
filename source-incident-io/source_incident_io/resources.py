import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_catalog_entries,
    backfill_client_side_filtered_resources,
    backfill_incidents,
    fetch_catalog_entries,
    fetch_client_side_filtered_resources,
    fetch_incidents,
    snapshot_incident_attachments,
    snapshot_resources,
)
from .models import (
    FULL_REFRESH_RESOURCES,
    INCREMENTAL_RESOURCES,
    BaseDocument,
    CatalogEntries,
    EndpointConfig,
    FullRefreshStream,
    IncidentAttachments,
    Incidents,
    IncrementalStream,
    ResourceConfig,
    ResourceState,
)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        async for _ in fetch_incidents(http, Incidents, log, datetime.now(tz=UTC)):
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
) -> list[common.Resource]:

    def open(
            stream: type[FullRefreshStream],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        if stream is IncidentAttachments:
            snapshot_fn = snapshot_incident_attachments
        else:
            snapshot_fn = snapshot_resources

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_fn,
                http,
                stream,
            ),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    return [
        common.Resource(
            name=stream.name,
            key=["/_meta/row_id"],
            model=BaseDocument,
            open=functools.partial(open, stream),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=stream.name, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        ) for stream in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            stream: type[IncrementalStream],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        if stream is Incidents:
            fetch_changes_fn = fetch_incidents
            fetch_page_fn = backfill_incidents
        elif stream is CatalogEntries:
            fetch_changes_fn = fetch_catalog_entries
            fetch_page_fn = backfill_catalog_entries
        else:
            fetch_changes_fn = fetch_client_side_filtered_resources
            fetch_page_fn = backfill_client_side_filtered_resources

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_changes_fn,
                http,
                stream,
            ),
            fetch_page=functools.partial(
                fetch_page_fn,
                http,
                stream,
            ),
        )

    cutoff = datetime.now(tz=UTC)

    return [
        common.Resource(
            name=stream.name,
            key=["/id"],
            model=IncrementalStream,
            open=functools.partial(open, stream),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(
                name=stream.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        ) for stream in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    resources = [
        *full_refresh_resources(log, http, config),
        *incremental_resources(log, http, config),
    ]

    return resources
