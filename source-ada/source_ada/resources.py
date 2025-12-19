from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    FULL_REFRESH_RESOURCES,
    INCREMENTAL_EXPORT_RESOURCES,
    AdaResource,
    EndpointConfig,
    ExportResource,
    ResourceConfig,
    ResourceState,
    Tags,
)
from .api import (
    EVENTUAL_CONSISTENCY_LAG,
    snapshot_resources,
    fetch_export_resources,
    backfill_export_resources,
)

from .shared import now


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        async for _ in snapshot_resources(http, config.bot_handle, Tags, log):
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
            stream: type[AdaResource],
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
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                config.bot_handle,
                stream,
            ),
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
                name=stream.name, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        )
        for stream in FULL_REFRESH_RESOURCES
    ]

    return resources


def incremental_export_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
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
            fetch_changes=functools.partial(
                fetch_export_resources,
                http,
                config.bot_handle,
                stream,
            ),
            fetch_page=functools.partial(
                backfill_export_resources,
                http,
                config.bot_handle,
                stream,
                config.start_date,
            )
        )

    cutoff = now() - EVENTUAL_CONSISTENCY_LAG

    resources = [
        Resource(
            name=stream.name,
            key=["/_id"],
            model=ExportResource,
            open=functools.partial(open, stream),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
            ),
            initial_config=ResourceConfig(
                name=stream.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in INCREMENTAL_EXPORT_RESOURCES
    ]

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        *full_refresh_resources(log, http, config),
        *incremental_export_resources(log, http, config),
    ]
