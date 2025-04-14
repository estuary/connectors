from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    OutreachResource,
    CursorField,
    INCREMENTAL_RESOURCES,
    OAUTH2_SPEC,
)
from .api import (
    backfill_resources,
    fetch_resources,
)


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        path: str,
        params: dict[str, str | int | bool] | None,
        cursor_field: CursorField,
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
            fetch_changes=functools.partial(
                fetch_resources,
                http,
                path,
                params,
                cursor_field,
            ),
            fetch_page=functools.partial(
                backfill_resources,
                http,
                path,
                params,
                cursor_field,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=OutreachResource,
            open=functools.partial(open, path, params, cursor_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, params, cursor_field) in INCREMENTAL_RESOURCES
    ]

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    resources = [
        *incremental_resources(log, http, config),
    ]

    return resources
