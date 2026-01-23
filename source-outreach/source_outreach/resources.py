from datetime import datetime, timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .models import (
    ConnectorState,
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
from .shared import now


REALTIME = "realtime"
LOOKBACK = "lookback"
LOOKBACK_LAG = timedelta(hours=6)


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def open(
        path: str,
        params: dict[str, str | int | bool] | None,
        cursor_field: CursorField,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Migrate from old single-cursor state to new subtask state format.
        #
        # JSON Merge Patch (RFC 7396) interprets null values as "delete this key".
        # By including "cursor": None in the dict, we remove the old cursor field
        # while adding the new realtime/lookback subtasks in a single checkpoint.
        if isinstance(state.inc, ResourceState.Incremental) and isinstance(state.inc.cursor, datetime):
            old_cursor = state.inc.cursor
            migrated_inc_state: dict[str, ResourceState.Incremental | None] = {
                "cursor": None,  # Remove old cursor key via merge-patch
                REALTIME: ResourceState.Incremental(cursor=old_cursor),
                LOOKBACK: ResourceState.Incremental(cursor=old_cursor - LOOKBACK_LAG),
            }
            state.inc = migrated_inc_state

            log.info("Migrating incremental state.", {
                "migrated_inc_state": migrated_inc_state,
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: ResourceState(inc=migrated_inc_state)}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                REALTIME: functools.partial(
                    fetch_resources,
                    http,
                    path,
                    params,
                    cursor_field,
                    None,
                ),
                LOOKBACK: functools.partial(
                    fetch_resources,
                    http,
                    path,
                    params,
                    cursor_field,
                    LOOKBACK_LAG,
                ),
            },
            fetch_page=functools.partial(
                backfill_resources,
                http,
                path,
                params,
                cursor_field,
                config.start_date,
            )
        )

    cutoff = now() - LOOKBACK_LAG

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=OutreachResource,
            open=functools.partial(open, path, params, cursor_field),
            initial_state=ResourceState(
                inc={
                    REALTIME: ResourceState.Incremental(cursor=cutoff),
                    LOOKBACK: ResourceState.Incremental(cursor=cutoff - LOOKBACK_LAG),
                },
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
