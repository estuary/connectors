import asyncio
from datetime import timedelta
import functools
from logging import Logger
from typing import AsyncGenerator, Callable

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import BaseDocument, ReductionStrategy, ResourceConfig
from estuary_cdk.http import HTTPMixin, TokenSource


from .shared import now

from .models import (
    EndpointConfig,
    AirtableResourceConfigWithSchedule,
    ResourceState,
    Base,
    Table,
    AirtableField,
    create_incremental_record_model,
    PermissionLevel,
    OAUTH2_SPEC,
)
from .api import (
    fetch_tables_in_base,
    fetch_incremental_records,
    backfill_incremental_records,
    snapshot_bases,
    snapshot_tables,
    snapshot_records,
)


SnapshotFn = Callable[[HTTPMixin, Logger], AsyncGenerator[BaseDocument, None]]


MAX_DISCOVERY_CONCURRENCY = 5

# The Airtable API is known to be eventually consistent. The connector utilizes a secondary
# stream that trails behind the present by a LOOKBACK_LAG amount to recheck what the REALTIME
# stream already captured and emit any documents that are new/updated that the REALTIME stream missed.
LOOKBACK = "lookback"
LOOKBACK_LAG = timedelta(hours=1)
# When editing data in Airtable's web UI, each key press is registered as a change. If the connector tried to capture
# changes right up to the present, it could capture data in the middle of a user making many individual key presses.
# The record could then have the mid-key press data until the lookback stream re-captures the record LOOKBACK_LAG later.
# REALTIME_LAG helps the connector avoid capturing users' changes mid-key press.
REALTIME = "realtime"
REALTIME_LAG = timedelta(seconds=10)

FULL_REFRESH_RESOURCES: list[tuple[str, SnapshotFn]] = [
    ("bases", snapshot_bases),
    ("tables", snapshot_tables),
]


def _resolve_resource_name(
    base: Base,
    table: Table,
) -> str:
    return f"{base.name}/{table.name}/{table.id}"


def _get_cursor_field(
    table: Table,
) -> AirtableField | None:
    for field in table.fields:
        if field.is_valid_cursor_field():
            return field
    return None


def full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:

    def open(
        snapshot_fn: SnapshotFn,
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
                snapshot_fn,
                http,
            ),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=BaseDocument,
            open=functools.partial(open, snapshot_fn),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        ) for (name, snapshot_fn) in FULL_REFRESH_RESOURCES
    ]


def incremental_table(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    base: Base,
    table: Table,
    cursor_field: AirtableField,
) -> common.Resource:
    record_cls = create_incremental_record_model(cursor_field.name)

    def open(
        binding: CaptureBinding[AirtableResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        path = f"{base.id}/{table.id}"

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                backfill_incremental_records,
                http,
                path,
                record_cls,
                table.fields,
            ),
            fetch_changes={
                REALTIME: functools.partial(
                    fetch_incremental_records,
                    http,
                    path,
                    record_cls,
                    REALTIME_LAG,
                ),
                LOOKBACK: functools.partial(
                    fetch_incremental_records,
                    http,
                    path,
                    record_cls,
                    LOOKBACK_LAG,
                )
            },
        )

    cutoff = now() - REALTIME_LAG
    name = _resolve_resource_name(base, table)

    return common.Resource(
        name=name,
        key=["/id"],
        model=record_cls,
        open=open,
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=cutoff),
                LOOKBACK: ResourceState.Incremental(cursor=cutoff - LOOKBACK_LAG)
            },
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=AirtableResourceConfigWithSchedule(
            name=name,
            interval=timedelta(minutes=5),
            # Default to performing a formula field refresh at 23:55 UTC every day for every enabled binding.
            schedule="55 23 * * *"
        ),
        schema_inference=True,
        reduction_strategy=ReductionStrategy.MERGE,
    )


def full_refresh_table(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    base: Base,
    table: Table,
) -> common.Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        path = f"{base.id}/{table.id}"

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_records,
                http,
                path,
            ),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    name = _resolve_resource_name(base, table)

    return common.Resource(
        name=name,
        key=["/_meta/row_id"],
        model=BaseDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=name, interval=timedelta(minutes=15)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    resources: list[common.Resource] = [
        *full_refresh_resources(log, http, config)
    ]

    accessible_bases: list[Base] = []

    async for b in snapshot_bases(http, log):
        match b.permissionLevel:
            case PermissionLevel.CREATE | PermissionLevel.EDIT | PermissionLevel.COMMENT | PermissionLevel.READ:
                accessible_bases.append(b)
            case _:
                log.debug(f"Omitting base {b.name} (id: {b.id}) and its tables from the list of discovered resources due to insufficient permission level: {b.permissionLevel}.")

    semaphore = asyncio.Semaphore(MAX_DISCOVERY_CONCURRENCY)

    async def fetch_resources_for_base(base: Base) -> list[common.Resource]:
        async with semaphore:
            base_resources: list[common.Resource] = []
            async for table in fetch_tables_in_base(http, base.id, log):
                cursor_field = _get_cursor_field(table)
                if cursor_field is not None:
                    base_resources.append(incremental_table(log, http, config, base, table, cursor_field))
                else:
                    base_resources.append(full_refresh_table(log, http, config, base, table))
            return base_resources

    results = await asyncio.gather(
        *(fetch_resources_for_base(base) for base in accessible_bases)
    )

    for base_resources in results:
        resources.extend(base_resources)

    return resources
