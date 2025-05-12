import asyncio
import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_changes,
    fetch_page,
    snapshot,
)
from .models import (
    ConnectorState,
    EndpointConfig,
    IncrementalResource,
    ResourceConfig,
    ResourceState,
    SnapshotResource,
)
from .sage import PAGE_SIZE, Sage

# INCREMENTAL_OBJECTS are those that have a WHENMODIFIED timestamp and support
# queries that filter and sort by that.
INCREMENTAL_OBJECTS = {
    "CUSTOMER",
    "APTERM",
    "CLASS",
    "DEPARTMENT",
    "EMPLOYEE",
    "GLACCOUNT",
    "LOCATION",
    "TAXDETAIL",
    "VENDOR",
    "TRXCURRENCIES",
    "GLJOURNAL",
    "PROJECT",
    "ITEM",
    "TASK",
}

SNAPSHOT_OBJECTS = {
    "COMPANYPREF",
}

# TODO: For "TAXSOLUTION", we don't have access to test this object, so we don't
#  know what it looks like it to capture it.


async def incremental_resource(
    sage: Sage, obj: str, started_at: datetime
) -> common.Resource:
    model = await sage.get_model(obj)

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        task.sourced_schema(binding_index, model.sourced_schema())
        task.checkpoint(state=ConnectorState())

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                "realtime": functools.partial(
                    fetch_changes, obj, sage, None, PAGE_SIZE
                ),
                "lookback": functools.partial(
                    fetch_changes, obj, sage, timedelta(hours=2), PAGE_SIZE
                ),
            },
            fetch_page=functools.partial(fetch_page, obj, sage, PAGE_SIZE),
        )

    return common.Resource(
        name=obj,
        key=["/RECORDNO"],
        model=IncrementalResource,
        open=open,
        initial_state=ResourceState(
            inc={
                "realtime": ResourceState.Incremental(cursor=started_at),
                "lookback": ResourceState.Incremental(cursor=started_at),
            },
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=obj, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


async def snapshot_resource(sage: Sage, obj: str) -> common.Resource:
    model = await sage.get_model(obj)

    def open(
        obj: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        task.sourced_schema(binding_index, model.sourced_schema())
        task.checkpoint(state=ConnectorState())

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot, obj, sage, PAGE_SIZE),
            tombstone=SnapshotResource(_meta=SnapshotResource.Meta(op="d")),
        )

    return common.Resource(
        name=obj,
        key=["/_meta/row_id"],
        model=SnapshotResource,
        open=functools.partial(open, obj),
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=obj, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    sage = Sage(log, http, config)
    await sage.setup()

    started_at = datetime.now(tz=UTC)
    inc = [incremental_resource(sage, obj, started_at) for obj in INCREMENTAL_OBJECTS]
    snap = [snapshot_resource(sage, obj) for obj in SNAPSHOT_OBJECTS]

    # TODO(whb): Add a binding for AUDITHISTORY to capture deletes once we have
    # access to this object type.

    return sorted(await asyncio.gather(*inc, *snap), key=lambda r: r.name)
