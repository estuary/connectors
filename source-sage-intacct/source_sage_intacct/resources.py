import asyncio
import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_changes,
    fetch_creations,
    fetch_deletions,
    fetch_page,
    snapshot,
)
from .models import (
    ConnectorState,
    Document,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    SagePermissionError,
    SnapshotResource,
)
from .sage import PAGE_SIZE, Sage

# REALTIME_LAG defers the "realtime" modified sub-task by this much so that
# a near-simultaneous create-then-modify can't race the "realtime_creations"
# sub-task into emitting an older creation record after a newer
# modification record has already been written.
REALTIME_LAG = timedelta(minutes=2)
LOOKBACK_LAG = timedelta(hours=2)
LOOKBACK_LAG_DELETIONS = timedelta(hours=2, minutes=30)

REALTIME = "realtime"
LOOKBACK = "lookback"
REALTIME_CREATIONS = "realtime_creations"
LOOKBACK_CREATIONS = "lookback_creations"
REALTIME_DELETIONS = "realtime_deletions"
LOOKBACK_DELETIONS = "lookback_deletions"

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
    "ITEM",
    "TASK",
}

SNAPSHOT_OBJECTS = {
    "COMPANYPREF",
    "PROJECT",
}

# TODO: For "TAXSOLUTION", we don't have access to test this object, so we don't
#  know what it looks like it to capture it.


async def incremental_resource(
    sage: Sage, obj: str, started_at: datetime, can_read_audit_history: bool
) -> common.Resource:
    model = await sage.get_model(obj)

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        task.sourced_schema(binding_index, model.sourced_schema())

        # Add state to existing captures that were created before the
        # realtime_creations / lookback_creations sub-tasks were added.
        if (
            isinstance(state.inc, dict)
            and REALTIME_CREATIONS not in state.inc
            and isinstance(existing := state.inc.get(REALTIME), ResourceState.Incremental)
            and isinstance(existing.cursor, datetime)
        ):
            current_cursor = existing.cursor
            creation_state: dict[str, ResourceState.Incremental] = {
                REALTIME_CREATIONS: ResourceState.Incremental(cursor=current_cursor),
                LOOKBACK_CREATIONS: ResourceState.Incremental(cursor=current_cursor - LOOKBACK_LAG),
            }
            state.inc = {**state.inc, **creation_state}

            task.log.info(
                "Adding creation subtask state to incremental state.",
                {"state.inc": state.inc},
            )
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: ResourceState(inc=creation_state)}),
            )
        else:
            await task.checkpoint(state=ConnectorState())

        # There are up to 6 separate subtasks for an incremental resource: 2
        # for capturing updated documents, 2 for capturing newly-created
        # records that haven't otherwise been modified, and 2 for capturing
        # deletions. It is known that the Sage Intacct API is eventually
        # consistent, so it is likely that the "realtime" subtasks will
        # occasionally miss change events, and the "lookback" subtasks will
        # follow behind to true-up the collection.
        #
        # The "realtime" modified subtask has a small REALTIME_LAG horizon.
        # This guards against a race where a record is created with null
        # WHENMODIFIED and then modified within seconds: without the lag, the
        # modified subtask could emit the post-modification document before
        # the creations subtask emits the pre-modification document, and the
        # older document would overwrite the newer one. The lag gives the
        # creations subtask time to poll and emit first.
        #
        # Capturing deletions is done in a separate subtask than creates &
        # updates, since it requires using a different API for polling the
        # audit history object. Note that the "horizon" for the deletions
        # lookback subtask is 2.5 hours instead of 2 hours - this is to
        # mitigate races where a record is created or updated and then
        # immediately deleted, which could otherwise cause the deletion
        # document to get captured before the create/update document.
        # Delaying the deletions lookback by an additional amount of time
        # should prevent such an out-of-order scenario.
        subtasks = {
            REALTIME: functools.partial(
                fetch_changes, obj, sage, REALTIME_LAG, PAGE_SIZE
            ),
            LOOKBACK: functools.partial(
                fetch_changes, obj, sage, LOOKBACK_LAG, PAGE_SIZE
            ),
            REALTIME_CREATIONS: functools.partial(
                fetch_creations, obj, sage, None, PAGE_SIZE
            ),
            LOOKBACK_CREATIONS: functools.partial(
                fetch_creations, obj, sage, LOOKBACK_LAG, PAGE_SIZE
            ),
        }
        # The deletion subtasks are only wired up when the configured user
        # has permission to read AUDITHISTORY. When the permission is
        # missing, their state entries are still seeded in initial_state so
        # that if the permission is granted later, the next connector
        # restart will reattach the deletion subtasks to their preserved
        # cursors and pick up deletions that occurred in the meantime.
        if can_read_audit_history:
            subtasks[REALTIME_DELETIONS] = functools.partial(
                fetch_deletions, obj, sage, None, PAGE_SIZE
            )
            subtasks[LOOKBACK_DELETIONS] = functools.partial(
                fetch_deletions, obj, sage, LOOKBACK_LAG_DELETIONS, PAGE_SIZE
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=subtasks,
            fetch_page=functools.partial(fetch_page, obj, sage, PAGE_SIZE),
        )

    return common.Resource(
        name=obj,
        key=["/RECORDNO"],
        model=Document,
        open=open,
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=started_at),
                LOOKBACK: ResourceState.Incremental(cursor=started_at),
                REALTIME_CREATIONS: ResourceState.Incremental(cursor=started_at),
                LOOKBACK_CREATIONS: ResourceState.Incremental(cursor=started_at),
                REALTIME_DELETIONS: ResourceState.Incremental(cursor=started_at),
                LOOKBACK_DELETIONS: ResourceState.Incremental(cursor=started_at),
            },
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name=obj, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


async def snapshot_resource(sage: Sage, obj: str) -> common.Resource:
    model = await sage.get_model(obj)

    async def open(
        obj: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        task.sourced_schema(binding_index, model.sourced_schema())
        await task.checkpoint(state=ConnectorState())

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

    # Sage gates QUERY access per-object — either via the user's role or
    # via what the tenant's subscription exposes. Probe each candidate up
    # front and drop bindings whose probe returns a PL04000005 denial,
    # rather than failing the whole capture on the first denial. The
    # warning surfaces Sage's own message so the reader can distinguish
    # "your role lacks the grant" from "this object isn't queryable here".
    async def _probe(obj: str) -> tuple[str, str | None]:
        try:
            await sage.probe_query_permission(obj)
            return obj, None
        except SagePermissionError as e:
            return obj, str(e)

    object_names = sorted(INCREMENTAL_OBJECTS | SNAPSHOT_OBJECTS)
    probes = await asyncio.gather(*(_probe(obj) for obj in object_names))
    accessible_objects = {obj for obj, denial in probes if denial is None}
    for obj, denial in probes:
        if denial is not None:
            log.warning(f"omitting binding for {obj}: {denial}")

    can_read_audit_history = True
    try:
        await sage.probe_query_permission("AUDITHISTORY")
    except SagePermissionError as e:
        can_read_audit_history = False
        log.warning(
            f"omitting deletion sub-tasks since AUDITHISTORY cannot be read: {e}. "
        )

    started_at = datetime.now(tz=UTC)
    inc = [
        incremental_resource(sage, obj, started_at, can_read_audit_history)
        for obj in INCREMENTAL_OBJECTS
        if obj in accessible_objects
    ]
    snap = [
        snapshot_resource(sage, obj)
        for obj in SNAPSHOT_OBJECTS
        if obj in accessible_objects
    ]

    # TODO(whb): Add a binding for AUDITHISTORY to capture deletes once we have
    # access to this object type.

    return sorted(await asyncio.gather(*inc, *snap), key=lambda r: r.name)
