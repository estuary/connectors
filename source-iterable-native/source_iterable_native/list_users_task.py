import asyncio
import functools
from datetime import UTC, datetime, timedelta

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    AssociatedDocument,
    BaseDocument,
    ConnectorState,
    FetchChangesFn,
)
from estuary_cdk.flow import CaptureBinding

from .api import get_list_id_from_cursor
from .models import ListUsers, ResourceConfig, ResourceState


async def _list_users_binding_task(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    fetch_changes: FetchChangesFn[ListUsers],
    state: ResourceState.Incremental,
    task: Task,
):
    """
    Custom incremental task for list_users that allows mixed cursor types.

    This is based on _binding_incremental_task from the CDK, but relaxes the
    restriction that all yielded LogCursors must be the same type. This allows
    list_users to use:
    - datetime cursor: sweep complete, triggers interval sleep
    - tuple cursor: mid-sweep, continue immediately

    The tuple cursor format is: ("{sweep_start_iso}|{last_list_id}",)
    """
    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(inc=state)}
    )

    sleep_for = timedelta()

    task.log.info("resuming incremental replication", {"state": state})

    # Initial sleep logic for datetime cursors
    if isinstance(state.cursor, datetime):
        lag = datetime.now(tz=UTC) - state.cursor
        if lag < binding.resourceConfig.interval:
            sleep_for = binding.resourceConfig.interval - lag
            task.log.info(
                "incremental task ran recently, sleeping until `interval` has fully elapsed",
                {"sleep_for": sleep_for, "interval": binding.resourceConfig.interval},
            )

    while True:
        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.log.debug("incremental replication is idle and is yielding to stop")
            return
        except asyncio.TimeoutError:
            pass  # `sleep_for` elapsed

        checkpoints = 0
        pending = False

        async for item in fetch_changes(task.log, state.cursor):
            if isinstance(item, BaseDocument) or isinstance(item, dict):
                task.captured(binding_index, item)
                pending = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                pending = True
            else:
                # LogCursor - allow both datetime and tuple types
                # For comparison, we check if the cursor is "larger" in a type-appropriate way
                is_valid = False

                if isinstance(item, datetime):
                    if isinstance(state.cursor, datetime):
                        is_valid = item > state.cursor
                    elif isinstance(state.cursor, tuple):
                        # Switching from tuple to datetime (sweep complete)
                        is_valid = True
                elif isinstance(item, tuple):
                    if isinstance(state.cursor, tuple):
                        # Compare by list_id within the same sweep
                        item_list_id = get_list_id_from_cursor(item)
                        cursor_list_id = get_list_id_from_cursor(state.cursor)
                        is_valid = item_list_id > cursor_list_id
                    elif isinstance(state.cursor, datetime):
                        # Switching from datetime to tuple (sweep starting)
                        is_valid = True

                if not is_valid:
                    raise RuntimeError(
                        f"Implementation error: fetch_list_users yielded LogCursor {item} "
                        f"which is not valid relative to the previous LogCursor {state.cursor}",
                    )

                state.cursor = item
                await task.checkpoint(connector_state)
                checkpoints += 1
                pending = False

        if pending:
            raise RuntimeError(
                "Implementation error: fetch_list_users yielded documents without a final LogCursor",
            )

        if not checkpoints:
            # We're idle. Sleep for the full back-off interval.
            sleep_for = binding.resourceConfig.interval

        elif isinstance(state.cursor, datetime):
            # Sweep complete - sleep based on interval.
            lag = datetime.now(tz=UTC) - state.cursor

            if lag > binding.resourceConfig.interval:
                # Cursor is old enough, start a new sweep.
                sleep_for = timedelta()
                continue
            else:
                # Sleep until the cursor is `interval` old.
                sleep_for = binding.resourceConfig.interval - lag
        else:
            # Tuple cursor - mid-sweep. Continue immediately.
            sleep_for = timedelta()
            continue

        task.log.debug(
            "incremental task is idle",
            {"sleep_for": sleep_for, "cursor": state.cursor},
        )


def open_list_users_binding(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    fetch_changes: FetchChangesFn[ListUsers],
):
    task.connector_status.inc_binding_count()
    prefix = ".".join(binding.resourceConfig.path())

    assert resource_state.inc and not isinstance(resource_state.inc, dict)
    task.spawn_child(
        f"{prefix}.incremental",
        functools.partial(
            _list_users_binding_task,
            binding,
            binding_index,
            fetch_changes,
            resource_state.inc,
        ),
    )
