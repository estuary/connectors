import asyncio
from collections import defaultdict
import typing

from estuary_cdk.logger import FlowLogger

if typing.TYPE_CHECKING:
    from estuary_cdk.capture.task import Task
    StoppingType = Task.Stopping
else:
    StoppingType = typing.Any


class ConnectorStatus:
    """
    Centralized instrumentation and logging of connectorStatus messages.

    For captures that use common.open_binding(), this keeps track of how many
    bindings there are in total, and how many are backfilling. Changes to these
    counts will be logged as connectorStatus's.
    """

    def __init__(self, log: FlowLogger, stopping: StoppingType):
        async def periodic_log():
            # Allow some initial setup time for bindings to be registered before
            # starting to poll for status changes every second.
            await asyncio.sleep(5)
            while not stopping.event.is_set():
                self._log_status()
                await asyncio.sleep(1)

        # Periodically log connector statuses.
        # We don't do this within a TaskGroup because we don't
        # want to block on it.
        asyncio.create_task(periodic_log())

        self.log = log
        self.binding_count = 0
        self.binding_backfill_counts = defaultdict(int)
        self.should_log = False

    def inc_binding_count(self):
        self.binding_count += 1
        self.should_log = True

    def inc_backfilling(self, binding_idx: int):
        self.binding_backfill_counts[binding_idx] += 1
        self.should_log = True

    def dec_backfilling(self, binding_idx: int):
        if binding_idx not in self.binding_backfill_counts:
            raise Exception(
                f"cannot decrement backfilling count for binding idx {binding_idx} because it is not in binding_backfill_counts"
            )

        self.binding_backfill_counts[binding_idx] -= 1
        if self.binding_backfill_counts[binding_idx] == 0:
            del self.binding_backfill_counts[binding_idx]

        self.should_log = True

    def _log_status(self):
        if not self.should_log:
            return

        if (backfill_count := len(self.binding_backfill_counts)) > 0:
            self.log.event.status(f"Backfilling {backfill_count} out of {self.binding_count} bindings")
        else:
            self.log.event.status(f"Streaming change events (all {self.binding_count} bindings are backfilled)")

        self.should_log = False
