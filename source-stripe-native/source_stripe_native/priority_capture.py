import asyncio
import heapq
from datetime import datetime, timedelta, UTC
from enum import IntEnum
from typing import Dict, cast, NamedTuple, List, Optional, Union, Protocol
from dataclasses import dataclass, field

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    CaptureBinding,
    ResourceState,
    ConnectorState,
    FetchChangesFn,
    FetchPageFn,
    RecurringFetchPageFn,
    BaseDocument,
    AssociatedDocument,
    Triggers,
    is_recurring_fetch_page_fn,
    ResourceConfig,
    FetchSnapshotFn,
)

# To avoid storing all fetch_changes and fetch_page function in memory, factory functions
# are used to create the fetch functions on demand for a specific account.
class FetchChangesFnFactory(Protocol):
    """Factory function for creating incremental fetch functions for a specific account."""
    def __call__(self, account_id: str) -> FetchChangesFn[BaseDocument]:
        ...


class FetchPageFnFactory(Protocol):
    """Factory function for creating backfill fetch functions for a specific account."""
    def __call__(self, account_id: str) -> Optional[Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]]]:
        ...


class PriorityLevel(IntEnum):
    BACKFILL = 0
    INCREMENTAL = 1


class WorkItemPriority(NamedTuple):
    """
    Represents priority information for a work item in a structured way.
    
    Work items are prioritized based on:
    1. Their backfill status (priority_level=0 for incomplete backfills, priority_level=1 for completed backfills).
    2. Their incremental cursor timestamp (older timestamps have higher priority).

    Work items that still need to backfill are prioritized over those that have completed their backfill already.
    Work items that have completed their backfill are prioritized based on their incremental cursor timestamp - the older the timestamp, the higher the priority.
    """
    level: PriorityLevel
    cursor_timestamp: datetime | None 
    account_id: str

    def __lt__(self, other):
        """Custom comparison for priority ordering."""
        if not isinstance(other, WorkItemPriority):
            raise TypeError(f"Cannot compare WorkItemPriority with {type(other).__name__}")

        if self.level != other.level:
            return self.level < other.level

        # For same priority level, compare by cursor timestamp.
        if self.cursor_timestamp is None and other.cursor_timestamp is None:
            # Both are backfills, sort by account_id for consistency.
            return self.account_id < other.account_id
        elif self.cursor_timestamp is None:
            # Self needs to backfill, other does not need to backfill.
            return True
        elif other.cursor_timestamp is None:
            # Self does not need to backfill, other needs to backfill.
            return False
        else:
            # Both have completed backfills, so the older incremental cursor has the higher priority.
            if self.cursor_timestamp == other.cursor_timestamp:
                # If timestamps are equal, sort by account_id for consistency.
                return self.account_id < other.account_id
            return self.cursor_timestamp < other.cursor_timestamp


@dataclass
class WorkItem:
    account_id: str
    priority: WorkItemPriority

    incremental_fetch_fn: FetchChangesFn[BaseDocument]
    incremental_state: ResourceState.Incremental

    backfill_fetch_fn: Optional[Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]]] = None
    backfill_state: Optional[ResourceState.Backfill] = None

    # Shared cancellation for both subtasks of this work item.
    cancellation_event: asyncio.Event = field(default_factory=asyncio.Event)

    def __lt__(self, other):
        """Enable priority ordering in PriorityQueue."""
        if not isinstance(other, WorkItem):
            raise TypeError(f"Cannot compare WorkItem with {type(other).__name__}")

        return self.priority < other.priority

    def has_backfill_work(self) -> bool:
        return self.backfill_fetch_fn is not None and self.backfill_state is not None


@dataclass
class PriorityQueueConfig:
    priority_refresh_interval: timedelta = timedelta(seconds=15)
    max_subtask_count: int = 40
    work_queue_size: int = 100


DEFAULT_CONFIG = PriorityQueueConfig()


class PriorityCalculator:
    def __init__(self, resource_state: ResourceState):
        self.resource_state = resource_state

    def get_account_priority(self, account_id: str) -> WorkItemPriority:
        """
        Calculate priority for connected account using structured priority tuple.

        Priority order:
        1. Accounts with incomplete backfills (priority_level=0)
        2. Accounts with incremental cursors, ordered by datetime (priority_level=1, older timestamp = higher priority)
        """
        if isinstance(self.resource_state.backfill, dict) and self.resource_state.backfill.get(account_id):
            return WorkItemPriority(level=PriorityLevel.BACKFILL, cursor_timestamp=None, account_id=account_id)

        if isinstance(self.resource_state.inc, dict):
            inc_state = self.resource_state.inc.get(account_id)
            if inc_state and isinstance(inc_state.cursor, datetime):
                return WorkItemPriority(
                    level=PriorityLevel.INCREMENTAL, 
                    cursor_timestamp=inc_state.cursor, 
                    account_id=account_id
                )

        raise RuntimeError(
            f"Account {account_id} has no state in inc or backfill. This should not happen "
            "after state reconciliation. Check _reconcile_connector_state implementation and usage."
        )

    def select_prioritized_accounts(self, all_account_ids: List[str], max_subtask_count: int) -> set[str]:
        account_priorities = [
            self.get_account_priority(account_id)
            for account_id in all_account_ids
        ]

        # Use heapq for efficient partial sorting - only sort as much as needed.
        top_priority_accounts = heapq.nsmallest(max_subtask_count, account_priorities)

        subtask_count = 0
        selected_accounts: list[WorkItemPriority] = []

        for account in top_priority_accounts:
            match account.level:
                case PriorityLevel.BACKFILL:
                    subtask_count += 2
                case PriorityLevel.INCREMENTAL:
                    subtask_count += 1
                case _:
                    raise RuntimeError(f"Unknown priority level {account.level}")

            selected_accounts.append(account)

            if subtask_count >= max_subtask_count:
                break

        return {priority.account_id for priority in selected_accounts}


class WorkItemManager:
    def __init__(self, priority_calculator: PriorityCalculator):
        self.priority_calculator = priority_calculator
        self.active_work_by_account: Dict[str, WorkItem] = {}
    
    def generate_work_items(
        self,
        all_account_ids: List[str],
        max_subtask_count: int,
        fetch_changes_factory: FetchChangesFnFactory,
        fetch_page_factory: FetchPageFnFactory,
        resource_state: ResourceState,
    ) -> tuple[list[WorkItem], set[str]]:
        work_items: list[WorkItem] = []

        priority_account_ids: set[str] = self.priority_calculator.select_prioritized_accounts(all_account_ids, max_subtask_count)

        for account_id in priority_account_ids:
            # Do not create duplicate work items for accounts that already have active work.
            if account_id in self.active_work_by_account:
                continue

            work_item = self._create_work_item_for_account(
                account_id, fetch_changes_factory, fetch_page_factory, resource_state
            )
            work_items.append(work_item)

        return work_items, priority_account_ids

    def _create_work_item_for_account(
        self, account_id: str,
        fetch_changes_factory: FetchChangesFnFactory,
        fetch_page_factory: FetchPageFnFactory,
        resource_state: ResourceState
    ) -> WorkItem:
        priority = self.priority_calculator.get_account_priority(account_id)

        incremental_fetch_fn, incremental_state = self._get_incremental_work(account_id, fetch_changes_factory, resource_state)

        backfill_fetch_fn, backfill_state = self._get_backfill_work(account_id, fetch_page_factory, resource_state)

        return WorkItem(
            account_id=account_id,
            priority=priority,
            incremental_fetch_fn=incremental_fetch_fn,
            incremental_state=incremental_state,
            backfill_fetch_fn=backfill_fetch_fn,
            backfill_state=backfill_state
        )

    def _get_incremental_work(
        self,
        account_id: str,
        fetch_changes_factory: FetchChangesFnFactory,
        resource_state: ResourceState
    ) -> tuple[FetchChangesFn[BaseDocument], ResourceState.Incremental]:
        if isinstance(resource_state.inc, dict):
            inc_state = resource_state.inc.get(account_id)
            if inc_state:
                fetch_changes_fn = fetch_changes_factory(account_id)
                return fetch_changes_fn, inc_state

        raise RuntimeError(f"Account {account_id} is missing incremental state. This should not happen after state reconciliation.")

    def _get_backfill_work(
        self, account_id: str,
        fetch_page_factory: FetchPageFnFactory,
        resource_state: ResourceState
    ) -> tuple[Optional[Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]]], Optional[ResourceState.Backfill]]:
        if isinstance(resource_state.backfill, dict):
            backfill_state = resource_state.backfill.get(account_id)
            if backfill_state:
                fetch_page_fn = fetch_page_factory(account_id)
                return fetch_page_fn, backfill_state

        return None, None

    def cancel_work_for_account(self, account_id: str):
        if account_id in self.active_work_by_account:
            work_item = self.active_work_by_account[account_id]
            work_item.cancellation_event.set()

    def register_active_work(self, work_item: WorkItem):
        self.active_work_by_account[work_item.account_id] = work_item

    def unregister_active_work(self, work_item: WorkItem):
        self.active_work_by_account.pop(work_item.account_id, None)


class SubtaskExecutor:
    def __init__(
        self,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int, 
        resource_state: ResourceState,
        prefix: str,
        main_task: Task
    ):
        self.binding = binding
        self.binding_index = binding_index
        self.resource_state = resource_state
        self.prefix = prefix
        self.main_task = main_task

    async def _execute_incremental_subtask(self, work_item: WorkItem, task: Task):
        await _binding_incremental_task_with_work_item(
            self.binding,
            self.binding_index,
            work_item.incremental_fetch_fn,
            work_item.incremental_state,
            task,
            work_item
        )

    async def _execute_backfill_subtask(self, work_item: WorkItem, task: Task):
        if not work_item.has_backfill_work():
            return

        assert work_item.backfill_fetch_fn is not None
        assert work_item.backfill_state is not None

        self.main_task.connector_status.inc_backfilling(self.binding_index)

        await _binding_backfill_task_with_work_item(
            self.binding,
            self.binding_index,
            work_item.backfill_fetch_fn,
            work_item.backfill_state,
            self.resource_state.last_initialized,
            self.resource_state.is_connector_initiated,
            task,
            work_item
        )

        # Update the resource state to reflect backfill completion.
        # This ensures PriorityCalculator sees the completed backfill during periodic re-prioritization.
        if isinstance(self.resource_state.backfill, dict):
            self.resource_state.backfill[work_item.account_id] = None

        self.main_task.connector_status.dec_backfilling(self.binding_index)

    async def execute_work_item(self, work_item: WorkItem, task: Task):
        subtasks: list[asyncio.Task] = []

        if work_item.has_backfill_work():
            backfill_task = task.spawn_child(
                f"backfill.{work_item.account_id}",
                lambda t: self._execute_backfill_subtask(work_item, t)
            )
            subtasks.append(backfill_task)

        incremental_task = task.spawn_child(
            f"incremental.{work_item.account_id}",
            lambda t: self._execute_incremental_subtask(work_item, t)
        )
        subtasks.append(incremental_task)

        await asyncio.gather(*subtasks)


class PriorityQueueManager:
    """
    Queue-based priority management system for connected accounts.

    Architecture:
    - Fixed pool of workers that process work items.
    - Priority queue containing work items for processing.
    - Periodic priority evaluation that updates the work queue.
    """
    def __init__(
        self, 
        all_account_ids: list[str], 
        resource_state: ResourceState,
        task: Task,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        fetch_changes_factory: FetchChangesFnFactory,
        fetch_page_factory: FetchPageFnFactory,
        prefix: str,
        config: Optional[PriorityQueueConfig] = None
    ):
        self.all_account_ids: list[str] = all_account_ids
        self.resource_state: ResourceState = resource_state
        self.config = config or DEFAULT_CONFIG
        self.running = False

        self.task = task
        self.binding = binding
        self.binding_index = binding_index
        self.fetch_changes_factory = fetch_changes_factory
        self.fetch_page_factory = fetch_page_factory
        self.prefix = prefix

        self.work_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=self.config.work_queue_size)
        self.workers: List[asyncio.Task] = []
        self.priority_evaluation_task: Optional[asyncio.Task] = None

        self.priority_calculator = PriorityCalculator(resource_state)
        self.work_item_manager = WorkItemManager(self.priority_calculator)
        self.subtask_executor = SubtaskExecutor(
            binding, binding_index, resource_state, prefix, task
        )

    def start_workers(self):
        self.running = True
        self._start_worker_pool()
        self._start_priority_evaluation()

    def _start_worker_pool(self):
        for worker_id in range(self.config.max_subtask_count):
            worker_task = self.task.spawn_child(
                f"{self.prefix}.worker.{worker_id}",
                lambda t, wid=worker_id: self._worker_loop(t, wid)
            )
            self.workers.append(worker_task)

    def _start_priority_evaluation(self):
        self.priority_evaluation_task = self.task.spawn_child(
            f"{self.prefix}.priority_evaluation",
            self._priority_evaluation_loop
        )
        asyncio.create_task(self._refresh_work_queue())

    async def _worker_loop(self, task: Task, worker_id: int):
        while self.running and not task.stopping.event.is_set():
            try:
                _, work_item = await asyncio.wait_for(
                    self.work_queue.get(),
                    timeout=1.0  # Short timeout to check stopping condition
                )

                self.work_item_manager.register_active_work(work_item)

                try:
                    await self.subtask_executor.execute_work_item(work_item, task)
                finally:
                    self.work_item_manager.unregister_active_work(work_item)
                    self.work_queue.task_done()

            except asyncio.TimeoutError:
                # The timeout to wait for work was reached. Check the stopping event again then wait for more work.
                continue
            except Exception as e:
                task.log.error(f"Worker {worker_id} error: {e}")
                raise

        task.log.debug(f"Worker {worker_id} shutting down")

    async def _priority_evaluation_loop(self, task: Task):
        while self.running and not task.stopping.event.is_set():
            try:
                await asyncio.wait_for(
                    task.stopping.event.wait(),
                    timeout=self.config.priority_refresh_interval.total_seconds()
                )

                break
            except asyncio.TimeoutError:
                # Time to re-evaluate priorities and update work queue.
                await self._refresh_work_queue()

    async def _refresh_work_queue(self):
        new_work_items, prioritized_account_ids = self.work_item_manager.generate_work_items(
            self.all_account_ids,
            self.config.max_subtask_count,
            self.fetch_changes_factory,
            self.fetch_page_factory,
            self.resource_state
        )

        current_account_ids = set(self.work_item_manager.active_work_by_account.keys())
        accounts_to_cancel = current_account_ids - prioritized_account_ids

        for account_id in accounts_to_cancel:
            self.work_item_manager.cancel_work_for_account(account_id)

        work_items_queued = 0
        for work_item in new_work_items:
            try:
                # Use priority as the sort key for PriorityQueue.
                # Lower priority values = higher priority in the queue.
                self.work_queue.put_nowait((work_item.priority, work_item))
                work_items_queued += 1
            except asyncio.QueueFull:
                # Work items should be processed quickly enough to avoid filling the queue.
                # A full queue indicates either that workers are taking too long to process items or the prioritization logic is not consistent.
                raise RuntimeError(
                    f"Work queue is full ({self.config.work_queue_size} items), indicating a processing bottleneck somewhere. "
                    " Please reach out to Estuary support for assistance debugging this error."
                )

        if work_items_queued > 0:
            self.task.log.info(
                f"Updated work queue",
                {
                    "prefix": self.prefix,
                    "accounts added": [w.account_id for w in new_work_items],
                    "accounts stopped": accounts_to_cancel,
                    "queue_size": self.work_queue.qsize(),
                }
            )


# Both _binding_incremental_task_with_work_item and _binding_backfill_task_with_work_item
# are adapted from estuary_cdk.capture.common._binding_incremental_task and
# estuary_cdk.capture.common._binding_backfill_task respectively, with modifications to support
# work item cancellation.
async def _binding_incremental_task_with_work_item(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    fetch_changes: FetchChangesFn[BaseDocument],
    state: ResourceState.Incremental,
    task: Task,
    work_item: WorkItem,
):
    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(inc={work_item.account_id: state})}
    )

    sleep_for = timedelta()

    task.log.info(
        "resuming incremental replication", {"state": state, "subtask_id": work_item.account_id}
    )

    if isinstance(state.cursor, datetime):
        lag = datetime.now(tz=UTC) - state.cursor

        if lag < binding.resourceConfig.interval:
            sleep_for = binding.resourceConfig.interval - lag
            task.log.info(
                "incremental task ran recently, sleeping until `interval` has fully elapsed",
                {
                    "sleep_for": sleep_for,
                    "interval": binding.resourceConfig.interval,
                    "subtask_id": work_item.account_id,
                },
            )

    while True:
        try:
            if task.stopping.event.is_set() or work_item.cancellation_event.is_set():
                task.log.debug(
                    "incremental replication is idle and is yielding to stop",
                    {"subtask_id": work_item.account_id},
                )
                return

            global_stop = asyncio.create_task(task.stopping.event.wait())
            work_cancelled = asyncio.create_task(work_item.cancellation_event.wait())
            
            done, pending = await asyncio.wait(
                [global_stop, work_cancelled],
                timeout=sleep_for.total_seconds(),
                return_when=asyncio.FIRST_COMPLETED
            )

            # Pending tasks must be cancelled _and_ cleaned up by awaiting them otherwise they'll
            # accumulate in the event loop's internal state and cause a slow memory leak.
            for p in pending:
                p.cancel()
                try:
                    await p
                except asyncio.CancelledError:
                    pass

            if done:
                task.log.debug(
                    "incremental replication is idle and is yielding to stop",
                    {"subtask_id": work_item.account_id},
                )
                return
        except asyncio.TimeoutError:
            pass  # `sleep_for` elapsed.

        checkpoints = 0
        pending = False

        async for item in fetch_changes(task.log, state.cursor):
            if isinstance(item, BaseDocument) or isinstance(item, dict):
                task.captured(binding_index, item)
                pending = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                pending = True
            elif item == Triggers.BACKFILL:
                task.log.info(
                    "incremental task triggered backfill", {"subtask_id": work_item.account_id}
                )
                task.stopping.event.set()
                await task.checkpoint(
                    ConnectorState(backfillRequests={binding.stateKey: True})
                )
                return
            else:
                # Ensure LogCursor types match and that they're strictly increasing.
                is_larger = False
                if isinstance(item, int) and isinstance(state.cursor, int):
                    is_larger = item > state.cursor
                elif isinstance(item, datetime) and isinstance(state.cursor, datetime):
                    is_larger = item > state.cursor
                elif (
                    isinstance(item, tuple)
                    and isinstance(state.cursor, tuple)
                    and isinstance(item[0], str)
                    and isinstance(state.cursor[0], str)
                ):
                    is_larger = item[0] > state.cursor[0]
                else:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} of a different type than the last LogCursor {state.cursor}",
                    )

                if not is_larger:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} which is not greater than the last LogCursor {state.cursor}",
                    )

                state.cursor = item
                await task.checkpoint(connector_state)
                checkpoints += 1
                pending = False

        if pending:
            raise RuntimeError(
                "Implementation error: FetchChangesFn yielded a documents without a final LogCursor",
            )

        if not checkpoints:
            # We're idle. Sleep for the full back-off interval.
            sleep_for = binding.resourceConfig.interval

        elif isinstance(state.cursor, datetime):
            lag = datetime.now(tz=UTC) - state.cursor

            if lag > binding.resourceConfig.interval:
                # We're not idle. Attempt to fetch the next changes.
                sleep_for = timedelta()
                continue
            else:
                # We're idle. Sleep until the cursor is `interval` old.
                sleep_for = binding.resourceConfig.interval - lag
        else:
            # We're not idle. Attempt to fetch the next changes.
            sleep_for = timedelta()
            continue

        task.log.debug(
            "incremental task is idle",
            {"sleep_for": sleep_for, "cursor": state.cursor, "subtask_id": work_item.account_id},
        )


async def _binding_backfill_task_with_work_item(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    fetch_page: Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]],
    state: ResourceState.Backfill,
    last_initialized: Optional[datetime],
    is_connector_initiated: bool,
    task: Task,
    work_item: WorkItem,
):
    connector_state = ConnectorState(
        bindingStateV1={
            binding.stateKey: ResourceState(backfill={work_item.account_id: state})
        }
    )

    if state.next_page is not None:
        task.log.info("resuming backfill", {"state": state, "subtask_id": work_item.account_id})
    else:
        task.log.info("beginning backfill", {"state": state, "subtask_id": work_item.account_id})

    while True:
        # Yield to the event loop to prevent starvation.
        await asyncio.sleep(0)

        # Check both global stopping and work item cancellation
        if task.stopping.event.is_set():
            task.log.debug("backfill is yielding to stop", {"subtask_id": work_item.account_id})
            return
        elif work_item.cancellation_event.is_set():
            task.log.info("backfill is yielding to work item cancellation", {"subtask_id": work_item.account_id})
            return

        # Track if fetch_page returns without having yielded a PageCursor.
        done = True

        # Distinguish between FetchPageFn and RecurringFetchPageFn to provide the correct arguments.
        if is_recurring_fetch_page_fn(fetch_page, task.log, state.next_page, state.cutoff, is_connector_initiated):
            fn = cast(RecurringFetchPageFn, fetch_page)
            pages = fn(task.log, state.next_page, state.cutoff, is_connector_initiated)
        else:
            fn = cast(FetchPageFn, fetch_page)
            pages = fn(task.log, state.next_page, state.cutoff)

        async for item in pages:
            if isinstance(item, BaseDocument) or isinstance(item, dict):
                task.captured(binding_index, item)
                done = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                done = True
            elif item is None:
                raise RuntimeError(
                    "Implementation error: FetchPageFn yielded PageCursor None. To represent end-of-sequence, yield documents and return without a final PageCursor."
                )
            else:
                state.next_page = item
                await task.checkpoint(connector_state)
                done = False

        if done:
            break

    await task.checkpoint(
        ConnectorState(
            bindingStateV1={
                binding.stateKey: ResourceState(backfill={work_item.account_id: None})
            }
        )
    )
    task.log.info("completed backfill", {"subtask_id": work_item.account_id})


# open_binding_with_priority_queue is a custom open_binding function that uses a queue-based worker system.
# It is based on estuary_cdk.capture.common.open_binding, but adapted to work with
# the QueueBasedPriorityManager and WorkItemManager classes defined above.
def open_binding_with_priority_queue(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_account_ids: list[str],
    fetch_changes_factory: FetchChangesFnFactory,
    fetch_page_factory: FetchPageFnFactory,
    fetch_snapshot: Optional[FetchSnapshotFn[BaseDocument]] = None,
    tombstone: Optional[BaseDocument] = None,
    config: Optional[PriorityQueueConfig] = None,
):
    task.connector_status.inc_binding_count()
    prefix = ".".join(binding.resourceConfig.path())

    priority_manager = PriorityQueueManager(
        all_account_ids=all_account_ids,
        resource_state=resource_state,
        task=task,
        binding=binding,
        binding_index=binding_index,
        fetch_changes_factory=fetch_changes_factory,
        fetch_page_factory=fetch_page_factory,
        prefix=prefix,
        config=config
    )

    priority_manager.start_workers()
