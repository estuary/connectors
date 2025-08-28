import asyncio
import time
from asyncio import Queue
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Optional

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import Events, IncrementalStream, LowerBoundOperator, UpperBoundOperator
from ..shared import dt_to_str, split_date_window, str_to_dt
from .common import (
    BASE_URL,
    SMALLEST_KLAVIYO_DATETIME_GRAIN,
    _construct_filter_param,
    _paginate_through_resources,
)


EVENTS_PAGE_SIZE = 200


@dataclass
class DateChunk:
    start: datetime
    end: datetime


@dataclass
class BackfillConfig:
    initial_chunks: int = 50
    num_event_workers: int = 5

    work_queue_size: int = 100
    subdivision_queue_size: int = 10

    # How many seconds to process a chunk before it is considered
    # a dense date window and should be divided into smaller chunks.
    dense_date_window_threshold: int = 30

    worker_timeout: float = 2.0
    subdivision_timeout: float = 1.0
    subdivision_request_timeout: float = 0.1


DEFAULT_BACKFILL_CONFIG = BackfillConfig()


async def _fetch_events_updated_between(
    http: HTTPSession,
    log: Logger,
    start: datetime,
    end: datetime,
) -> AsyncGenerator[IncrementalStream, None]:
    url = f"{BASE_URL}/{Events.path}"

    params: dict[str, str | int] = {
        "sort": Events.cursor_field,
        "filter": _construct_filter_param(
            cursor_field=Events.cursor_field,
            lower_bound_operator=LowerBoundOperator.GREATER_THAN,
            lower_bound=start,
            upper_bound_operator=UpperBoundOperator.LESS_OR_EQUAL,
            upper_bound=end,
            additional_filters=Events.additional_filters,
        )
    }

    if Events.extra_params:
        params.update(Events.extra_params)

    async for doc in _paginate_through_resources(
        http, url, params, Events, log,
    ):
        yield doc


class QueueManager:
    def __init__(self, work_queue_size: int, subdivision_queue_size: int):
        self.work_queue = Queue(maxsize=work_queue_size)
        self.subdivision_request_queue = Queue(maxsize=subdivision_queue_size)

    async def get_work_chunk(self, timeout: float = 2.0) -> Optional[DateChunk]:
        try:
            return await asyncio.wait_for(self.work_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def put_work_chunks(self, chunks: list[DateChunk]) -> None:
        for chunk in chunks:
            await self.work_queue.put(chunk)

    def put_subdivision_chunk(self, chunk: DateChunk) -> None:
        self.subdivision_request_queue.put_nowait(chunk)

    async def get_subdivision_chunk(self, timeout: float = 1.0) -> Optional[DateChunk]:
        try:
            return await asyncio.wait_for(self.subdivision_request_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def are_all_queues_empty(self) -> bool:
        return (
            self.work_queue.empty() and
            self.subdivision_request_queue.empty()
        )


class WorkManager:
    def __init__(
        self,
        http: HTTPSession,
        log: Logger,
    ):
        self.http = http
        self.log = log

        self.queue_manager = QueueManager(
            work_queue_size=DEFAULT_BACKFILL_CONFIG.work_queue_size,
            subdivision_queue_size=DEFAULT_BACKFILL_CONFIG.subdivision_queue_size,
        )
        self.shutdown_event = asyncio.Event()

        self.worker_tasks: list[asyncio.Task] = []
        self.subdivision_task: Optional[asyncio.Task] = None
        self.shutdown_monitor_task: Optional[asyncio.Task] = None

        self._active_worker_count = 0

    def mark_worker_active(self) -> None:
        if self._active_worker_count >= DEFAULT_BACKFILL_CONFIG.num_event_workers:
            raise Exception(f"A worker attempted to mark itself active when the active worker count is {self._active_worker_count}.")
        self._active_worker_count += 1

    def mark_worker_inactive(self) -> None:
        if self._active_worker_count <= 0:
            raise Exception(f"A worker attempted to mark itself inactive when the active worker count is {self._active_worker_count}.")
        self._active_worker_count -= 1

    def are_active_workers(self) -> bool:
        return self._active_worker_count > 0

    async def fetch_events_between(
        self,
        start: datetime,
        end: datetime,
        external_queue: asyncio.Queue,
    ) -> None:
        initial_chunks = self._create_initial_chunks(start, end)
        await self.queue_manager.put_work_chunks(initial_chunks)

        async with asyncio.TaskGroup() as tg:
            self.worker_tasks = [
                tg.create_task(
                    chunk_worker(
                        worker_id=i,
                        queue_manager=self.queue_manager,
                        work_manager=self,
                        http=self.http,
                        log=self.log,
                        shutdown_event=self.shutdown_event,
                        document_queue=external_queue,
                    ),
                    name=f"chunk_worker_{i + 1}"
                )
                for i in range(DEFAULT_BACKFILL_CONFIG.num_event_workers)
            ]

            self.subdivision_task = tg.create_task(
                subdivision_worker(
                    queue_manager=self.queue_manager,
                    log=self.log,
                    shutdown_event=self.shutdown_event,
                ),
                name="subdivision_worker"
            )

            self.shutdown_monitor_task = tg.create_task(
                self._shutdown_monitor(),
                name="shutdown_monitor"
            )

    def _create_initial_chunks(self, start: datetime, end: datetime) -> list[DateChunk]:
        return [
            DateChunk(start=chunk_start, end=chunk_end)
            for chunk_start, chunk_end in split_date_window(start, end, DEFAULT_BACKFILL_CONFIG.initial_chunks)
        ]

    async def _shutdown_monitor(self) -> None:
        while not self.shutdown_event.is_set():
            if not self.are_active_workers() and self.queue_manager.are_all_queues_empty():
                self.log.debug("All work complete - shutdown monitor initiating shutdown")
                self.shutdown_event.set()
                break

            await asyncio.sleep(1)


# chunk_worker pulls a date chunk from the queue and fetches events created in
# that chunk. If fetching that chunk takes too long and other workers have capacity
# to help, the remaining unprocessed date range is submitted to the subdivision worker
# to split into smaller chunks.
async def chunk_worker(
    worker_id: int,
    queue_manager: QueueManager,
    work_manager: WorkManager,
    http: HTTPSession,
    log: Logger,
    shutdown_event: asyncio.Event,
    document_queue: asyncio.Queue,
) -> None:
    log.debug(f"Event worker {worker_id} started.")

    is_looking_for_work = False
    while not shutdown_event.is_set():
        chunk = await queue_manager.get_work_chunk(timeout=DEFAULT_BACKFILL_CONFIG.worker_timeout)
        if chunk is None:
            if not is_looking_for_work:
                log.debug(f"Event worker {worker_id} is idle and checking for new work.")
                is_looking_for_work = True
            # No work is available. Check the shutdown_event and then check for more work.
            continue

        log.debug(f"Event worker {worker_id} is working on {chunk.start} to {chunk.end}")
        is_looking_for_work = False

        work_manager.mark_worker_active()

        start_time = time.time()
        count = 0
        elapsed: float  = 0
        last_cursor = chunk.start
        is_divisible = chunk.end - chunk.start >= (SMALLEST_KLAVIYO_DATETIME_GRAIN * 3)
        is_dense_date_window = False

        events_gen = _fetch_events_updated_between(http, log, chunk.start, chunk.end)

        async for event in events_gen:
            event_cursor = event.get_cursor_value()

            # If this date window is dense (i.e. has a lot of events in it) and there's an idle worker,
            # divide the rest of the current date window up into smaller chunks so the idle worker
            # can help process it faster.
            if (
                is_dense_date_window and
                event_cursor > last_cursor and
                work_manager._active_worker_count < DEFAULT_BACKFILL_CONFIG.num_event_workers
            ):
                subdivision_chunk = DateChunk(
                    start=last_cursor,
                    end=chunk.end
                )
                queue_manager.put_subdivision_chunk(subdivision_chunk)
                break

            last_cursor = event_cursor
            count += 1
            await document_queue.put(event)

            # Check if this is a dense date window that should be divided into smaller chunks.
            if (
                is_divisible and
                not is_dense_date_window and
                # Check every EVENTS_PAGE_SIZE events to avoid checking too frequently.
                # It's fine for is_dense_date_window to be set after the worker's been
                # processing for more than dense_date_window_threshold seconds; the
                # current date range won't be divided into smaller chunks until another
                # worker is idle anyway.
                count % EVENTS_PAGE_SIZE == 0
            ):
                elapsed = time.time() - start_time
                if elapsed > DEFAULT_BACKFILL_CONFIG.dense_date_window_threshold:
                    is_dense_date_window = True

        work_manager.mark_worker_inactive()

    log.debug(f"Event worker {worker_id} exited.")


# subdivision_worker handles splitting date windows with
# large amounts of data into smaller chunks.
async def subdivision_worker(
    queue_manager: QueueManager,
    log: Logger,
    shutdown_event: asyncio.Event,
) -> None:
    log.debug("Subdivision worker started.")
    while not shutdown_event.is_set():
        chunk = await queue_manager.get_subdivision_chunk()
        if chunk is None:
            continue

        log.debug(f"Subdivision worker is splitting chunk from {chunk.start} to {chunk.end} into smaller chunks.")

        divided_date_ranges = split_date_window(
            start=chunk.start,
            end=chunk.end,
            num_chunks=DEFAULT_BACKFILL_CONFIG.num_event_workers,
            minimum_chunk_size=SMALLEST_KLAVIYO_DATETIME_GRAIN
        )
        divided_chunks = [
            DateChunk(start, end)
            for (start, end)
            in divided_date_ranges
        ]

        await queue_manager.put_work_chunks(divided_chunks)

    log.debug("Subdivision worker exited.")


async def backfill_events(
    http: HTTPSession,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalStream | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(page, str)

    start = str_to_dt(page)
    if start >= cutoff:
        return

    end = min(start + window_size, cutoff)

    queue = asyncio.Queue(maxsize=1)

    work_manager = WorkManager(
        http=http,
        log=log,
    )

    async def populate_queue() -> None:
        try:
            await work_manager.fetch_events_between(start, end, queue)
            # All tasks completed successfully. Signal completion with a None sentinel value.
            await queue.put(None)
        except* Exception as eg:
            first_exception = eg.exceptions[0]
            log.error(f"Populating queue failed: {first_exception}")
            await queue.put(first_exception)

    # Start populating the queue in the background.
    task_runner = asyncio.create_task(populate_queue())

    try:
        while True:
            item = await queue.get()

            if isinstance(item, IncrementalStream):
                yield item
            elif item is None:
                # Sentinel. All work completed successfully.
                break
            elif isinstance(item, Exception):
                raise item
            else:
                raise RuntimeError(f"Unexpected {type(item)} item: {item}")
    finally:
        # Clean up the background task.
        if not task_runner.done():
            task_runner.cancel()
        await asyncio.gather(task_runner, return_exceptions=True)

    if end >= cutoff:
        return
    else:
        log.debug(f"Finished fetching events between {start} and {end}. Yielding cursor {end}.")
        yield dt_to_str(end)
