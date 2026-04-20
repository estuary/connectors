import asyncio
import time
import traceback
from asyncio import CancelledError
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import Logger

from estuary_cdk.http import HTTPSession
from estuary_cdk.utils import format_error_message

from .api import API
from .models import Accounts, ListResult


# Stripe launched in 2011. No connected accounts can exist before this date.
STRIPE_EPOCH = int(datetime(2011, 2, 1, tzinfo=UTC).timestamp())


@dataclass
class TimestampChunk:
    start: int
    end: int


@dataclass
class AccountFetchConfig:
    initial_chunks: int = 50
    num_workers: int = 10

    # How many seconds a worker processes a chunk before it is considered
    # a dense time window and should be divided into smaller chunks.
    dense_chunk_threshold_seconds: int = 30

    # Minimum chunk size in seconds (1 minute) to prevent over-subdivision.
    minimum_chunk_seconds: int = 60

    work_queue_size: int = 100

    page_limit: int = 100


DEFAULT_CONFIG = AccountFetchConfig()


def split_timestamp_range(
    start: int,
    end: int,
    num_chunks: int,
    minimum_chunk_seconds: int,
) -> list[TimestampChunk]:
    """Split a unix timestamp range into roughly equal-sized chunks.

    If minimum_chunk_seconds would be violated, fewer chunks are produced.
    Returns a list of TimestampChunks covering the full range.
    """
    if num_chunks <= 0:
        raise ValueError("num_chunks must be positive")

    if start >= end:
        raise ValueError("start must be before end")

    total_duration = end - start
    num_chunks = max(
        1,
        min(
            num_chunks,
            total_duration // minimum_chunk_seconds,
        ),
    )
    chunk_duration = total_duration / num_chunks

    chunks: list[TimestampChunk] = []
    current_start = start

    for i in range(num_chunks):
        if i == num_chunks - 1:
            chunk_end = end
        else:
            chunk_end = int(current_start + chunk_duration)

        chunks.append(TimestampChunk(start=current_start, end=chunk_end))
        current_start = chunk_end

    return chunks


class AccountWorkManager:
    def __init__(
        self,
        http: HTTPSession,
        log: Logger,
        config: AccountFetchConfig = DEFAULT_CONFIG,
    ):
        self.http = http
        self.log = log
        self.config = config

        self.work_queue: asyncio.Queue[TimestampChunk | None] = asyncio.Queue(maxsize=config.work_queue_size)

        self._active_worker_count = 0
        self.first_worker_error: str | None = None

        self.account_ids: set[str] = set()

    def mark_worker_active(self) -> None:
        if self._active_worker_count >= self.config.num_workers:
            raise Exception(f"A worker attempted to mark itself active when the active worker count is {self._active_worker_count}.")
        self._active_worker_count += 1

    def mark_worker_inactive(self) -> None:
        if self._active_worker_count <= 0:
            raise Exception(f"A worker attempted to mark itself inactive when the active worker count is {self._active_worker_count}.")
        self._active_worker_count -= 1

    def are_active_workers(self) -> bool:
        return self._active_worker_count > 0

    def has_idle_workers(self) -> bool:
        return self._active_worker_count < self.config.num_workers

    async def fetch_account_ids(self, start: int, end: int) -> set[str]:
        initial_chunks = self._create_initial_chunks(start, end)
        for chunk in initial_chunks:
            self.work_queue.put_nowait(chunk)

        # Purely diagnostic: logs task outcomes to aid debugging when a task
        # fails or is cancelled unexpectedly. Not used for control flow —
        # the TaskGroup handles exception propagation and cancellation.
        def callback(task: asyncio.Task):
            task_name = task.get_name()
            status: str = ""
            stack_trace: str | None = None

            if task.cancelled():
                status = "cancelled"
            elif exc := task.exception():
                status = f"failed with exception {format_error_message(exc)}"
                if exc.__traceback__:
                    stack_trace = "\nStack trace:\n" + "".join(traceback.format_list(traceback.extract_tb(exc.__traceback__)))
            else:
                status = "completed"

            self.log.debug(f"Task {task_name} {status}.", {
                "first_worker_error": self.first_worker_error,
                "active_worker_count": self._active_worker_count,
                "stack_trace": stack_trace,
            })

        self.log.debug("Starting concurrent account fetch workers.")
        async with asyncio.TaskGroup() as tg:
            for i in range(self.config.num_workers):
                worker_id = i + 1
                task = tg.create_task(
                    account_chunk_worker(
                        worker_id=worker_id,
                        work_queue=self.work_queue,
                        work_manager=self,
                        http=self.http,
                        log=self.log,
                        config=self.config,
                    ),
                    name=f"account_chunk_worker_{worker_id}"
                )
                task.add_done_callback(callback)

            task = tg.create_task(
                self._shutdown_coordinator(),
                name="account_shutdown_coordinator"
            )
            task.add_done_callback(callback)

        self.log.debug(f"Concurrent account fetch complete. Found {len(self.account_ids)} account IDs.")
        return self.account_ids

    def _create_initial_chunks(self, start: int, end: int) -> list[TimestampChunk]:
        return split_timestamp_range(
            start=start,
            end=end,
            num_chunks=self.config.initial_chunks,
            minimum_chunk_seconds=self.config.minimum_chunk_seconds,
        )

    async def _shutdown_coordinator(self) -> None:
        """Wait for all work items to be processed, then signal workers to exit."""
        await self.work_queue.join()
        for _ in range(self.config.num_workers):
            self.work_queue.put_nowait(None)


async def account_chunk_worker(
    worker_id: int,
    work_queue: asyncio.Queue[TimestampChunk | None],
    work_manager: AccountWorkManager,
    http: HTTPSession,
    log: Logger,
    config: AccountFetchConfig,
) -> None:
    try:
        log.debug(f"Account worker {worker_id} started.")

        while True:
            chunk = await work_queue.get()
            if chunk is None:
                work_queue.task_done()
                break

            log.debug(f"Account worker {worker_id} working on chunk [{chunk.start}, {chunk.end}]")

            work_manager.mark_worker_active()

            try:
                url = f"{API}/accounts"
                params: dict[str, str | int] = {
                    "limit": config.page_limit,
                    "created[gte]": chunk.start,
                    "created[lte]": chunk.end,
                }
                start_time = time.time()
                page_count = 0
                last_created: int | None = None
                # Only consider subdividing if the chunk is large enough that the resulting
                # sub-chunks would each still be meaningfully sized (above minimum_chunk_seconds).
                is_divisible = (chunk.end - chunk.start) >= (config.minimum_chunk_seconds * 3)
                is_dense_chunk = False

                while True:
                    response = ListResult[Accounts].model_validate_json(
                        await http.request(log, url, params=params)
                    )

                    for account in response.data:
                        work_manager.account_ids.add(account.id)
                        last_created = account.created

                    page_count += 1

                    if not response.has_more:
                        break

                    # Stripe returns results in descending created order. After paginating
                    # partway through chunk [S, E], the worker has processed accounts from E
                    # down to last_created. The unprocessed range is [S, last_created].
                    if (
                        is_dense_chunk
                        and last_created is not None
                        and last_created > chunk.start
                        and work_manager.has_idle_workers()
                    ):
                        sub_chunks = split_timestamp_range(
                            start=chunk.start,
                            end=last_created,
                            num_chunks=config.num_workers,
                            minimum_chunk_seconds=config.minimum_chunk_seconds,
                        )
                        for sub_chunk in sub_chunks:
                            work_queue.put_nowait(sub_chunk)
                        break

                    # Check if this chunk is dense after each page.
                    if (
                        is_divisible
                        and not is_dense_chunk
                    ):
                        elapsed = time.time() - start_time
                        if elapsed > config.dense_chunk_threshold_seconds:
                            is_dense_chunk = True

                    params["starting_after"] = response.data[-1].id

                log.debug(f"Account worker {worker_id} finished chunk. Pages: {page_count}, dense: {is_dense_chunk}")

                # task_done() is called after any sub-chunks are enqueued to prevent
                # join() from returning before the sub-chunks are processed.
                work_queue.task_done()
            finally:
                work_manager.mark_worker_inactive()

        log.debug(f"Account worker {worker_id} exited.")
    except CancelledError as e:
        if not work_manager.first_worker_error:
            msg = format_error_message(e)
            work_manager.first_worker_error = msg
            raise Exception(f"Account worker {worker_id} was unexpectedly cancelled: {msg}")
        else:
            raise e
    except BaseException as e:
        msg = format_error_message(e)
        if not work_manager.first_worker_error:
            work_manager.first_worker_error = msg

        log.error(f"Account worker {worker_id} encountered an error.", {
            "exception": msg,
        })
        raise e


async def fetch_connected_account_ids(
    http: HTTPSession,
    log: Logger,
    config: AccountFetchConfig = DEFAULT_CONFIG,
) -> list[str]:
    """Fetch all connected account IDs using concurrent workers.

    Uses time-range partitioning with created[gte]/created[lte] to parallelize
    paginating through the accounts list endpoint.
    """
    start = STRIPE_EPOCH
    end = int(time.time())

    work_manager = AccountWorkManager(http=http, log=log, config=config)
    await work_manager.fetch_account_ids(start, end)

    return list(work_manager.account_ids)
