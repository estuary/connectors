"""
Tests for process_completed_fetches, which fans out page fetches and yields
their resources as each fetch completes.
"""

import asyncio
import gc
import weakref
from typing import Any

import pytest

from source_braintree_native.api.common import process_completed_fetches


class Page(list):
    """A fetch result that the tests can hold a weak reference to."""

    __slots__ = ("__weakref__",)


class TestProcessCompletedFetches:
    @pytest.mark.asyncio
    async def test_releases_each_page_once_it_has_been_drained(self):
        """Pages must not stay resident for the whole fan-out.

        A completed asyncio.Task holds its return value, so retaining the tasks kept every
        page a backfill window fetched resident at once. Each fetch blocks on its own event
        so exactly one page is released at a time, making the resident count deterministic.
        """
        page_count = 8
        page_size = 5
        events = [asyncio.Event() for _ in range(page_count)]
        pages: list[weakref.ref[Page]] = []

        async def fetch(page: int) -> list[dict[str, Any]]:
            await events[page].wait()
            result = Page({"id": f"{page}-{i}"} for i in range(page_size))
            pages.append(weakref.ref(result))
            return result

        generator = process_completed_fetches([fetch(page) for page in range(page_count)])
        consumed: list[dict[str, Any]] = []

        for page in range(page_count):
            events[page].set()

            for _ in range(page_size):
                consumed.append(await generator.__anext__())

            gc.collect()
            resident = sum(1 for ref in pages if ref() is not None)

            # Only the page currently being drained should still be held.
            assert resident == 1, (
                f"after draining page {page}, {resident} of {page + 1} fetched pages "
                "are still resident"
            )

        with pytest.raises(StopAsyncIteration):
            await generator.__anext__()

        assert consumed == [
            {"id": f"{page}-{i}"}
            for page in range(page_count)
            for i in range(page_size)
        ]

    @pytest.mark.asyncio
    async def test_drains_every_page_reported_in_a_single_batch(self):
        """asyncio.wait reports every fetch that has already finished, not just one.

        A batch of responses that are readable in the same iteration of the event loop
        puts several fetches in one `done` set, which is the common case at the default
        concurrency of 20. None of them may be dropped.
        """
        page_count = 12
        page_size = 3

        async def fetch(page: int) -> list[dict[str, Any]]:
            return Page({"id": f"{page}-{i}"} for i in range(page_size))

        consumed = [
            resource
            async for resource in process_completed_fetches(
                [fetch(page) for page in range(page_count)]
            )
        ]

        assert sorted(resource["id"] for resource in consumed) == sorted(
            f"{page}-{i}" for page in range(page_count) for i in range(page_size)
        )

    @pytest.mark.asyncio
    async def test_retrieves_exceptions_from_fetches_that_were_never_drained(self):
        """Fetches that failed but were never drained must have their exceptions retrieved.

        Every fetch here fails in the same iteration of the event loop, so asyncio.wait
        reports them together and all but the first are still undrained when the error
        propagates. Leaving those exceptions unretrieved makes asyncio report "Task
        exception was never retrieved", which is the shutdown noise the cancellation in
        this helper exists to avoid.
        """
        unhandled: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: unhandled.append(context))

        async def failing_fetch(page: int) -> list[dict[str, Any]]:
            raise RuntimeError(f"fetch {page} failed")

        try:
            raised = False
            # Caught without binding the exception, so its traceback stops referencing the
            # generator's frame once this block exits and the undrained fetches can be
            # collected. pytest.raises would keep that frame - and them - alive.
            try:
                async for _ in process_completed_fetches(
                    [failing_fetch(page) for page in range(6)]
                ):
                    pass
            except RuntimeError:
                raised = True

            assert raised, "the failing fetch should have propagated"

            # Unretrieved exceptions are reported when the task is collected.
            for _ in range(3):
                gc.collect()
                await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(None)

        assert unhandled == [], f"unretrieved task exceptions: {unhandled}"

    @pytest.mark.asyncio
    async def test_cancels_outstanding_fetches_when_consumer_exits_early(self):
        cancelled: list[int] = []

        async def fetch(page: int) -> list[dict[str, Any]]:
            if page == 0:
                return Page([{"id": "0-0"}])

            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(page)
                raise

            return Page()

        generator = process_completed_fetches([fetch(page) for page in range(3)])

        async for _ in generator:
            break

        await generator.aclose()

        assert sorted(cancelled) == [1, 2]

    @pytest.mark.asyncio
    async def test_propagates_fetch_errors_and_cancels_siblings(self):
        cancelled: list[int] = []

        async def failing_fetch() -> list[dict[str, Any]]:
            raise RuntimeError("fetch failed")

        async def fetch(page: int) -> list[dict[str, Any]]:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(page)
                raise

            return Page()

        with pytest.raises(RuntimeError, match="fetch failed"):
            async for _ in process_completed_fetches(
                [failing_fetch(), fetch(1), fetch(2)]
            ):
                pass

        assert sorted(cancelled) == [1, 2]

    @pytest.mark.asyncio
    async def test_yields_nothing_when_there_are_no_fetches(self):
        assert [resource async for resource in process_completed_fetches([])] == []
