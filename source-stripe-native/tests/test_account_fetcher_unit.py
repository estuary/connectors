import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from source_stripe_native.account_fetcher import (
    AccountFetchConfig,
    AccountWorkManager,
    TimestampChunk,
    account_chunk_worker,
    fetch_connected_account_ids,
    split_timestamp_range,
)


class TestSplitTimestampRange:
    def test_basic_split(self):
        chunks = split_timestamp_range(0, 100, num_chunks=5, minimum_chunk_seconds=1)
        assert len(chunks) == 5
        # First chunk starts at 0, last chunk ends at 100
        assert chunks[0].start == 0
        assert chunks[-1].end == 100
        # Chunks are contiguous
        for i in range(len(chunks) - 1):
            assert chunks[i].end == chunks[i + 1].start

    def test_single_chunk(self):
        chunks = split_timestamp_range(0, 100, num_chunks=1, minimum_chunk_seconds=1)
        assert len(chunks) == 1
        assert chunks[0] == TimestampChunk(0, 100)

    def test_minimum_chunk_size_reduces_count(self):
        # 100 seconds total, 50 chunks requested, but minimum is 25 seconds
        # Should produce 4 chunks (100 / 25 = 4)
        chunks = split_timestamp_range(0, 100, num_chunks=50, minimum_chunk_seconds=25)
        assert len(chunks) == 4
        assert chunks[0].start == 0
        assert chunks[-1].end == 100

    def test_minimum_chunk_size_larger_than_range(self):
        # Range is 50 seconds but minimum chunk is 100 seconds -> 1 chunk
        chunks = split_timestamp_range(0, 50, num_chunks=10, minimum_chunk_seconds=100)
        assert len(chunks) == 1
        assert chunks[0] == TimestampChunk(0, 50)

    def test_invalid_num_chunks(self):
        with pytest.raises(ValueError, match="num_chunks must be positive"):
            split_timestamp_range(0, 100, num_chunks=0, minimum_chunk_seconds=1)

    def test_invalid_range(self):
        with pytest.raises(ValueError, match="start must be before end"):
            split_timestamp_range(100, 50, num_chunks=5, minimum_chunk_seconds=1)

    def test_equal_start_end(self):
        with pytest.raises(ValueError, match="start must be before end"):
            split_timestamp_range(100, 100, num_chunks=5, minimum_chunk_seconds=1)

    def test_large_range(self):
        # ~15 years in seconds (similar to STRIPE_EPOCH -> now)
        start = 1296518400
        end = start + (15 * 365 * 24 * 3600)
        chunks = split_timestamp_range(start, end, num_chunks=50, minimum_chunk_seconds=60)
        assert len(chunks) == 50
        assert chunks[0].start == start
        assert chunks[-1].end == end
        for i in range(len(chunks) - 1):
            assert chunks[i].end == chunks[i + 1].start


class TestAccountWorkManager:
    def test_mark_worker_active_inactive(self):
        wm = AccountWorkManager(http=MagicMock(), log=MagicMock())
        assert not wm.are_active_workers()

        wm.mark_worker_active()
        assert wm.are_active_workers()

        wm.mark_worker_inactive()
        assert not wm.are_active_workers()

    def test_mark_worker_active_overflow(self):
        config = AccountFetchConfig(num_workers=1)
        wm = AccountWorkManager(http=MagicMock(), log=MagicMock(), config=config)
        wm.mark_worker_active()
        with pytest.raises(Exception, match="active worker count"):
            wm.mark_worker_active()

    def test_mark_worker_inactive_underflow(self):
        wm = AccountWorkManager(http=MagicMock(), log=MagicMock())
        with pytest.raises(Exception, match="active worker count"):
            wm.mark_worker_inactive()


def _make_accounts_response(account_ids: list[str], created_timestamps: list[int], has_more: bool = False) -> bytes:
    """Build a JSON response matching ListResult[Accounts] shape."""
    data = []
    for aid, created in zip(account_ids, created_timestamps):
        data.append({
            "id": aid,
            "object": "account",
            "created": created,
        })
    return json.dumps({
        "object": "list",
        "url": "/v1/accounts",
        "has_more": has_more,
        "data": data,
    }).encode()


class TestFetchConnectedAccountIdsConcurrent:
    @pytest.mark.asyncio
    async def test_concurrent_fetch_collects_all_ids(self):
        """Workers should find all accounts across time-range chunks."""
        http = MagicMock()
        log = MagicMock()

        # Generate accounts across different time ranges
        all_accounts: dict[str, int] = {}
        for i in range(250):
            aid = f"acct_{i:04d}"
            created = 1296518400 + (i * 100000)
            all_accounts[aid] = created

        async def mock_request(_log, url, params: dict[str, str | int]):
            gte = int(params["created[gte]"])
            lte = int(params["created[lte]"])
            starting_after = params.get("starting_after")

            matching = [
                (a, c) for a, c in all_accounts.items()
                if gte <= c <= lte
            ]
            matching.sort(key=lambda x: x[1], reverse=True)

            if starting_after:
                idx = next(
                    (j for j, (a, _) in enumerate(matching) if a == starting_after),
                    None,
                )
                if idx is not None:
                    matching = matching[idx + 1:]

            page_size = int(params.get("limit", 100))
            page = matching[:page_size]
            has_more = len(matching) > page_size

            return _make_accounts_response(
                [a for a, _ in page],
                [c for _, c in page],
                has_more=has_more,
            )

        http.request = mock_request

        config = AccountFetchConfig(
            initial_chunks=10,
            num_workers=3,
            minimum_chunk_seconds=1,
        )
        result = await fetch_connected_account_ids(http, log, config=config)

        assert set(result) == set(all_accounts.keys())

    @pytest.mark.asyncio
    async def test_deduplication(self):
        """Accounts appearing in multiple chunks (edge boundary) are deduplicated."""
        http = MagicMock()
        log = MagicMock()

        async def mock_request(_log, url, params: dict[str, str | int]):
            gte = int(params["created[gte]"])

            return _make_accounts_response(
                ["acct_boundary", f"acct_unique_{gte}"],
                [1500000000, 1500000000 + gte],
                has_more=False,
            )

        http.request = mock_request

        config = AccountFetchConfig(
            initial_chunks=5,
            num_workers=2,
            minimum_chunk_seconds=1,
        )
        result = await fetch_connected_account_ids(http, log, config=config)

        # acct_boundary should appear exactly once despite being in multiple chunks
        assert result.count("acct_boundary") == 1


class TestAccountChunkWorker:
    @pytest.mark.asyncio
    async def test_worker_processes_chunk_and_collects_ids(self):
        """A single worker should paginate through its chunk and collect IDs."""
        http = MagicMock()
        log = MagicMock()
        config = AccountFetchConfig(num_workers=1)
        work_manager = AccountWorkManager(http=http, log=log, config=config)
        work_queue = work_manager.work_queue

        response = _make_accounts_response(
            ["acct_1", "acct_2"],
            [1700000200, 1700000100],
            has_more=False,
        )
        http.request = AsyncMock(return_value=response)

        work_queue.put_nowait(TimestampChunk(1700000000, 1700000300))
        work_queue.put_nowait(None)  # Sentinel to shut down the worker.

        await account_chunk_worker(
            worker_id=1,
            work_queue=work_queue,
            work_manager=work_manager,
            http=http,
            log=log,
            config=config,
        )

        assert work_manager.account_ids == {"acct_1", "acct_2"}

    @pytest.mark.asyncio
    async def test_worker_paginates_multiple_pages(self):
        """Worker should follow pagination within its time chunk."""
        http = MagicMock()
        log = MagicMock()
        config = AccountFetchConfig(num_workers=1, page_limit=2)
        work_manager = AccountWorkManager(http=http, log=log, config=config)
        work_queue = work_manager.work_queue

        page1 = _make_accounts_response(
            ["acct_1", "acct_2"],
            [1700000300, 1700000200],
            has_more=True,
        )
        page2 = _make_accounts_response(
            ["acct_3"],
            [1700000100],
            has_more=False,
        )
        http.request = AsyncMock(side_effect=[page1, page2])

        work_queue.put_nowait(TimestampChunk(1700000000, 1700000400))
        work_queue.put_nowait(None)  # Sentinel to shut down the worker.

        await account_chunk_worker(
            worker_id=1,
            work_queue=work_queue,
            work_manager=work_manager,
            http=http,
            log=log,
            config=config,
        )

        assert work_manager.account_ids == {"acct_1", "acct_2", "acct_3"}
        assert http.request.call_count == 2


class TestDenseChunkDetection:
    @pytest.mark.asyncio
    async def test_dense_chunk_triggers_subdivision(self):
        """When a chunk takes too long and idle workers exist, subdivision should be triggered.

        The worker flow is:
          Page 1: fetch -> density check sets is_dense_chunk=True -> continue
          Page 2: fetch -> has_more=True -> subdivision check triggers -> break

        So we need at least 2 pages with has_more=True for subdivision to fire.
        """
        http = MagicMock()
        log = MagicMock()
        config = AccountFetchConfig(
            num_workers=2,
            dense_chunk_threshold_seconds=0,  # Immediately mark as dense
            minimum_chunk_seconds=1,
        )
        work_manager = AccountWorkManager(http=http, log=log, config=config)
        work_queue = work_manager.work_queue

        # Page 1: has_more=True, triggers density detection
        page1 = _make_accounts_response(
            ["acct_1", "acct_2"],
            [1700000200, 1700000150],
            has_more=True,
        )
        # Page 2: has_more=True, now is_dense_chunk is True so subdivision triggers
        page2 = _make_accounts_response(
            ["acct_3", "acct_4"],
            [1700000100, 1700000050],
            has_more=True,
        )
        # Page 3 should not be reached because worker breaks after subdivision
        page3 = _make_accounts_response(
            ["acct_5"],
            [1700000025],
            has_more=False,
        )
        http.request = AsyncMock(side_effect=[page1, page2, page3])

        chunk = TimestampChunk(start=1700000000, end=1700000300)
        work_queue.put_nowait(chunk)
        work_queue.put_nowait(None)  # Sentinel to shut down the worker.

        await account_chunk_worker(
            worker_id=1,
            work_queue=work_queue,
            work_manager=work_manager,
            http=http,
            log=log,
            config=config,
        )

        # Worker should have collected accounts from pages 1 and 2
        assert work_manager.account_ids >= {"acct_1", "acct_2", "acct_3", "acct_4"}
        # Page 3 should not have been fetched
        assert "acct_5" not in work_manager.account_ids

        # The remaining range [chunk.start, last_created] should have been split into
        # sub-chunks on the work queue. last_created after page 2 is 1700000050.
        # With num_workers=2, we get 2 sub-chunks plus the sentinel we already consumed.
        sub_chunks = []
        while not work_queue.empty():
            item = work_queue.get_nowait()
            if item is not None:
                sub_chunks.append(item)

        assert len(sub_chunks) == 2
        assert sub_chunks[0].start == chunk.start
        assert sub_chunks[-1].end == 1700000050
        for i in range(len(sub_chunks) - 1):
            assert sub_chunks[i].end == sub_chunks[i + 1].start

        # Only two API calls: page 1 (density detected) and page 2 (subdivision triggered)
        assert http.request.call_count == 2
