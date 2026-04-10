from datetime import datetime, UTC
from unittest.mock import MagicMock

import pytest

from estuary_cdk.capture.common import ResourceState

from source_stripe_native.priority_capture import (
    PriorityLevel,
    WorkItemPriority,
    PriorityCalculator,
    PriorityQueueConfig,
    PriorityQueueManager,
    WorkItemManager,
)


def _make_resource_state(account_ids, backfill_ids=None):
    """Create a ResourceState with given accounts. Accounts in backfill_ids get active backfill state."""
    if backfill_ids is None:
        backfill_ids = set()

    inc = {}
    backfill = {}
    for aid in account_ids:
        inc[aid] = ResourceState.Incremental(cursor=datetime(2025, 1, 1, tzinfo=UTC))
        if aid in backfill_ids:
            backfill[aid] = ResourceState.Backfill(cutoff=datetime(2025, 1, 1, tzinfo=UTC), next_page=None)
        else:
            backfill[aid] = None

    return ResourceState(inc=inc, backfill=backfill)


def _make_factories():
    """Create mock fetch factories."""
    fetch_changes_factory = MagicMock(side_effect=lambda aid: MagicMock(name=f"fetch_changes_{aid}"))
    fetch_page_factory = MagicMock(side_effect=lambda aid: MagicMock(name=f"fetch_page_{aid}"))
    return fetch_changes_factory, fetch_page_factory


class TestWorkItemPriority:
    def test_backfill_higher_priority_than_incremental(self):
        backfill = WorkItemPriority(level=PriorityLevel.BACKFILL, cursor_timestamp=None, account_id="acct_1")
        incremental = WorkItemPriority(level=PriorityLevel.INCREMENTAL, cursor_timestamp=datetime(2025, 6, 1, tzinfo=UTC), account_id="acct_2")
        assert backfill < incremental
        assert not (incremental < backfill)

    def test_older_incremental_cursor_higher_priority(self):
        older = WorkItemPriority(level=PriorityLevel.INCREMENTAL, cursor_timestamp=datetime(2025, 1, 1, tzinfo=UTC), account_id="acct_1")
        newer = WorkItemPriority(level=PriorityLevel.INCREMENTAL, cursor_timestamp=datetime(2025, 6, 1, tzinfo=UTC), account_id="acct_2")
        assert older < newer
        assert not (newer < older)

    def test_same_timestamp_ordered_by_account_id(self):
        ts = datetime(2025, 1, 1, tzinfo=UTC)
        a = WorkItemPriority(level=PriorityLevel.INCREMENTAL, cursor_timestamp=ts, account_id="acct_a")
        b = WorkItemPriority(level=PriorityLevel.INCREMENTAL, cursor_timestamp=ts, account_id="acct_b")
        assert a < b
        assert not (b < a)

    def test_both_backfills_ordered_by_account_id(self):
        a = WorkItemPriority(level=PriorityLevel.BACKFILL, cursor_timestamp=None, account_id="acct_a")
        b = WorkItemPriority(level=PriorityLevel.BACKFILL, cursor_timestamp=None, account_id="acct_b")
        assert a < b
        assert not (b < a)


class TestPriorityCalculator:
    def test_account_with_backfill_gets_backfill_priority(self):
        state = _make_resource_state(["acct_1"], backfill_ids={"acct_1"})
        calc = PriorityCalculator(state)
        priority = calc.get_account_priority("acct_1")
        assert priority.level == PriorityLevel.BACKFILL
        assert priority.cursor_timestamp is None

    def test_account_without_backfill_gets_incremental_priority(self):
        cursor = datetime(2025, 3, 15, tzinfo=UTC)
        state = ResourceState(
            inc={"acct_1": ResourceState.Incremental(cursor=cursor)},
            backfill={"acct_1": None},
        )
        calc = PriorityCalculator(state)
        priority = calc.get_account_priority("acct_1")
        assert priority.level == PriorityLevel.INCREMENTAL
        assert priority.cursor_timestamp == cursor

    def test_missing_account_raises(self):
        state = _make_resource_state(["acct_1"])
        calc = PriorityCalculator(state)
        with pytest.raises(RuntimeError, match="has no state"):
            calc.get_account_priority("acct_missing")

    def test_select_respects_subtask_limit(self):
        state = ResourceState(
            inc={
                "acct_bf": ResourceState.Incremental(cursor=datetime(2025, 1, 1, tzinfo=UTC)),
                "acct_inc_old": ResourceState.Incremental(cursor=datetime(2025, 1, 1, tzinfo=UTC)),
                "acct_inc_new": ResourceState.Incremental(cursor=datetime(2025, 6, 1, tzinfo=UTC)),
            },
            backfill={
                "acct_bf": ResourceState.Backfill(cutoff=datetime(2025, 1, 1, tzinfo=UTC), next_page=None),
                "acct_inc_old": None,
                "acct_inc_new": None,
            },
        )
        calc = PriorityCalculator(state)

        # backfill(2 subtasks) + inc_old(1 subtask) = 3, which hits the limit
        selected = calc.select_prioritized_accounts(
            ["acct_bf", "acct_inc_old", "acct_inc_new"], max_subtask_count=3
        )
        assert "acct_bf" in selected
        assert "acct_inc_old" in selected
        assert "acct_inc_new" not in selected


class TestWorkItemManagerCancellation:
    def test_cancel_sets_event_on_tracked_item(self):
        state = _make_resource_state(["acct_1"])
        calc = PriorityCalculator(state)
        mgr = WorkItemManager(calc)
        fc, fp = _make_factories()

        work_items, _ = mgr.generate_work_items(
            ["acct_1"], max_subtask_count=10,
            fetch_changes_factory=fc, fetch_page_factory=fp, resource_state=state,
        )
        mgr.register_tracked_work(work_items[0])

        assert not work_items[0].cancellation_event.is_set()
        mgr.cancel_work_for_account("acct_1")
        assert work_items[0].cancellation_event.is_set()

    def test_cancel_untracked_account_is_noop(self):
        state = _make_resource_state(["acct_1"])
        calc = PriorityCalculator(state)
        mgr = WorkItemManager(calc)

        # Should not raise.
        mgr.cancel_work_for_account("acct_nonexistent")

    def test_cancelled_but_tracked_item_still_blocks_duplicate(self):
        """
        A cancelled-but-not-yet-unregistered item must still prevent
        duplicates — the worker hasn't finished cleanup yet.
        """
        state = _make_resource_state(["acct_1"])
        calc = PriorityCalculator(state)
        mgr = WorkItemManager(calc)
        fc, fp = _make_factories()

        work_items_1, _ = mgr.generate_work_items(
            ["acct_1"], max_subtask_count=10,
            fetch_changes_factory=fc, fetch_page_factory=fp, resource_state=state,
        )
        mgr.register_tracked_work(work_items_1[0])
        mgr.cancel_work_for_account("acct_1")

        # Still tracked, so no duplicate.
        work_items_2, _ = mgr.generate_work_items(
            ["acct_1"], max_subtask_count=10,
            fetch_changes_factory=fc, fetch_page_factory=fp, resource_state=state,
        )
        assert len(work_items_2) == 0


class TestWorkItemBackfill:
    def test_has_backfill_work_with_backfill(self):
        state = _make_resource_state(["acct_1"], backfill_ids={"acct_1"})
        calc = PriorityCalculator(state)
        mgr = WorkItemManager(calc)
        fc, fp = _make_factories()

        work_items, _ = mgr.generate_work_items(
            ["acct_1"], max_subtask_count=10,
            fetch_changes_factory=fc, fetch_page_factory=fp, resource_state=state,
        )
        assert work_items[0].has_backfill_work()

    def test_has_backfill_work_without_backfill(self):
        state = _make_resource_state(["acct_1"])
        calc = PriorityCalculator(state)
        mgr = WorkItemManager(calc)
        fc, fp = _make_factories()

        work_items, _ = mgr.generate_work_items(
            ["acct_1"], max_subtask_count=10,
            fetch_changes_factory=fc, fetch_page_factory=fp, resource_state=state,
        )
        assert not work_items[0].has_backfill_work()


class TestPriorityQueueManagerRefresh:
    @pytest.mark.asyncio
    async def test_second_refresh_does_not_duplicate_queued_items(self):
        account_ids = ["acct_1", "acct_2"]
        state = _make_resource_state(account_ids)
        fc, fp = _make_factories()

        mock_task = MagicMock()

        mgr = PriorityQueueManager(
            all_account_ids=account_ids,
            resource_state=state,
            task=mock_task,
            binding=MagicMock(),
            binding_index=0,
            fetch_changes_factory=fc,
            fetch_page_factory=fp,
            prefix="test",
            config=PriorityQueueConfig(max_subtask_count=10, work_queue_size=100),
        )

        await mgr._refresh_work_queue()
        assert mgr.work_queue.qsize() == 2

        # Second refresh should not enqueue any new items.
        await mgr._refresh_work_queue()
        assert mgr.work_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_unregistered_account_is_re_enqueued_after_refresh(self):
        """After a worker finishes and unregisters, the next refresh re-enqueues that account."""
        account_ids = ["acct_1", "acct_2"]
        state = _make_resource_state(account_ids)
        fc, fp = _make_factories()

        mgr = PriorityQueueManager(
            all_account_ids=account_ids,
            resource_state=state,
            task=MagicMock(),
            binding=MagicMock(),
            binding_index=0,
            fetch_changes_factory=fc,
            fetch_page_factory=fp,
            prefix="test",
            config=PriorityQueueConfig(max_subtask_count=10, work_queue_size=100),
        )

        await mgr._refresh_work_queue()
        assert mgr.work_queue.qsize() == 2

        # Simulate a worker finishing acct_1: dequeue and unregister.
        _, work_item = mgr.work_queue.get_nowait()
        mgr.work_item_manager.unregister_tracked_work(work_item)
        finished_account = work_item.account_id

        # Next refresh should re-enqueue that account.
        await mgr._refresh_work_queue()
        assert mgr.work_queue.qsize() == 2

        # Verify the re-enqueued item is for the finished account.
        queued_accounts = set()
        while not mgr.work_queue.empty():
            _, wi = mgr.work_queue.get_nowait()
            queued_accounts.add(wi.account_id)
        assert finished_account in queued_accounts
