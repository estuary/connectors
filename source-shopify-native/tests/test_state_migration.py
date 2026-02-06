"""
Tests for Shopify native connector state migration.

These tests validate:
- State is correctly migrated from flat to dict-based format at resource level
- No interleaving or misrepresentation of state between streams / keys
- State remains stable and consistent across multiple migration runs
- Backwards compatibility when existing flat state is present
- Proper error handling for invalid state formats
"""

import pytest
from datetime import datetime, timedelta, UTC
from logging import Logger
from unittest.mock import MagicMock, AsyncMock

from estuary_cdk.capture.common import ResourceState, ResourceConfig, CaptureBinding
from estuary_cdk.capture import Task

from source_shopify_native.models import ConnectorState
from source_shopify_native.resources import (
    _reconcile_connector_state,
    _migrate_flat_to_dict_state,
    _add_missing_store_entries,
)


def make_binding(state_key: str) -> CaptureBinding[ResourceConfig]:
    """Create a mock CaptureBinding with the given state key."""
    binding = MagicMock(spec=CaptureBinding)
    binding.stateKey = state_key
    return binding


def make_task() -> Task:
    """Create a mock Task with logging and checkpoint capabilities."""
    task = MagicMock(spec=Task)
    task.log = MagicMock()
    task.log.info = MagicMock()
    task.log.error = MagicMock()
    task.checkpoint = AsyncMock()
    return task


def make_initial_state(
    store_ids: list[str],
    use_backfill: bool = False,
) -> ResourceState:
    """Create initial state for testing, matching the connector's _create_initial_state."""
    cutoff = datetime.now(tz=UTC)
    start_date = datetime.now(tz=UTC) - timedelta(days=30)

    if use_backfill:
        return ResourceState(
            inc={sid: ResourceState.Incremental(cursor=cutoff) for sid in store_ids},
            backfill={sid: ResourceState.Backfill(next_page=start_date.isoformat(), cutoff=cutoff) for sid in store_ids},
        )
    return ResourceState(
        inc={sid: ResourceState.Incremental(cursor=start_date) for sid in store_ids},
    )


class TestShopifyStateMigration:
    """Tests for Shopify connector state migration from flat to dict-based format."""

    @pytest.mark.asyncio
    async def test_migrate_flat_incremental_state_to_dict(self):
        """Flat incremental state should be migrated to dict format using first store."""
        cursor = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        flat_inc = ResourceState.Incremental(cursor=cursor)

        state = ResourceState(inc=flat_inc)
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        assert "my-store" in state.inc
        inc_state = state.inc["my-store"]
        assert inc_state is not None
        assert inc_state.cursor == cursor
        task.checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_migrate_flat_backfill_state_to_dict(self):
        """Flat backfill state should be migrated along with incremental state."""
        cursor = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        cutoff = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)

        state = ResourceState(
            inc=ResourceState.Incremental(cursor=cursor),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page="cursor_abc123"),
        )
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids, use_backfill=True)
        binding = make_binding("products")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        # Check incremental state
        assert isinstance(state.inc, dict)
        inc_state = state.inc["my-store"]
        assert inc_state is not None
        assert inc_state.cursor == cursor

        # Check backfill state
        assert isinstance(state.backfill, dict)
        backfill_state = state.backfill["my-store"]
        assert backfill_state is not None
        assert backfill_state.cutoff == cutoff
        assert backfill_state.next_page == "cursor_abc123"

    @pytest.mark.asyncio
    async def test_skip_already_migrated_state(self):
        """Dict-based state should not trigger migration."""
        cursor = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

        # Already dict-based state
        state = ResourceState(
            inc={"my-store": ResourceState.Incremental(cursor=cursor)},
        )
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        inc_state = state.inc["my-store"]
        assert inc_state is not None
        assert inc_state.cursor == cursor
        # No checkpoint needed since state was already dict-based
        task.checkpoint.assert_not_called()

    @pytest.mark.asyncio
    async def test_migration_uses_first_store_from_list(self):
        """Migration should use first store in store_ids as the key for migrated state."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)

        state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
        # Multiple stores - first one should be used for migration
        store_ids = ["primary-store", "secondary-store", "third-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        # Should use first store as key
        assert "primary-store" in state.inc
        # Other stores should be added from initial state
        assert "secondary-store" in state.inc
        assert "third-store" in state.inc


class TestShopifyStateMigrationIdempotency:
    """Tests to ensure migration is idempotent and stable."""

    @pytest.mark.asyncio
    async def test_migration_is_idempotent(self):
        """Running reconciliation twice should produce the same result."""
        cursor = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

        state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        # First reconciliation
        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)
        assert task.checkpoint.call_count == 1

        # Second reconciliation with already-migrated state
        task2 = make_task()
        await _reconcile_connector_state(store_ids, binding, state, initial_state, task2)

        # No checkpoint needed since no changes
        task2.checkpoint.assert_not_called()
        assert isinstance(state.inc, dict)
        inc_state = state.inc["my-store"]
        assert inc_state is not None
        assert inc_state.cursor == cursor


class TestShopifyStateMigrationEdgeCases:
    """Edge case tests for state migration."""

    @pytest.mark.asyncio
    async def test_migration_preserves_none_backfill(self):
        """Completed backfill (None) should be preserved during migration."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)

        state = ResourceState(
            inc=ResourceState.Incremental(cursor=cursor),
            backfill=None,  # Completed backfill
        )
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        # Backfill should remain None (completed)
        assert state.backfill is None

    @pytest.mark.asyncio
    async def test_migration_with_complex_cursor_types(self):
        """Migration should handle different cursor types correctly."""
        datetime_cursor = datetime(2024, 6, 15, 12, 30, 45, 123456, tzinfo=UTC)

        state = ResourceState(inc=ResourceState.Incremental(cursor=datetime_cursor))
        store_ids = ["my-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        inc_state = state.inc["my-store"]
        assert inc_state is not None
        # Verify cursor is exactly preserved
        migrated_cursor = inc_state.cursor
        assert migrated_cursor == datetime_cursor
        assert isinstance(migrated_cursor, datetime)
        assert migrated_cursor.tzinfo == UTC


class TestShopifyStateReconciliation:
    """Tests for adding new stores to existing captures."""

    @pytest.mark.asyncio
    async def test_add_new_store_to_existing_capture(self):
        """Adding a new store should initialize its state from initial_state."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)

        # Existing state with one store
        state = ResourceState(
            inc={"existing-store": ResourceState.Incremental(cursor=cursor)},
        )
        # Now we have two stores
        store_ids = ["existing-store", "new-store"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        assert isinstance(state.inc, dict)
        # Existing store should keep its cursor
        assert state.inc["existing-store"].cursor == cursor
        # New store should be initialized
        assert "new-store" in state.inc
        assert state.inc["new-store"] is not None
        task.checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_missing_incremental_with_backfill(self):
        """Edge case: backfill exists but incremental doesn't - should reinitialize."""
        cutoff = datetime(2024, 6, 1, tzinfo=UTC)

        # State with backfill but missing incremental for a store
        state = ResourceState(
            inc={"store-a": ResourceState.Incremental(cursor=datetime.now(tz=UTC))},
            backfill={"store-b": ResourceState.Backfill(cutoff=cutoff, next_page="page2")},
        )
        store_ids = ["store-a", "store-b"]
        initial_state = make_initial_state(store_ids, use_backfill=True)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        # store-b should be reinitialized
        assert "store-b" in state.inc
        task.checkpoint.assert_called_once()


class TestShopifyStateMigrationBackwardsCompatibility:
    """Tests for backwards compatibility with existing flat state."""

    @pytest.mark.asyncio
    async def test_legacy_flat_state_works_after_migration(self):
        """Legacy flat state should be usable after migration."""
        legacy_cursor = datetime(2024, 1, 1, tzinfo=UTC)
        legacy_cutoff = datetime(2023, 12, 1, tzinfo=UTC)

        state = ResourceState(
            inc=ResourceState.Incremental(cursor=legacy_cursor),
            backfill=ResourceState.Backfill(cutoff=legacy_cutoff, next_page="legacy_page"),
        )
        store_ids = ["migrated-store"]
        initial_state = make_initial_state(store_ids, use_backfill=True)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        # State should be dict-based but preserve original values
        assert isinstance(state.inc, dict)
        inc_state = state.inc["migrated-store"]
        assert inc_state is not None
        assert inc_state.cursor == legacy_cursor
        assert isinstance(state.backfill, dict)
        backfill_state = state.backfill["migrated-store"]
        assert backfill_state is not None
        assert backfill_state.cutoff == legacy_cutoff
        assert backfill_state.next_page == "legacy_page"

    @pytest.mark.asyncio
    async def test_new_capture_with_dict_state_not_modified(self):
        """New captures that start with dict state should not be modified."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)

        # State that was created after the multi-store feature
        state = ResourceState(
            inc={"store-a": ResourceState.Incremental(cursor=cursor)},
        )
        store_ids = ["store-a"]
        initial_state = make_initial_state(store_ids)
        binding = make_binding("orders")
        task = make_task()

        await _reconcile_connector_state(store_ids, binding, state, initial_state, task)

        # No checkpoint since no modification
        task.checkpoint.assert_not_called()
        assert isinstance(state.inc, dict)
        inc_state = state.inc["store-a"]
        assert inc_state is not None
        assert inc_state.cursor == cursor


class TestMigrateFlatToDictState:
    """Unit tests for _migrate_flat_to_dict_state helper function."""

    def test_migrates_flat_incremental_state(self):
        """Flat incremental state should be converted to dict format."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
        log = MagicMock(spec=Logger)

        result = _migrate_flat_to_dict_state(state, "my-store", log)

        assert result is True
        assert isinstance(state.inc, dict)
        assert "my-store" in state.inc
        assert state.inc["my-store"].cursor == cursor
        log.info.assert_called_once()

    def test_migrates_flat_backfill_state(self):
        """Flat backfill state should be converted along with incremental."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        cutoff = datetime(2024, 6, 1, tzinfo=UTC)
        state = ResourceState(
            inc=ResourceState.Incremental(cursor=cursor),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page="page1"),
        )
        log = MagicMock(spec=Logger)

        result = _migrate_flat_to_dict_state(state, "my-store", log)

        assert result is True
        assert isinstance(state.inc, dict)
        assert isinstance(state.backfill, dict)
        assert state.backfill["my-store"].cutoff == cutoff

    def test_skips_already_dict_state(self):
        """Dict-based state should not be modified."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        state = ResourceState(
            inc={"existing-store": ResourceState.Incremental(cursor=cursor)},
        )
        log = MagicMock(spec=Logger)

        result = _migrate_flat_to_dict_state(state, "my-store", log)

        assert result is False
        # State unchanged
        assert "existing-store" in state.inc
        assert "my-store" not in state.inc
        log.info.assert_not_called()

    def test_skips_none_state(self):
        """None incremental state should not be modified."""
        state = ResourceState(inc=None)
        log = MagicMock(spec=Logger)

        result = _migrate_flat_to_dict_state(state, "my-store", log)

        assert result is False
        assert state.inc is None


class TestAddMissingStoreEntries:
    """Unit tests for _add_missing_store_entries helper function."""

    def test_adds_missing_store(self):
        """Missing store should be added from initial_state."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        state = ResourceState(
            inc={"store-a": ResourceState.Incremental(cursor=cursor)},
        )
        initial_state = make_initial_state(["store-a", "store-b"])
        log = MagicMock(spec=Logger)

        result = _add_missing_store_entries(state, ["store-a", "store-b"], initial_state, log)

        assert result is True
        assert "store-b" in state.inc
        log.info.assert_called_once()

    def test_no_changes_when_all_stores_exist(self):
        """No changes when all stores already have state."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        state = ResourceState(
            inc={
                "store-a": ResourceState.Incremental(cursor=cursor),
                "store-b": ResourceState.Incremental(cursor=cursor),
            },
        )
        initial_state = make_initial_state(["store-a", "store-b"])
        log = MagicMock(spec=Logger)

        result = _add_missing_store_entries(state, ["store-a", "store-b"], initial_state, log)

        assert result is False
        log.info.assert_not_called()

    def test_raises_error_for_non_dict_state(self):
        """Should raise RuntimeError if state.inc is not a dict."""
        state = ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC)))
        initial_state = make_initial_state(["my-store"])
        log = MagicMock(spec=Logger)

        with pytest.raises(RuntimeError) as exc_info:
            _add_missing_store_entries(state, ["my-store"], initial_state, log)

        assert "not dict-based" in str(exc_info.value)
        assert "bug in state migration" in str(exc_info.value)


class TestReconcileConnectorStateErrorHandling:
    """Tests for error handling in _reconcile_connector_state."""

    @pytest.mark.asyncio
    async def test_raises_error_for_invalid_initial_state(self):
        """Should raise RuntimeError if initial_state.inc is not a dict."""
        cursor = datetime(2024, 6, 15, tzinfo=UTC)
        state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
        # Invalid initial_state with flat inc (this should never happen in practice)
        invalid_initial_state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
        binding = make_binding("orders")
        task = make_task()

        with pytest.raises(RuntimeError) as exc_info:
            await _reconcile_connector_state(
                ["my-store"], binding, state, invalid_initial_state, task
            )

        assert "Invalid initial_state" in str(exc_info.value)
        assert "orders" in str(exc_info.value)
        assert "bug in state creation" in str(exc_info.value)
        task.checkpoint.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_error_for_unmigrated_non_flat_state(self):
        """Should raise RuntimeError if state.inc is neither flat nor dict after migration attempt."""
        # This is a contrived scenario - state.inc is None which is neither Incremental nor dict
        state = ResourceState(inc=None)
        initial_state = make_initial_state(["my-store"])
        binding = make_binding("orders")
        task = make_task()

        # This should raise because _add_missing_store_entries expects dict state
        with pytest.raises(RuntimeError) as exc_info:
            await _reconcile_connector_state(
                ["my-store"], binding, state, initial_state, task
            )

        assert "not dict-based" in str(exc_info.value)
