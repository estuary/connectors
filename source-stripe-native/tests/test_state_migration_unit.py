from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock
import pytest

from estuary_cdk.capture.common import ResourceState
from estuary_cdk.flow import CaptureBinding

from source_stripe_native.resources import (
    _maybe_migrate_exempt_state,
    build_subtask_to_flat_migration_patch,
)


@pytest.fixture
def mock_binding():
    binding = MagicMock(spec=CaptureBinding)
    binding.stateKey = "Accounts"
    return binding


@pytest.fixture
def mock_task():
    task = MagicMock()
    task.log = MagicMock()
    task.checkpoint = AsyncMock()
    return task


@pytest.mark.asyncio
async def test_migrate_dict_state_uses_platform_account_cursor(mock_binding, mock_task):
    """Migration should use the platform account's cursor when available."""
    platform_id = "acct_platform123"
    platform_cursor = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

    old_state = ResourceState(
        inc={
            platform_id: ResourceState.Incremental(cursor=platform_cursor),
            "acct_connected001": ResourceState.Incremental(cursor=datetime(2025, 1, 10, tzinfo=UTC)),
            "acct_connected002": ResourceState.Incremental(cursor=datetime(2025, 1, 20, tzinfo=UTC)),
        },
        backfill={
            platform_id: None,
            "acct_connected001": None,
            "acct_connected002": None,
        },
        last_initialized=datetime(2025, 1, 1, tzinfo=UTC),
    )

    migrated = await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, old_state, platform_id, mock_task
    )

    # Should be non-dict format
    assert not isinstance(migrated.inc, dict)
    assert not isinstance(migrated.backfill, dict)

    # Should use platform account's cursor
    assert migrated.inc.cursor == platform_cursor

    # Backfill should be reset with cutoff matching the cursor
    assert migrated.backfill.cutoff == platform_cursor
    assert migrated.backfill.next_page is None

    # Should preserve last_initialized
    assert migrated.last_initialized == datetime(2025, 1, 1, tzinfo=UTC)

    # Should checkpoint once
    mock_task.checkpoint.assert_called_once()


@pytest.mark.asyncio
async def test_migrate_dict_state_fallback_to_earliest_cursor(mock_binding, mock_task):
    """Migration should use earliest cursor when platform account cursor is missing."""
    platform_id = "acct_platform123"
    earliest_cursor = datetime(2025, 1, 5, tzinfo=UTC)

    old_state = ResourceState(
        inc={
            # Platform account has no cursor
            "acct_connected001": ResourceState.Incremental(cursor=datetime(2025, 1, 20, tzinfo=UTC)),
            "acct_connected002": ResourceState.Incremental(cursor=earliest_cursor),  # Earliest
            "acct_connected003": ResourceState.Incremental(cursor=datetime(2025, 1, 15, tzinfo=UTC)),
        },
        backfill={
            "acct_connected001": None,
            "acct_connected002": None,
            "acct_connected003": None,
        },
    )

    migrated = await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, old_state, platform_id, mock_task
    )

    # Should be non-dict format
    assert not isinstance(migrated.inc, dict)

    # Should use earliest cursor as fallback
    assert migrated.inc.cursor == earliest_cursor
    assert migrated.backfill.cutoff == earliest_cursor


@pytest.mark.asyncio
async def test_migrate_dict_state_with_none_cursors(mock_binding, mock_task):
    """Migration should skip None cursors when finding earliest."""
    platform_id = "acct_platform123"
    valid_cursor = datetime(2025, 1, 10, tzinfo=UTC)

    old_state = ResourceState(
        inc={
            "acct_connected001": None,  # None cursor
            "acct_connected002": ResourceState.Incremental(cursor=valid_cursor),
            "acct_connected003": None,  # None cursor
        },
        backfill={
            "acct_connected001": None,
            "acct_connected002": None,
            "acct_connected003": None,
        },
    )

    migrated = await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, old_state, platform_id, mock_task
    )

    # Should use the only valid cursor
    assert migrated.inc.cursor == valid_cursor


@pytest.mark.asyncio
async def test_non_dict_state_unchanged(mock_binding, mock_task):
    """Non-dict state should be returned unchanged."""
    cursor = datetime(2025, 1, 15, tzinfo=UTC)

    original_state = ResourceState(
        inc=ResourceState.Incremental(cursor=cursor),
        backfill=ResourceState.Backfill(next_page=None, cutoff=cursor),
        last_initialized=datetime(2025, 1, 1, tzinfo=UTC),
    )

    migrated = await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, original_state, "acct_platform123", mock_task
    )

    # Should return the same state (no migration needed)
    assert migrated.inc.cursor == cursor
    assert migrated.backfill.cutoff == cursor
    assert migrated.last_initialized == datetime(2025, 1, 1, tzinfo=UTC)

    # Should not checkpoint since no migration was needed
    mock_task.checkpoint.assert_not_called()


@pytest.mark.asyncio
async def test_migrate_empty_dict_state(mock_binding, mock_task):
    """Migration with empty dicts should use current time as cutoff."""
    old_state = ResourceState(
        inc={},
        backfill={},
    )

    before = datetime.now(tz=UTC)
    migrated = await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, old_state, "acct_platform123", mock_task
    )
    after = datetime.now(tz=UTC)

    # Should be non-dict format with cursor set to approximately now
    assert not isinstance(migrated.inc, dict)
    assert before <= migrated.inc.cursor <= after
    assert before <= migrated.backfill.cutoff <= after


@pytest.mark.asyncio
async def test_non_exempt_stream_unchanged(mock_binding, mock_task):
    """Non-exempt streams should not be migrated even if they have dict state."""
    platform_id = "acct_platform123"

    old_state = ResourceState(
        inc={
            platform_id: ResourceState.Incremental(cursor=datetime(2025, 1, 15, tzinfo=UTC)),
            "acct_connected001": ResourceState.Incremental(cursor=datetime(2025, 1, 10, tzinfo=UTC)),
        },
        backfill={
            platform_id: None,
            "acct_connected001": None,
        },
    )

    # BankAccounts is not in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    migrated = await _maybe_migrate_exempt_state(
        "BankAccounts", mock_binding, old_state, platform_id, mock_task
    )

    # Should return the same state (no migration for non-exempt streams)
    assert isinstance(migrated.inc, dict)
    assert isinstance(migrated.backfill, dict)

    # Should not checkpoint since no migration was needed
    mock_task.checkpoint.assert_not_called()


@pytest.mark.asyncio
async def test_checkpoint_patch_structure(mock_binding, mock_task):
    """Verify the checkpoint contains both deletions (null) and new keys in a single patch."""
    platform_id = "acct_platform123"
    platform_cursor = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

    old_state = ResourceState(
        inc={
            platform_id: ResourceState.Incremental(cursor=platform_cursor),
            "acct_connected001": ResourceState.Incremental(cursor=datetime(2025, 1, 10, tzinfo=UTC)),
        },
        backfill={
            platform_id: None,
            "acct_connected001": None,
        },
    )

    await _maybe_migrate_exempt_state(
        "Accounts", mock_binding, old_state, platform_id, mock_task
    )

    # Should checkpoint exactly once (single atomic operation)
    mock_task.checkpoint.assert_called_once()
    checkpoint_arg = mock_task.checkpoint.call_args[0][0]

    # Extract the migration state (built via model_construct)
    migration_state = checkpoint_arg.bindingStateV1["Accounts"]

    # inc should have null values for old keys AND the new cursor key
    assert migration_state.inc[platform_id] is None
    assert migration_state.inc["acct_connected001"] is None
    assert migration_state.inc["cursor"] == platform_cursor

    # backfill should have null values for old keys AND new cutoff/next_page
    assert migration_state.backfill[platform_id] is None
    assert migration_state.backfill["acct_connected001"] is None
    assert migration_state.backfill["cutoff"] == platform_cursor
    assert migration_state.backfill["next_page"] is None


# =============================================================================
# Tests for build_subtask_to_flat_migration_patch helper function
# =============================================================================

class TestBuildSubtaskToFlatMigrationPatch:
    """Unit tests for the migration patch builder helper function."""

    def test_patch_has_required_keys(self):
        """Patch should have inc, backfill, and last_initialized keys."""
        cutoff = datetime(2025, 1, 15, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys={"acct_1", "acct_2"},
            cutoff=cutoff,
        )

        assert "inc" in patch
        assert "backfill" in patch

    def test_patch_inc_has_deletion_markers_and_cursor(self):
        """inc dict should have None for old keys and cursor field."""
        cutoff = datetime(2025, 1, 15, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys={"acct_platform", "acct_connected_1", "acct_connected_2"},
            cutoff=cutoff,
        )

        inc = patch["inc"]
        # Deletion markers for old keys
        assert inc["acct_platform"] is None
        assert inc["acct_connected_1"] is None
        assert inc["acct_connected_2"] is None
        # New cursor field
        assert inc["cursor"] == cutoff

    def test_patch_backfill_has_deletion_markers_and_new_fields(self):
        """backfill dict should have None for old keys, cutoff, and next_page."""
        cutoff = datetime(2025, 1, 15, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys={"acct_1", "acct_2"},
            cutoff=cutoff,
        )

        backfill = patch["backfill"]
        # Deletion markers for old keys
        assert backfill["acct_1"] is None
        assert backfill["acct_2"] is None
        # New fields
        assert backfill["cutoff"] == cutoff
        assert backfill["next_page"] is None

    def test_patch_with_empty_old_keys(self):
        """Patch with empty old_keys should still have new cursor/backfill fields."""
        cutoff = datetime(2025, 1, 15, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys=set(),
            cutoff=cutoff,
        )

        # inc should only have cursor (no deletion markers)
        assert patch["inc"] == {"cursor": cutoff}
        # backfill should only have new fields (no deletion markers)
        assert patch["backfill"] == {"cutoff": cutoff, "next_page": None}

    def test_patch_structure_is_valid_for_json_serialization(self):
        """Patch should be JSON-serializable (no unsupported types)."""
        import json

        cutoff = datetime(2025, 1, 15, 12, 30, 45, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys={"acct_1", "acct_2"},
            cutoff=cutoff,
        )

        # Should be serializable (datetime needs conversion, but structure should be valid)
        # Convert datetimes to ISO strings for JSON
        def default(o):
            if isinstance(o, datetime):
                return o.isoformat()
            raise TypeError(f"Object of type {type(o)} is not JSON serializable")

        json_str = json.dumps(patch, default=default)
        assert json_str  # Should not raise

    def test_patch_can_be_used_with_model_construct(self):
        """Patch should work with ResourceState.model_construct()."""
        cutoff = datetime(2025, 1, 15, tzinfo=UTC)

        patch = build_subtask_to_flat_migration_patch(
            old_keys={"acct_platform", "acct_connected"},
            cutoff=cutoff,
        )

        # model_construct bypasses validation, so this should work
        state = ResourceState.model_construct(**patch)

        # Verify the structure is as expected (dict, not Incremental/Backfill)
        assert isinstance(state.inc, dict)
        assert isinstance(state.backfill, dict)
        assert state.inc["cursor"] == cutoff
        assert state.inc["acct_platform"] is None
        assert state.backfill["cutoff"] == cutoff
        assert state.backfill["next_page"] is None
