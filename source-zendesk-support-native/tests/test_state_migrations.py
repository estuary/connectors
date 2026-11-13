from datetime import datetime, timedelta, UTC
from typing import Any

import pytest

from source_zendesk_support_native.api import cursor_export_cursor_to_dt
from source_zendesk_support_native.api.shared import s_to_dt
from source_zendesk_support_native.models import ResourceState
from source_zendesk_support_native.resources import _migrate_export_state_to_keyset

USERS_SUBTASKS = ("users", "deleted_users")
USERS_MARGIN = timedelta(seconds=1)


def _merge_patch(target: Any, patch: Any) -> Any:
    # RFC 7396, which the runtime applies to each checkpoint.
    if not isinstance(patch, dict):
        return patch
    result = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = _merge_patch(result.get(key), value)
    return result


def _saved_after(state: ResourceState, checkpoint: ResourceState) -> ResourceState:
    saved = _merge_patch(
        state.model_dump(mode="json", exclude_unset=True),
        checkpoint.model_dump(mode="json", exclude_unset=True),
    )
    return ResourceState.model_validate(saved)


def test_cursor_export_cursor_to_dt() -> None:
    # A decoded cursor from the Bruno "users cursor" example, and the plain epoch
    # string stored before the export's first page.
    assert cursor_export_cursor_to_dt("1791388325.0||54337475674900|") == s_to_dt(1791388325)
    assert cursor_export_cursor_to_dt("1791388325") == s_to_dt(1791388325)

    with pytest.raises(ValueError):
        cursor_export_cursor_to_dt("not a cursor")


def test_migrates_users_state_to_keyset() -> None:
    cutoff = datetime(2026, 10, 1, 12, 0, 0, 500_000, tzinfo=UTC)
    state = ResourceState(
        inc=ResourceState.Incremental(cursor=("1791388325.0||54337475674900|",)),
        backfill=ResourceState.Backfill(cutoff=cutoff, next_page="1759000000.0||123|"),
    )

    migrated = _migrate_export_state_to_keyset(state, USERS_SUBTASKS, USERS_MARGIN)
    assert migrated is not None

    saved = _saved_after(state, migrated)
    assert saved.inc == {
        "users": ResourceState.Incremental(cursor=s_to_dt(1791388324)),
        "deleted_users": ResourceState.Incremental(cursor=s_to_dt(1791388324)),
    }
    assert saved.backfill == ResourceState.Backfill(cutoff=cutoff, next_page=1758999999)

    assert _migrate_export_state_to_keyset(saved, USERS_SUBTASKS, USERS_MARGIN) is None


def test_users_migration_keeps_an_unstarted_backfill() -> None:
    cutoff = datetime(2026, 10, 1, 12, 0, 0, 500_000, tzinfo=UTC)
    state = ResourceState(
        inc=ResourceState.Incremental(cursor=("1791388325",)),
        backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
    )

    migrated = _migrate_export_state_to_keyset(state, USERS_SUBTASKS, USERS_MARGIN)
    assert migrated is not None

    saved = _saved_after(state, migrated)
    assert saved.inc == {
        "users": ResourceState.Incremental(cursor=s_to_dt(1791388324)),
        "deleted_users": ResourceState.Incremental(cursor=s_to_dt(1791388324)),
    }
    assert saved.backfill == ResourceState.Backfill(cutoff=cutoff, next_page=None)
