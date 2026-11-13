import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, AsyncGenerator, Callable

import pytest
from pydantic import BaseModel

from source_zendesk_support_native.api import (
    backfill_incremental_cursor_export_resources,
    cursor_export_cursor_to_dt,
)
from source_zendesk_support_native.api.shared import s_to_dt
from source_zendesk_support_native.models import ResourceState
from source_zendesk_support_native.resources import (
    _migrate_export_state_to_keyset,
    _migrate_keyset_state_to_export,
)

USERS_SUBTASKS = ("users", "deleted_users")
USERS_MARGIN = timedelta(seconds=1)

log = logging.getLogger(__name__)


class MockHTTP:
    """Queues JSON bodies for http.request_stream and records each request's params."""

    def __init__(self, *pages: dict[str, Any]) -> None:
        self._pages = list(pages)
        self.params: list[dict[str, Any]] = []

    async def request_stream(self, log: Any, url: str, **kwargs: Any) -> tuple[dict[str, str], Callable[[], AsyncGenerator[bytes, None]]]:
        self.params.append(dict(kwargs["params"]))
        body = json.dumps(self._pages.pop(0)).encode()

        async def stream() -> AsyncGenerator[bytes, None]:
            yield body

        return {}, stream


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


def _saved_after(state: ResourceState, checkpoint: BaseModel) -> ResourceState:
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


def test_users_state_round_trip_never_starts_later() -> None:
    cutoff = datetime(2026, 10, 1, 12, 0, 0, 500_000, tzinfo=UTC)
    export_state = ResourceState(
        inc=ResourceState.Incremental(cursor=("1791388325.0||54337475674900|",)),
        backfill=ResourceState.Backfill(cutoff=cutoff, next_page="1759000000.0||123|"),
    )

    migrated = _migrate_export_state_to_keyset(export_state, USERS_SUBTASKS, USERS_MARGIN)
    assert migrated is not None
    keyset_state = _saved_after(export_state, migrated)

    # The users subtask moves ahead while the deletion subtask waits for its
    # window to cover an hour.
    assert isinstance(keyset_state.inc, dict)
    keyset_state.inc["users"] = ResourceState.Incremental(cursor=s_to_dt(1791392000))

    patch = _migrate_keyset_state_to_export(keyset_state)
    assert patch is not None

    reverted = _saved_after(keyset_state, patch)
    assert reverted.inc == ResourceState.Incremental(cursor=("1791388324",))
    assert reverted.backfill == ResourceState.Backfill(cutoff=cutoff, next_page="1758999999")

    assert _migrate_keyset_state_to_export(reverted) is None


@pytest.mark.asyncio
async def test_export_backfill_resumes_from_a_plain_epoch_page() -> None:
    http = MockHTTP({"users": [], "after_cursor": None, "end_of_stream": True})

    results = [
        r async for r in backfill_incremental_cursor_export_resources(
            http,  # type: ignore[arg-type]
            "example", "users", s_to_dt(0), log, "1758999999", datetime.now(tz=UTC),
        )
    ]

    assert results == []
    assert http.params[0]["start_time"] == 1758999999
    assert "cursor" not in http.params[0]
