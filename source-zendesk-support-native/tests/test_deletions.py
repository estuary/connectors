import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, AsyncGenerator, Callable

import pytest

from source_zendesk_support_native.api.deletions import (
    DELETION_LAG,
    fetch_deleted_tickets,
    fetch_deleted_users,
)
from source_zendesk_support_native.api.incremental_keyset_paginated import build_seek_cursor

log = logging.getLogger(__name__)


def _user(id: int, updated_at: datetime) -> dict[str, Any]:
    return {"id": id, "updated_at": updated_at.strftime("%Y-%m-%dT%H:%M:%SZ"), "active": False}


def _deleted_ticket(id: int, deleted_at: datetime) -> dict[str, Any]:
    return {"id": id, "subject": "Deleted ticket", "deleted_at": deleted_at.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _page(key: str, records: list[dict[str, Any]], after_cursor: str | None) -> dict[str, Any]:
    return {
        key: records,
        "meta": {"has_more": after_cursor is not None, "after_cursor": after_cursor, "before_cursor": None},
    }


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


async def _fetch_changes(fetch: Callable[..., AsyncGenerator[Any, None]], http: MockHTTP, log_cursor: datetime) -> tuple[list[Any], datetime, datetime]:
    before = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)
    results = [r async for r in fetch(http, "example", log, log_cursor)]
    after = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)
    return results, before, after


@pytest.mark.asyncio
async def test_emits_deletions_in_window() -> None:
    now = datetime.now(tz=UTC).replace(microsecond=0)
    log_cursor = now - timedelta(hours=2)

    http = MockHTTP(
        _page("deleted_users", [_user(1, log_cursor), _user(2, log_cursor + timedelta(seconds=1))], after_cursor="c1"),
        _page("deleted_users", [_user(3, now - timedelta(hours=1)), _user(4, now)], after_cursor=None),
    )

    results, before, after = await _fetch_changes(fetch_deleted_users, http, log_cursor)

    deletions, horizon = results[:-1], results[-1]
    assert [d.id for d in deletions] == [2, 3]
    assert all(d.meta_ is not None and d.meta_.op == "d" for d in deletions)
    assert before <= horizon <= after

    assert http.params == [
        {"sort": "id", "page[size]": 100},
        {"sort": "id", "page[size]": 100, "page[after]": "c1"},
    ]


@pytest.mark.asyncio
async def test_empty_read_still_checkpoints() -> None:
    log_cursor = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(hours=2)

    http = MockHTTP(_page("deleted_users", [], after_cursor=None))

    results, before, after = await _fetch_changes(fetch_deleted_users, http, log_cursor)

    assert len(results) == 1
    assert before <= results[0] <= after


@pytest.mark.asyncio
async def test_emits_deleted_tickets_in_window_from_every_page() -> None:
    now = datetime.now(tz=UTC).replace(microsecond=0)
    log_cursor = now - timedelta(hours=2)

    # Ticket 2 was deleted after the horizon, and ticket 3 was deleted long
    # before log_cursor but sorts after it.
    http = MockHTTP(
        _page("deleted_tickets", [
            _deleted_ticket(1, log_cursor + timedelta(seconds=1)),
            _deleted_ticket(2, now),
            _deleted_ticket(3, log_cursor - timedelta(days=20)),
        ], after_cursor="c1"),
        _page("deleted_tickets", [_deleted_ticket(4, now - timedelta(hours=1))], after_cursor=None),
    )

    results, before, after = await _fetch_changes(fetch_deleted_tickets, http, log_cursor)

    deletions, horizon = results[:-1], results[-1]
    assert [d.id for d in deletions] == [1, 4]
    assert all(d.meta_ is not None and d.meta_.op == "d" for d in deletions)
    assert before <= horizon <= after

    assert http.params == [
        {"sort": "updated_at", "page[size]": 100, "page[after]": build_seek_cursor(log_cursor), "support_type_scope": "all"},
        {"sort": "updated_at", "page[size]": 100, "page[after]": "c1", "support_type_scope": "all"},
    ]
