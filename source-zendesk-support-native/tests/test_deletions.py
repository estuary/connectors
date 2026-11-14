import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, AsyncGenerator, Callable

import pytest

from source_zendesk_support_native.api.deletions import (
    DELETION_LAG,
    fetch_deleted_users,
)
from source_zendesk_support_native.models import TimestampedResource

log = logging.getLogger(__name__)


def _user(id: int, updated_at: datetime) -> dict[str, Any]:
    return {"id": id, "updated_at": updated_at.strftime("%Y-%m-%dT%H:%M:%SZ"), "active": False}


def _page(users: list[dict[str, Any]], after_cursor: str | None) -> dict[str, Any]:
    return {
        "deleted_users": users,
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


async def _fetch_changes(http: MockHTTP, log_cursor: datetime) -> tuple[list[TimestampedResource | Any], datetime, datetime]:
    before = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)
    results = [
        r async for r in fetch_deleted_users(
            http,  # type: ignore[arg-type]
            "example", log, log_cursor,
        )
    ]
    after = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)
    return results, before, after


@pytest.mark.asyncio
async def test_emits_deletions_in_window() -> None:
    now = datetime.now(tz=UTC).replace(microsecond=0)
    log_cursor = now - timedelta(hours=2)

    http = MockHTTP(
        _page([_user(1, log_cursor), _user(2, log_cursor + timedelta(seconds=1))], after_cursor="c1"),
        _page([_user(3, now - timedelta(hours=1)), _user(4, now)], after_cursor=None),
    )

    results, before, after = await _fetch_changes(http, log_cursor)

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

    http = MockHTTP(_page([], after_cursor=None))

    results, before, after = await _fetch_changes(http, log_cursor)

    assert len(results) == 1
    assert before <= results[0] <= after
