import json
import logging
from datetime import UTC, datetime
from typing import Any

import pytest

from source_zendesk_support_native.api import fetch_incremental_cursor_paginated_resources
from source_zendesk_support_native.models import FilterParam, TicketMetricEventsResponse

log = logging.getLogger(__name__)

CURSOR = datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)


def _event(id: int, time: str) -> dict[str, Any]:
    return {
        "id": id,
        "ticket_id": id * 10,
        "metric": "reply_time",
        "instance_id": 1,
        "type": "fulfill",
        "time": time,
    }


class MockHTTP:
    """Minimal mock that queues JSON responses for http.request calls."""

    def __init__(self) -> None:
        self._queue: list[bytes] = []

    def queue(self, response: dict[str, Any]) -> None:
        self._queue.append(json.dumps(response).encode())

    async def request(self, log: Any, url: str, **kwargs: Any) -> bytes:
        return self._queue.pop(0)


async def _run(http: MockHTTP) -> tuple[set[int], datetime | None]:
    """Returns the yielded resource ids and the last yielded checkpoint."""
    ids: set[int] = set()
    checkpoint: datetime | None = None
    generator = fetch_incremental_cursor_paginated_resources(
        http,  # type: ignore[arg-type]
        "subdomain",
        "incremental/ticket_metric_events",
        FilterParam.START_TIME,
        "time",
        TicketMetricEventsResponse,
        log,
        CURSOR,
    )
    async for result in generator:
        if isinstance(result, datetime):
            checkpoint = result
        else:
            ids.add(result.id)
    return ids, checkpoint


@pytest.mark.asyncio
class TestFetchIncrementalCursorPaginatedResources:
    async def test_each_resource_evaluated_by_its_own_cursor_field(self):
        # start_time is inclusive: the first resource is a boundary duplicate.
        http = MockHTTP()
        http.queue({
            "ticket_metric_events": [
                _event(1, "2026-07-01T00:00:00Z"),
                _event(2, "2026-07-01T00:00:10Z"),
                _event(3, "2026-07-01T00:00:20Z"),
            ],
            "meta": {"has_more": True, "after_cursor": "cursor-page-2"},
        })
        http.queue({
            "ticket_metric_events": [_event(4, "2026-07-01T00:00:30Z")],
            "meta": {"has_more": False},
        })

        ids, checkpoint = await _run(http)

        assert ids == {2, 3, 4}
        assert checkpoint == datetime(2026, 7, 1, 0, 0, 30, tzinfo=UTC)

    async def test_cursor_advances_on_single_page_starting_at_the_cursor(self):
        http = MockHTTP()
        http.queue({
            "ticket_metric_events": [
                _event(1, "2026-07-01T00:00:00Z"),
                _event(2, "2026-07-01T00:00:10Z"),
            ],
            "meta": {"has_more": False},
        })

        ids, checkpoint = await _run(http)

        assert ids == {2}
        assert checkpoint == datetime(2026, 7, 1, 0, 0, 10, tzinfo=UTC)
