import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from source_zendesk_support_native.api import (
    _dt_to_s,
    _fetch_side_conversation_events,
    backfill_side_conversation_events,
    fetch_side_conversation_events,
)
from source_zendesk_support_native.models import SideConversationEvent

log = logging.getLogger(__name__)

T0 = datetime(2026, 5, 8, 14, 17, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _event(id: str, seconds: float) -> dict[str, Any]:
    created_at = T0 + timedelta(seconds=seconds)
    return {
        "id": id,
        "side_conversation_id": "sc-1",
        "ticket_id": 1,
        "type": "reply",
        "created_at": created_at.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
        "message": {"body": f"body of {id}"},
        "updates": {},
    }


def _page(
    events: list[dict[str, Any]], end_seconds: int, has_next: bool
) -> dict[str, Any]:
    end_time = _dt_to_s(T0) + end_seconds
    return {
        "events": events,
        "count": len(events),
        "end_time": end_time,
        "next_page": (
            f"https://example.zendesk.com/api/v2/tickets/side_conversations/events?start_time={end_time}"
            if has_next
            else None
        ),
    }


class MockStreamHTTP:
    """Minimal mock that queues JSON responses for http.request_stream calls."""

    def __init__(self) -> None:
        self._queue: list[bytes] = []
        self.start_times: list[int] = []

    def queue(self, response: dict[str, Any]) -> None:
        self._queue.append(json.dumps(response).encode())

    async def request_stream(
        self, log: Any, url: str, params: dict[str, Any] | None = None, **kwargs: Any
    ):
        assert params is not None
        self.start_times.append(params["start_time"])
        payload = self._queue.pop(0)

        async def body() -> AsyncGenerator[bytes, None]:
            yield payload

        return {}, body


@pytest.fixture(autouse=True)
def no_rate_limit_sleep():
    with patch(
        "source_zendesk_support_native.api.SIDE_CONVERSATION_EVENTS_REQ_PER_MIN_LIMIT",
        60_000_000,
    ):
        yield


def _ids(results: list[Any]) -> list[str]:
    return [r.id for r in results if isinstance(r, SideConversationEvent)]


# ---------------------------------------------------------------------------
# Tests for _fetch_side_conversation_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestFetchSideConversationEventsCore:
    async def test_skips_events_resent_from_the_boundary_second(self):
        http = MockStreamHTTP()
        # Page 1 ends inside second 1; page 2 starts at second 1 and re-sends e2 and e3.
        http.queue(
            _page(
                [_event("e1", 0.1), _event("e2", 1.2), _event("e3", 1.5)],
                end_seconds=1,
                has_next=True,
            )
        )
        http.queue(
            _page(
                [
                    _event("e2", 1.2),
                    _event("e3", 1.5),
                    _event("e4", 1.9),
                    _event("e5", 2.3),
                ],
                end_seconds=2,
                has_next=False,
            )
        )

        results = [
            r
            async for r in _fetch_side_conversation_events(
                http, "subdomain", T0, T0 + timedelta(minutes=1), log
            )
        ]

        assert _ids(results) == ["e1", "e2", "e3", "e4", "e5"]
        assert results[3] == T0 + timedelta(seconds=1), (
            "page 1's end_time is checkpointed before page 2's events"
        )
        assert http.start_times == [_dt_to_s(T0), _dt_to_s(T0) + 1]

    async def test_final_page_is_not_checkpointed(self):
        http = MockStreamHTTP()
        # The final page's end_time can run past its last event; it must not become a checkpoint.
        http.queue(_page([_event("e1", 0.1)], end_seconds=2500, has_next=False))

        results = [
            r
            async for r in _fetch_side_conversation_events(
                http, "subdomain", T0, T0 + timedelta(hours=1), log
            )
        ]

        assert _ids(results) == ["e1"]
        assert not any(isinstance(r, datetime) for r in results)

    async def test_stops_at_the_horizon(self):
        http = MockStreamHTTP()
        http.queue(
            _page(
                [_event("e1", 0.1), _event("e2", 4.0), _event("e3", 5.2)],
                end_seconds=5,
                has_next=True,
            )
        )

        results = [
            r
            async for r in _fetch_side_conversation_events(
                http, "subdomain", T0, T0 + timedelta(seconds=4), log
            )
        ]

        assert _ids(results) == ["e1"]
        assert not any(isinstance(r, datetime) for r in results)
        assert len(http.start_times) == 1, "no page past the horizon is requested"

    async def test_raises_when_a_full_second_does_not_fit_on_one_page(self):
        http = MockStreamHTTP()
        http.queue(
            _page([_event("e1", 0.1), _event("e2", 0.2)], end_seconds=0, has_next=True)
        )

        with pytest.raises(RuntimeError, match="cannot progress"):
            async for _ in _fetch_side_conversation_events(
                http, "subdomain", T0, T0 + timedelta(minutes=1), log
            ):
                pass


# ---------------------------------------------------------------------------
# Tests for fetch_side_conversation_events (incremental)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestFetchSideConversationEventsIncremental:
    async def test_ends_with_the_horizon_as_cursor(self):
        http = MockStreamHTTP()
        http.queue(_page([_event("e1", 0.1)], end_seconds=0, has_next=False))

        results = [
            r async for r in fetch_side_conversation_events(http, "subdomain", log, T0)
        ]

        assert _ids(results) == ["e1"]
        horizon = results[-1]
        assert isinstance(horizon, datetime)
        assert horizon.microsecond == 0
        assert T0 < horizon <= datetime.now(tz=UTC) - timedelta(minutes=5)

    async def test_returns_without_requesting_when_cursor_is_at_the_horizon(self):
        http = MockStreamHTTP()

        results = [
            r
            async for r in fetch_side_conversation_events(
                http, "subdomain", log, datetime.now(tz=UTC)
            )
        ]

        assert results == []
        assert http.start_times == []


# ---------------------------------------------------------------------------
# Tests for backfill_side_conversation_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestBackfillSideConversationEvents:
    async def test_yields_page_cursors_and_stops_before_cutoff(self):
        cutoff = T0 + timedelta(seconds=3)
        http = MockStreamHTTP()
        http.queue(
            _page([_event("e1", 0.1), _event("e2", 1.5)], end_seconds=1, has_next=True)
        )
        http.queue(
            _page(
                [_event("e2", 1.5), _event("e3", 2.9), _event("e4", 3.0)],
                end_seconds=3,
                has_next=True,
            )
        )

        results = [
            r
            async for r in backfill_side_conversation_events(
                http, "subdomain", T0, log, None, cutoff
            )
        ]

        assert _ids(results) == ["e1", "e2", "e3"]
        assert [r for r in results if isinstance(r, int)] == [_dt_to_s(T0) + 1]

    async def test_resumes_from_page_cursor(self):
        cutoff = T0 + timedelta(minutes=1)
        http = MockStreamHTTP()
        http.queue(_page([_event("e2", 1.5)], end_seconds=1, has_next=False))

        results = [
            r
            async for r in backfill_side_conversation_events(
                http, "subdomain", T0, log, _dt_to_s(T0) + 1, cutoff
            )
        ]

        assert _ids(results) == ["e2"]
        assert http.start_times == [_dt_to_s(T0) + 1]
