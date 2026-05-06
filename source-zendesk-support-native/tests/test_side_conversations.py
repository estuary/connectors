import json
import logging
from datetime import datetime, UTC
from typing import Any, AsyncGenerator
from unittest.mock import patch, MagicMock

import pytest

from estuary_cdk.http import HTTPError
from source_zendesk_support_native.api import (
    _fetch_side_conversations,
    fetch_side_conversations,
    backfill_side_conversations,
    _dt_to_s,
)
from source_zendesk_support_native.models import SideConversation, TimestampedResource
from source_zendesk_support_native.resources import _is_side_conversations_enabled

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _side_conv(id: str, ticket_id: int = 1) -> dict[str, Any]:
    return {
        "id": id,
        "ticket_id": ticket_id,
        "url": f"https://example.zendesk.com/api/v2/tickets/{ticket_id}/side_conversations/{id}",
        "subject": "Test conversation",
        "state": "open",
        "preview_text": "Some preview text",
        "participants": [],
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "message_added_at": "2026-01-01T00:00:00Z",
        "state_updated_at": "2026-01-01T00:00:00Z",
    }


def _ticket(id: int, updated_at: str = "2026-01-01T00:00:00Z", status: str = "open") -> TimestampedResource:
    return TimestampedResource.model_validate({
        "id": id,
        "status": status,
        "updated_at": updated_at,
    })


async def _async_iter(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


class MockHTTP:
    """Minimal mock that queues JSON responses for http.request calls."""

    def __init__(self) -> None:
        self._queue: list[bytes] = []
        self.urls: list[str] = []

    def queue(self, response: dict[str, Any]) -> None:
        self._queue.append(json.dumps(response).encode())

    def queue_error(self, code: int) -> None:
        self._queue.append(HTTPError(f"HTTP {code}", code))  # type: ignore[arg-type]

    async def request(self, log: Any, url: str, **kwargs: Any) -> bytes:
        self.urls.append(url)
        item = self._queue.pop(0)
        if isinstance(item, HTTPError):
            raise item
        return item


# ---------------------------------------------------------------------------
# Tests for _fetch_side_conversations (the per-ticket fetcher)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchSideConversationsPerTicket:
    async def test_single_page_returns_all_records(self):
        http = MockHTTP()
        http.queue({"side_conversations": [_side_conv("uuid-1"), _side_conv("uuid-2")], "next_page": None})

        results = [r async for r in _fetch_side_conversations(http, "subdomain", 123, log)]

        assert len(results) == 2
        assert results[0].id == "uuid-1"
        assert results[1].id == "uuid-2"

    async def test_follows_next_page_url(self):
        page2_url = "https://example.zendesk.com/api/v2/tickets/123/side_conversations?page=2"
        http = MockHTTP()
        http.queue({"side_conversations": [_side_conv("uuid-1")], "next_page": page2_url})
        http.queue({"side_conversations": [_side_conv("uuid-2")], "next_page": None})

        results = [r async for r in _fetch_side_conversations(http, "subdomain", 123, log)]

        assert len(results) == 2
        assert http.urls[1] == page2_url, "second request should follow next_page URL verbatim"

    @pytest.mark.parametrize("code", [403, 404, 500])
    async def test_http_errors_propagate(self, code: int):
        http = MockHTTP()
        http.queue_error(code)

        with pytest.raises(HTTPError) as exc_info:
            async for _ in _fetch_side_conversations(http, "subdomain", 123, log):
                pass

        assert exc_info.value.code == code

    async def test_empty_response_yields_nothing(self):
        http = MockHTTP()
        http.queue({"side_conversations": [], "next_page": None})

        results = [r async for r in _fetch_side_conversations(http, "subdomain", 123, log)]

        assert results == []


# ---------------------------------------------------------------------------
# Tests for fetch_side_conversations (incremental, ticket-cursor based)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchSideConversationsIncremental:
    async def test_yields_side_conversations_for_updated_tickets(self):
        start_time = _dt_to_s(datetime(2026, 1, 1, tzinfo=UTC))
        cursor = (str(start_time),)
        next_page = "some-export-cursor"

        http = MockHTTP()
        http.queue({"side_conversations": [_side_conv("sc-1", 101)], "next_page": None})
        http.queue({"side_conversations": [_side_conv("sc-2", 102)], "next_page": None})

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            mock_export.return_value = _async_iter([_ticket(101), _ticket(102), next_page])

            results = [r async for r in fetch_side_conversations(http, "subdomain", log, cursor)]

        side_convs = [r for r in results if isinstance(r, SideConversation)]
        cursors = [r for r in results if isinstance(r, tuple)]

        assert [sc.id for sc in side_convs] == ["sc-1", "sc-2"]
        assert cursors == [(next_page,)]

    async def test_skips_deleted_tickets(self):
        start_time = _dt_to_s(datetime(2026, 1, 1, tzinfo=UTC))
        cursor = (str(start_time),)
        next_page = "some-export-cursor"

        http = MockHTTP()
        # Only ticket 102 should be fetched (101 is deleted)
        http.queue({"side_conversations": [_side_conv("sc-2", 102)], "next_page": None})

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            mock_export.return_value = _async_iter([
                _ticket(101, status="deleted"),
                _ticket(102),
                next_page,
            ])

            results = [r async for r in fetch_side_conversations(http, "subdomain", log, cursor)]

        side_convs = [r for r in results if isinstance(r, SideConversation)]
        assert len(side_convs) == 1
        assert side_convs[0].id == "sc-2"
        assert len(http.urls) == 1, "should only have fetched side conversations for one ticket"

    async def test_no_tickets_yields_nothing(self):
        start_time = _dt_to_s(datetime(2026, 1, 1, tzinfo=UTC))
        cursor = (str(start_time),)

        http = MockHTTP()

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            # No next_page cursor means generator completes without yielding
            mock_export.return_value = _async_iter([])

            results = [r async for r in fetch_side_conversations(http, "subdomain", log, cursor)]

        assert results == []


# ---------------------------------------------------------------------------
# Tests for backfill_side_conversations
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestBackfillSideConversations:
    async def test_yields_records_and_page_cursor_before_cutoff(self):
        cutoff = datetime(2026, 6, 1, tzinfo=UTC)
        start_date = datetime(2025, 1, 1, tzinfo=UTC)
        next_page = "page-cursor-2"

        http = MockHTTP()
        http.queue({"side_conversations": [_side_conv("sc-1", 101)], "next_page": None})
        http.queue({"side_conversations": [_side_conv("sc-2", 102)], "next_page": None})

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            mock_export.return_value = _async_iter([
                _ticket(101, updated_at="2026-01-01T00:00:00Z"),
                _ticket(102, updated_at="2026-02-01T00:00:00Z"),
                next_page,
            ])

            results = [r async for r in backfill_side_conversations(
                http, "subdomain", start_date, log, None, cutoff
            )]

        side_convs = [r for r in results if isinstance(r, SideConversation)]
        page_cursors = [r for r in results if isinstance(r, str)]

        assert [sc.id for sc in side_convs] == ["sc-1", "sc-2"]
        assert page_cursors == [next_page]

    async def test_stops_when_ticket_reaches_cutoff(self):
        cutoff = datetime(2026, 1, 15, tzinfo=UTC)
        start_date = datetime(2025, 1, 1, tzinfo=UTC)
        next_page = "page-cursor-2"

        http = MockHTTP()
        # Only ticket 101 (before cutoff) should be fetched
        http.queue({"side_conversations": [_side_conv("sc-1", 101)], "next_page": None})

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            mock_export.return_value = _async_iter([
                _ticket(101, updated_at="2026-01-01T00:00:00Z"),   # before cutoff
                _ticket(102, updated_at="2026-02-01T00:00:00Z"),   # at/after cutoff — stop
                next_page,
            ])

            results = [r async for r in backfill_side_conversations(
                http, "subdomain", start_date, log, None, cutoff
            )]

        side_convs = [r for r in results if isinstance(r, SideConversation)]
        assert len(side_convs) == 1
        assert side_convs[0].id == "sc-1"

    async def test_skips_deleted_tickets_in_backfill(self):
        cutoff = datetime(2026, 6, 1, tzinfo=UTC)
        start_date = datetime(2025, 1, 1, tzinfo=UTC)
        next_page = "page-cursor-2"

        http = MockHTTP()
        http.queue({"side_conversations": [_side_conv("sc-2", 102)], "next_page": None})

        with patch("source_zendesk_support_native.api._fetch_incremental_cursor_export_resources") as mock_export:
            mock_export.return_value = _async_iter([
                _ticket(101, status="deleted"),
                _ticket(102),
                next_page,
            ])

            results = [r async for r in backfill_side_conversations(
                http, "subdomain", start_date, log, None, cutoff
            )]

        side_convs = [r for r in results if isinstance(r, SideConversation)]
        assert len(side_convs) == 1
        assert side_convs[0].id == "sc-2"
        assert len(http.urls) == 1


# ---------------------------------------------------------------------------
# Tests for _is_side_conversations_enabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestIsSideConversationsEnabled:
    def _config(self, subdomain: str = "example") -> Any:
        config = MagicMock()
        config.subdomain = subdomain
        return config

    async def test_returns_true_when_show_in_context_panel_is_true(self):
        http = MockHTTP()
        http.queue({"settings": {"side_conversations": {"show_in_context_panel": True}}})

        result = await _is_side_conversations_enabled(log, http, self._config())

        assert result is True

    async def test_returns_false_when_show_in_context_panel_is_false(self):
        http = MockHTTP()
        http.queue({"settings": {"side_conversations": {"show_in_context_panel": False}}})

        result = await _is_side_conversations_enabled(log, http, self._config())

        assert result is False

    async def test_returns_false_when_side_conversations_key_missing(self):
        http = MockHTTP()
        http.queue({"settings": {}})

        result = await _is_side_conversations_enabled(log, http, self._config())

        assert result is False

    async def test_reraises_http_errors(self):
        http = MockHTTP()
        http.queue_error(403)

        with pytest.raises(HTTPError) as exc_info:
            await _is_side_conversations_enabled(log, http, self._config())

        assert exc_info.value.code == 403
