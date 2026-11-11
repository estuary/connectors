"""Unit tests for the live-unvalidated Claude Code usage report logic.

The org used for live verification had no Claude Code data, so the day-iterator,
freshness gate, and actor_id derivation are exercised here against a fake HTTP
session instead of the real API.
"""

import json
import logging
from datetime import UTC, datetime

import pytest

from source_claude_admin_api import api
from source_claude_admin_api.api import fetch_cursor_list, fetch_singleton
from source_claude_admin_api.models import ClaudeCodeUsageRecord, Organization, User

log = logging.getLogger()


def _record(day: str, actor: dict) -> dict:
    return {
        "date": f"{day}T00:00:00Z",
        "organization_id": "org_1",
        "actor": actor,
    }


class FakeHTTP:
    """Returns canned usage-report envelopes keyed by (starting_at, page)."""

    def __init__(self, pages_by_day: dict[str, list[dict]]):
        # pages_by_day: {day: [envelope, envelope, ...]} — one entry per page.
        self.pages_by_day = pages_by_day
        self.requests: list[dict] = []

    async def request(self, log, url, params=None, headers=None, **kwargs):
        params = params or {}
        self.requests.append(params)
        assert headers == api.ANTHROPIC_VERSION_HEADERS  # version header always sent
        day = params["starting_at"]
        pages = self.pages_by_day.get(day, [{"data": [], "has_more": False, "next_page": None}])
        page_token = params.get("page")
        idx = 0 if page_token is None else int(page_token)
        return json.dumps(pages[idx]).encode()


# --- actor_id derivation ---


@pytest.mark.parametrize(
    "actor, expected",
    [
        ({"type": "user_actor", "email_address": "a@x.com"}, "a@x.com"),
        ({"type": "api_actor", "api_key_name": "ci-key"}, "ci-key"),
        ({}, "unknown"),
    ],
)
def test_actor_id_known_shapes(actor, expected):
    rec = ClaudeCodeUsageRecord.model_validate(_record("2026-06-05", actor))
    assert rec.actor_id == expected


def test_actor_id_unknown_shape_does_not_collapse():
    a = ClaudeCodeUsageRecord.model_validate(_record("2026-06-05", {"type": "weird", "x": 1}))
    b = ClaudeCodeUsageRecord.model_validate(_record("2026-06-05", {"type": "weird", "x": 2}))
    assert a.actor_id.startswith("weird:")
    assert a.actor_id != b.actor_id  # distinct actors get distinct keys
    # And it is present in the emitted document (computed fields survive serialization).
    assert "actor_id" in a.model_dump()


def test_actor_id_in_serialized_json():
    rec = ClaudeCodeUsageRecord.model_validate(
        _record("2026-06-05", {"type": "user_actor", "email_address": "a@x.com"})
    )
    assert json.loads(rec.model_dump_json())["actor_id"] == "a@x.com"


# --- day iterator + freshness gate ---


async def _drain(gen):
    records, cursors = [], []
    async for item in gen:
        (cursors if isinstance(item, datetime) else records).append(item)
    return records, cursors


@pytest.fixture
def frozen_now(monkeypatch):
    # Fixed "now" so the freshness gate is deterministic.
    monkeypatch.setattr(api, "now_utc", lambda: datetime(2026, 6, 10, 12, 0, tzinfo=UTC))


async def test_day_iteration_advances_and_stops_at_freshness_floor(frozen_now):
    # Days are "ready" when now >= midnight(day)+1d+2h. With now=06-10T12:00,
    # days 06-05..06-09 are ready; 06-10 is not.
    http = FakeHTTP(
        {
            "2026-06-05": [
                {"data": [_record("2026-06-05", {"type": "user_actor", "email_address": "a@x.com"})],
                 "has_more": False, "next_page": None}
            ],
            # 06-06 spans two pages to exercise the opaque page cursor.
            "2026-06-06": [
                {"data": [_record("2026-06-06", {"type": "api_actor", "api_key_name": "k1"})],
                 "has_more": True, "next_page": "1"},
                {"data": [_record("2026-06-06", {"type": "user_actor", "email_address": "b@x.com"})],
                 "has_more": False, "next_page": None},
            ],
        }
    )
    cursor = datetime(2026, 6, 5, 0, 0, tzinfo=UTC)
    records, cursors = await _drain(api.fetch_claude_code_usage(http, "https://api", log, cursor))

    # 3 records captured across the populated days; empty days emit none.
    assert len(records) == 3
    # One checkpoint per drained day, advancing one day each, strictly increasing.
    assert cursors == [
        datetime(2026, 6, d, 0, 0, tzinfo=UTC) for d in (6, 7, 8, 9, 10)
    ]
    assert cursors == sorted(cursors)
    # Never requested the incomplete day 06-10.
    assert all(p["starting_at"] != "2026-06-10" for p in http.requests)
    # The two-page day issued a second request carrying the opaque page cursor.
    assert {"starting_at": "2026-06-06", "limit": ClaudeCodeUsageRecord.page_limit, "page": "1"} in http.requests


async def test_no_ready_days_yields_nothing(frozen_now):
    # Cursor is "today" (not yet complete) — nothing is ready, cursor unchanged.
    http = FakeHTTP({})
    cursor = datetime(2026, 6, 10, 0, 0, tzinfo=UTC)
    records, cursors = await _drain(api.fetch_claude_code_usage(http, "https://api", log, cursor))
    assert records == []
    assert cursors == []
    assert http.requests == []  # no API calls made
