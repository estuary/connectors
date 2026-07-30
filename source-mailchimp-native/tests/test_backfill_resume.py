"""Offline tests for backfill checkpoint semantics (no live API).

A backfill window is frozen, but rows *leave* it mid-walk (an update or
deletion), renumbering every later row — so a positional offset must never
cross a checkpoint: a resumed offset would skip an untouched row that the
incremental task never revisits. These tests pin the resulting contract:
backfills drain their whole window in a single invocation and yield no
PageCursor at all, keeping offsets local to that invocation.

The canned server emulates the probed boundary semantics: `since_*` filters
exclusively, `before_*` inclusively.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

from source_mailchimp_native.api.shared import (
    MAX_PAGE_SIZE,
    backfill_campaigns,
    backfill_list_children,
)
from source_mailchimp_native.models import ListMember, Segment

LOG = logging.getLogger("test")

BASE = datetime(2026, 1, 1, tzinfo=UTC)
START_DATE = BASE - timedelta(days=1)
CUTOFF = BASE + timedelta(days=1)


class FakeMailchimp:
    """Serves one collection endpoint from an in-memory row store.

    `cursor_field` names the timestamp the `since_*`/`before_*` params filter
    on (exclusive / inclusive respectively, matching the probed boundary
    semantics). Honors `sort_field`/`sort_dir=ASC` when requested; otherwise
    serves rows in store order, like the segments endpoint.
    """

    def __init__(self, items_key: str, cursor_field: str, rows: list[dict]):
        self.items_key = items_key
        self.cursor_field = cursor_field
        self.rows = rows
        self.requests: list[dict] = []

    async def request_stream(self, log, url, method="GET", params=None, **kwargs):
        params = dict(params or {})
        self.requests.append(params)

        rows = self.rows
        for param, keep in [
            (f"since_{self.cursor_field}", lambda c, bound: c > bound),
            (f"before_{self.cursor_field}", lambda c, bound: c <= bound),
        ]:
            if param in params:
                bound = datetime.fromisoformat(str(params[param]))
                rows = [
                    r
                    for r in rows
                    if keep(datetime.fromisoformat(r[self.cursor_field]), bound)
                ]

        if params.get("sort_field"):
            assert params.get("sort_dir") == "ASC"
            rows = sorted(rows, key=lambda r: r[params["sort_field"]])

        offset, count = int(params["offset"]), int(params["count"])
        body_bytes = json.dumps(
            {self.items_key: rows[offset : offset + count], "total_items": len(rows)}
        ).encode()

        async def body():
            yield body_bytes

        return ({}, body)


def test_members_backfill_drains_window_and_never_checkpoints():
    rows = [
        {
            "id": f"m{i:04}",
            "list_id": "l1",
            "email_address": f"m{i:04}@example.com",
            "last_changed": (BASE + timedelta(seconds=i)).isoformat(),
            "status": "subscribed",
        }
        for i in range(2500)
    ]
    server = FakeMailchimp("members", "last_changed", rows)

    async def run():
        return [
            out
            async for out in backfill_list_children(
                server,  # type: ignore[arg-type]
                "https://test",
                ListMember,
                "l1",
                {"sort_field": "last_changed", "sort_dir": "ASC"},
                START_DATE,
                LOG,
                None,
                CUTOFF,
            )
        ]

    outs = asyncio.run(run())

    assert [out.id for out in outs] == [f"m{i:04}" for i in range(2500)]
    assert not any(isinstance(out, (str, int, dict)) for out in outs)
    # Offsets stay local to the single invocation, over one frozen window.
    assert [
        (p["offset"], p["since_last_changed"], p["before_last_changed"])
        for p in server.requests
    ] == [
        (
            i * MAX_PAGE_SIZE,
            START_DATE.isoformat(),
            (CUTOFF - timedelta(seconds=1)).isoformat(),
        )
        for i in range(3)
    ]


def test_segments_backfill_drains_window_and_never_checkpoints():
    rows = [
        {
            "id": i,
            "list_id": "l1",
            "updated_at": (BASE + timedelta(seconds=i)).isoformat(),
        }
        for i in range(1500)
    ]
    server = FakeMailchimp("segments", "updated_at", rows)

    async def run():
        return [
            out
            async for out in backfill_list_children(
                server,  # type: ignore[arg-type]
                "https://test",
                Segment,
                "l1",
                {},
                START_DATE,
                LOG,
                None,
                CUTOFF,
            )
        ]

    outs = asyncio.run(run())

    assert sorted(out.id for out in outs) == list(range(1500))
    assert not any(isinstance(out, (str, int, dict)) for out in outs)


def test_campaigns_backfill_drains_window_and_never_checkpoints():
    rows = [
        {"id": f"c{i:04}", "create_time": (BASE + timedelta(seconds=i)).isoformat()}
        for i in range(1500)
    ]
    server = FakeMailchimp("campaigns", "create_time", rows)

    async def run():
        return [
            out
            async for out in backfill_campaigns(
                server,  # type: ignore[arg-type]
                "https://test",
                START_DATE,
                LOG,
                None,
                CUTOFF,
            )
        ]

    outs = asyncio.run(run())

    assert [out.id for out in outs] == [f"c{i:04}" for i in range(1500)]
    assert not any(isinstance(out, (str, int, dict)) for out in outs)
    assert [p["offset"] for p in server.requests] == [0, 1000]
