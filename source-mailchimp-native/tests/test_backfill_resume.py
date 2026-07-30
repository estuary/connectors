"""Offline tests for backfill resume semantics (no live API).

These pin the value-watermark resume contract of `backfill_list_children` and
`backfill_campaigns` against a canned Mailchimp emulation: `since_*` filters
exclusively, `before_*` inclusively, and pages slice a sorted (or, for
segments, insertion-ordered) row store. The regression case is the one that
motivated the design: a row leaving the frozen window mid-backfill renumbers
the tail, so a positional resume would skip an untouched row.
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
from source_mailchimp_native.models import Campaign, ListMember, Segment

LOG = logging.getLogger("test")

BASE = datetime(2026, 1, 1, tzinfo=UTC)
START_DATE = BASE - timedelta(days=1)
CUTOFF = BASE + timedelta(days=1)


class FakeMailchimp:
    """Serves one collection endpoint from an in-memory row store.

    Rows are dicts; `cursor_field` names the timestamp the `since_*`/`before_*`
    params filter on (exclusive / inclusive respectively, matching the probed
    boundary semantics). Honors `sort_field`/`sort_dir=ASC` when requested;
    otherwise serves rows in store order, like the segments endpoint.
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


def member_row(i: int, last_changed: datetime) -> dict:
    return {
        "id": f"m{i:04}",
        "list_id": "l1",
        "email_address": f"m{i:04}@example.com",
        "last_changed": last_changed.isoformat(),
        "status": "subscribed",
    }


def make_members(n: int) -> list[dict]:
    return [member_row(i, BASE + timedelta(seconds=i)) for i in range(n)]


MEMBER_SORT: dict[str, str | int] = {"sort_field": "last_changed", "sort_dir": "ASC"}


def drain_members(server: FakeMailchimp, page, *, stop_after_checkpoints=None):
    """Run one backfill invocation; return (doc ids, checkpoints yielded).

    `stop_after_checkpoints` closes the generator right after the Nth
    checkpoint, emulating a crash whose resume starts from that checkpoint.
    """

    async def run():
        ids, checkpoints = [], []
        gen = backfill_list_children(
            server,  # type: ignore[arg-type]
            "https://test",
            ListMember,
            "l1",
            MEMBER_SORT,
            True,
            START_DATE,
            LOG,
            page,
            CUTOFF,
        )
        async for out in gen:
            if isinstance(out, str):
                checkpoints.append(out)
                if stop_after_checkpoints and len(checkpoints) == stop_after_checkpoints:
                    await gen.aclose()
                    break
            else:
                ids.append(out.id)
        return ids, checkpoints

    return asyncio.run(run())


def test_members_backfill_checkpoints_watermark_not_offset():
    server = FakeMailchimp("members", "last_changed", make_members(2500))
    ids, checkpoints = drain_members(server, None)

    assert len(ids) == 2500 and len(set(ids)) == 2500
    # One watermark per full page, none after the short final page, and each
    # is the max last_changed of the rows walked so far — never an offset.
    assert checkpoints == [
        (BASE + timedelta(seconds=999)).isoformat(),
        (BASE + timedelta(seconds=1999)).isoformat(),
    ]
    # Offsets advance only within the invocation, anchored to one since value.
    assert [(p["offset"], p["since_last_changed"]) for p in server.requests] == [
        (i * MAX_PAGE_SIZE, START_DATE.isoformat()) for i in range(3)
    ]


def test_members_resume_reanchors_since_at_watermark_minus_1s():
    server = FakeMailchimp("members", "last_changed", make_members(2500))
    watermark = BASE + timedelta(seconds=999)

    ids, _ = drain_members(server, watermark.isoformat())

    # since is exclusive at watermark − 1s, so the boundary second re-reads:
    # rows 999.. return, duplicates collapsing under the collection key.
    first = server.requests[0]
    assert first["offset"] == 0
    assert first["since_last_changed"] == (watermark - timedelta(seconds=1)).isoformat()
    assert ids[0] == "m0999" and ids[-1] == "m2499"


def test_members_row_leaving_window_mid_backfill_skips_nothing():
    """The C1 regression: a member updated after a crash's last checkpoint
    leaves the frozen window and renumbers the tail. A positional resume
    (the old `offset + count` cursor) would skip the row that slid across
    the boundary; the watermark resume must re-emit it."""
    server = FakeMailchimp("members", "last_changed", make_members(2500))

    first_ids, checkpoints = drain_members(server, None, stop_after_checkpoints=1)
    assert len(first_ids) == 1000

    # Mid-backfill churn inside the already-walked prefix: the update stamps
    # a cursor past the cutoff, so the row exits the window server-side.
    server.rows[100]["last_changed"] = (CUTOFF + timedelta(hours=1)).isoformat()

    resumed_ids, _ = drain_members(server, checkpoints[0])

    # Every row appears in one run or the other: the churned row was already
    # emitted pre-crash (its post-churn version belongs to the incremental
    # task), and nothing else may go missing.
    assert set(first_ids) | set(resumed_ids) == {f"m{i:04}" for i in range(2500)}
    assert "m0100" not in resumed_ids  # left the window server-side
    # In particular the innocent bystander a stale offset would have skipped:
    assert "m1000" in resumed_ids


def test_segments_backfill_never_checkpoints():
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
        outs = [
            out
            async for out in backfill_list_children(
                server,  # type: ignore[arg-type]
                "https://test",
                Segment,
                "l1",
                {},
                False,
                START_DATE,
                LOG,
                None,
                CUTOFF,
            )
        ]
        return outs

    outs = asyncio.run(run())

    # No sort on the endpoint means no order survives a resume gap, so the
    # whole window drains in one invocation with zero cross-invocation state.
    assert not any(isinstance(out, str) for out in outs)
    assert sorted(out.id for out in outs) == list(range(1500))


def test_campaigns_backfill_checkpoints_watermark():
    rows = [
        {"id": f"c{i:04}", "create_time": (BASE + timedelta(seconds=i)).isoformat()}
        for i in range(1500)
    ]
    server = FakeMailchimp("campaigns", "create_time", rows)

    async def run(page):
        ids, checkpoints = [], []
        async for out in backfill_campaigns(
            server,  # type: ignore[arg-type]
            "https://test",
            START_DATE,
            LOG,
            page,
            CUTOFF,
        ):
            (checkpoints if isinstance(out, str) else ids).append(
                out if isinstance(out, str) else out.id
            )
        return ids, checkpoints

    ids, checkpoints = asyncio.run(run(None))
    assert len(ids) == 1500
    assert checkpoints == [(BASE + timedelta(seconds=999)).isoformat()]

    # A resume re-anchors since_create_time one second before the watermark.
    server.requests.clear()
    resumed_ids, _ = asyncio.run(run(checkpoints[0]))
    assert server.requests[0]["offset"] == 0
    assert (
        server.requests[0]["since_create_time"]
        == (BASE + timedelta(seconds=998)).isoformat()
    )
    assert resumed_ids[0] == "c0999"
