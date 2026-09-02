"""Unit tests for the chunked delayed-stream path.

These cover the pieces that make delayed streams checkpoint progress within a
large window:
  - fetch_search_objects returning a fully-read chunk plus a resume cursor at the
    Search API's 10k offset boundary,
  - fetch_chunked_changes_with_associations yielding the millisecond before
    each page's resume cursor as the intermediate checkpoint, and
  - fetch_delayed_changes treating an interleaved bare datetime as an
    intermediate, strictly-increasing checkpoint.
"""

import json
from datetime import datetime, timedelta, UTC
from logging import getLogger
from typing import Any

import pytest

import estuary_cdk.emitted_changes_cache as cache
from source_hubspot_native.api.object_with_associations import (
    fetch_changes_with_associations,
    fetch_chunked_changes_with_associations,
)
from source_hubspot_native.api.properties import properties_cache
from source_hubspot_native.api.search_objects import fetch_search_objects
from source_hubspot_native.api.shared import (
    MAX_DELAYED_WINDOW,
    dt_to_str,
    fetch_delayed_changes,
)
from source_hubspot_native.models import Product, TimestampedId

log = getLogger("test_search_objects")


class FakeHTTP:
    """Returns canned response bodies in FIFO order, recording each request."""

    def __init__(self, responses: list[bytes]):
        self._responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    async def request(
        self, log, url, method="GET", params=None, json=None, **kwargs
    ) -> bytes:
        self.requests.append({"url": url, "method": method, "json": json})
        assert self._responses, f"unexpected request: {json}"
        return self._responses.pop(0)


def _search_page(items: list[tuple[datetime, int]], next_after: str | None) -> bytes:
    """Build a SearchPageResult[CustomObjectSearchResult] JSON body."""
    body: dict[str, Any] = {
        "total": len(items),
        "results": [
            {"id": id, "properties": {"hs_lastmodifieddate": ts.isoformat()}}
            for ts, id in items
        ],
    }
    if next_after is not None:
        body["paging"] = {"next": {"after": next_after}}
    return json.dumps(body).encode()


@pytest.mark.asyncio
async def test_fetch_search_objects_fully_read_chunk_and_inclusive_resume():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    t1, t2, t3 = base, base + timedelta(seconds=1), base + timedelta(seconds=2)
    since = base - timedelta(seconds=10)

    http = FakeHTTP(
        [
            # First page hits the offset cap (after=10000). t3 is the in-progress
            # millisecond and may have more records past the cap, so it's
            # withheld; the chunk covers only up to t2.
            _search_page([(t1, 1), (t2, 2), (t3, 3)], next_after="10000"),
            # Resuming from the cursor re-searches GTE t3 and finds the rest.
            _search_page([(t3, 3), (t3 + timedelta(seconds=1), 4)], next_after=None),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, since, None, None)
    assert items == [(t1, "1"), (t2, "2")]  # ascending, t3 dropped
    assert page == dt_to_str(t3)

    items2, page2 = await fetch_search_objects("deals", log, http, since, None, page)
    # The resume search starts at the cursor inclusively, re-reading the
    # withheld t3 instant.
    assert http.requests[1]["json"]["filters"][0]["value"] == dt_to_str(t3)
    assert page2 is None
    assert set(items2) == {(t3, "3"), (t3 + timedelta(seconds=1), "4")}


@pytest.mark.asyncio
async def test_fetch_search_objects_drains_dense_millisecond_cycle():
    instant = datetime(2026, 1, 1, tzinfo=UTC)
    since = instant  # every record shares `since`'s millisecond

    http = FakeHTTP(
        [
            # The whole capped page sits at a single instant, so advancing by
            # timestamp can't make progress: a cycle.
            _search_page([(instant, i) for i in range(1, 6)], next_after="10000"),
            # The modified-at drain returns every record at that instant, in
            # ascending hs_object_id order, with no further pages.
            _search_page([(instant, i) for i in range(1, 9)], next_after=None),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, since, None, None)

    # All ids at the instant are returned, and the cursor steps one ms past it.
    assert {id for _, id in items} == {str(i) for i in range(1, 9)}
    assert page == dt_to_str(instant + timedelta(milliseconds=1))


def _properties_page(names: list[str]) -> bytes:
    """Build a Properties JSON body."""
    return json.dumps(
        {"results": [{"name": n, "type": "string"} for n in names]}
    ).encode()


def _batch_page(items: list[tuple[datetime, int]]) -> bytes:
    """Build a BatchResult[Product] JSON body echoing the requested ids."""
    return json.dumps(
        {
            "status": "COMPLETE",
            "results": [
                {
                    "id": id,
                    "createdAt": ts.isoformat(),
                    "updatedAt": ts.isoformat(),
                    "archived": False,
                    "properties": {},
                }
                for ts, id in items
            ],
            "startedAt": "2026-01-01T00:00:00+00:00",
            "completedAt": "2026-01-01T00:00:00+00:00",
        }
    ).encode()


@pytest.mark.asyncio
async def test_fetch_chunked_changes_checkpoints_at_the_page_resume_boundary():
    since = datetime(2026, 1, 1, tzinfo=UTC)
    t1, t2, t3 = (since + timedelta(minutes=i) for i in (1, 2, 3))
    ms = timedelta(milliseconds=1)

    # A page's cursor is the first millisecond it does not cover. IDs are
    # deliberately out of order within the chunk since the chunked layer is
    # order-agnostic.
    pages = [
        ([TimestampedId(t2, "2"), TimestampedId(t1, "1")], dt_to_str(t2 + ms)),
        ([], dt_to_str(t2 + 2 * ms)),  # empty non-final chunk
        ([TimestampedId(t3, "3")], None),  # final page
    ]

    async def fake_fetcher(page: Any, count: int) -> Any:
        return pages.pop(0)

    # Product has no associated entities, so each non-empty chunk costs exactly
    # one batch-read request (plus one properties fetch, cached after the first).
    properties_cache.pop("products", None)
    http = FakeHTTP(
        [
            _properties_page(["hs_lastmodifieddate"]),
            _batch_page([(t1, 1), (t2, 2)]),
            _batch_page([(t3, 3)]),
        ]
    )

    out = []
    async for item in fetch_chunked_changes_with_associations(
        "products", Product, fake_fetcher, log, http, False, since, None
    ):
        out.append(item)

    checkpoints = [x for x in out if isinstance(x, datetime)]
    docs = [(ts, id) for x in out if not isinstance(x, datetime) for ts, id, _ in [x]]

    # Documents are emitted oldest-first within each chunk.
    assert docs == [(t1, "1"), (t2, "2"), (t3, "3")]
    # Each non-final page checkpoints at the millisecond before its cursor,
    # including the empty one, since the cursor alone says what has been
    # covered. The final page's checkpoint is the window edge emitted by
    # fetch_delayed_changes instead.
    assert checkpoints == [t2, t2 + ms]
    # The first checkpoint lands between the first chunk's documents and the rest.
    assert out.index(t2) == 2


@pytest.mark.asyncio
async def test_fetch_chunked_changes_emits_every_id_the_fetcher_returns():
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = since + timedelta(hours=1)
    below, inside, above = since - timedelta(minutes=1), since + timedelta(minutes=1), until + timedelta(minutes=1)

    pages = [
        ([TimestampedId(below, "0"), TimestampedId(inside, "1"), TimestampedId(above, "9")], None),
    ]

    async def fake_fetcher(page: Any, count: int) -> Any:
        return pages.pop(0)

    properties_cache.pop("products", None)
    http = FakeHTTP(
        [
            _properties_page(["hs_lastmodifieddate"]),
            _batch_page([(below, 0), (inside, 1), (above, 9)]),
        ]
    )

    docs = []
    async for item in fetch_chunked_changes_with_associations(
        "products", Product, fake_fetcher, log, http, False, since, until
    ):
        if not isinstance(item, datetime):
            docs.append((item[0], item[1]))

    # The fetcher's window decides membership; timestamps outside it are payload.
    assert docs == [(below, "0"), (inside, "1"), (above, "9")]


@pytest.mark.asyncio
async def test_fetch_changes_with_associations_filters_to_the_window():
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = since + timedelta(hours=1)
    below, inside, above = since, since + timedelta(minutes=1), until + timedelta(minutes=1)

    # A legacy recents-style fetcher overruns `since` on its last page and has
    # no server-side `until`.
    pages = [
        ([TimestampedId(above, "9"), TimestampedId(inside, "1"), TimestampedId(below, "0")], None),
    ]

    async def fake_fetcher(page: Any, count: int) -> Any:
        return pages.pop(0)

    properties_cache.pop("products", None)
    http = FakeHTTP(
        [
            _properties_page(["hs_lastmodifieddate"]),
            _batch_page([(inside, 1)]),
        ]
    )

    docs = []
    async for ts, id, _ in fetch_changes_with_associations(
        "products", Product, fake_fetcher, log, http, False, since, until
    ):
        docs.append((ts, id))

    assert docs == [(inside, "1")]
    # Only the in-window id was batch-read.
    assert http.requests[1]["json"]["inputs"] == [{"id": "1"}]


def _make_delayed(items: list[Any]):
    async def fake_delayed(log, http, with_history, since, until):
        for item in items:
            yield item

    return fake_delayed


@pytest.mark.asyncio
async def test_fetch_delayed_changes_checkpoints_at_boundary_datetimes():
    object_name = "test_delayed_checkpoints"
    cache.emitted_cache.pop(object_name, None)

    # Window is [lower_bound, lower_bound + 1h]; keep every timestamp inside it.
    lower_bound = datetime.now(UTC) - timedelta(hours=2)
    ta = lower_bound + timedelta(minutes=5)
    c1 = lower_bound + timedelta(minutes=10)
    tb = lower_bound + timedelta(minutes=15)
    c2 = lower_bound + timedelta(minutes=20)
    tc = lower_bound + timedelta(minutes=25)

    fake_delayed = _make_delayed(
        [(ta, "k1", "doc1"), c1, (tb, "k2", "doc2"), c2, (tc, "k3", "doc3")]
    )

    out = []
    async for item in fetch_delayed_changes(
        object_name, fake_delayed, None, False, log, lower_bound
    ):
        out.append(item)

    docs = [x for x in out if isinstance(x, str)]
    cursors = [x for x in out if isinstance(x, datetime)]

    assert docs == ["doc1", "doc2", "doc3"]
    # Intermediate cursors land exactly on the boundary datetimes, plus a final
    # window checkpoint covering the last chunk's documents.
    assert cursors[0] == c1
    assert cursors[1] == c2
    assert cursors[-1] > c2
    assert all(b > a for a, b in zip(cursors, cursors[1:]))  # strictly increasing
    assert all(c > lower_bound for c in cursors)


@pytest.mark.asyncio
async def test_fetch_delayed_changes_ignores_duplicate_and_regressing_datetimes():
    object_name = "test_delayed_regressing"
    cache.emitted_cache.pop(object_name, None)

    lower_bound = datetime.now(UTC) - timedelta(hours=2)
    ta = lower_bound + timedelta(minutes=5)
    c1 = lower_bound + timedelta(minutes=10)
    tb = lower_bound + timedelta(minutes=15)

    fake_delayed = _make_delayed(
        [
            (ta, "k1", "doc1"),
            c1,
            c1,  # duplicate -> ignored
            lower_bound + timedelta(minutes=3),  # regressing (< c1) -> ignored
            (tb, "k2", "doc2"),
        ]
    )

    out = []
    async for item in fetch_delayed_changes(
        object_name, fake_delayed, None, False, log, lower_bound
    ):
        out.append(item)

    cursors = [x for x in out if isinstance(x, datetime)]

    assert cursors.count(c1) == 1
    assert all(b > a for a, b in zip(cursors, cursors[1:]))  # strictly increasing


@pytest.mark.asyncio
async def test_fetch_delayed_changes_emits_out_of_window_documents_and_checkpoints_at_bound():
    object_name = "test_delayed_out_of_window"
    cache.emitted_cache.pop(object_name, None)

    # lower_bound is two hours back, so the stream is catching up and the
    # window is capped at [lower_bound, lower_bound + MAX_DELAYED_WINDOW].
    lower_bound = datetime.now(UTC) - timedelta(hours=2)
    upper_bound = lower_bound + MAX_DELAYED_WINDOW
    below = lower_bound - timedelta(minutes=30)
    inside = lower_bound + timedelta(minutes=30)
    above = upper_bound + timedelta(minutes=30)

    fake_delayed = _make_delayed(
        [(below, "k1", "doc_below"), (inside, "k2", "doc_inside"), (above, "k3", "doc_above")]
    )

    out = []
    async for item in fetch_delayed_changes(
        object_name, fake_delayed, None, False, log, lower_bound
    ):
        out.append(item)

    docs = [x for x in out if isinstance(x, str)]
    cursors = [x for x in out if isinstance(x, datetime)]

    # Documents outside the window are emitted rather than dropped, and one
    # below the window does not end the stream early.
    assert docs == ["doc_below", "doc_inside", "doc_above"]
    # The checkpoint is the window's bound, unaffected by any document's timestamp.
    assert cursors == [upper_bound]


@pytest.mark.asyncio
async def test_fetch_delayed_changes_leaves_an_empty_caught_up_window_open():
    object_name = "test_delayed_empty_window"
    cache.emitted_cache.pop(object_name, None)

    # Ten minutes of unread time: wide enough to poll, and within an hour of
    # the horizon, so the stream is caught up and the window reaches it.
    lower_bound = datetime.now(UTC) - timedelta(hours=1, minutes=10)

    out = []
    async for item in fetch_delayed_changes(
        object_name, _make_delayed([]), None, False, log, lower_bound
    ):
        out.append(item)

    assert out == []


@pytest.mark.asyncio
async def test_fetch_delayed_changes_checkpoints_an_empty_catch_up_window_at_its_bound():
    object_name = "test_delayed_empty_catch_up"
    cache.emitted_cache.pop(object_name, None)

    # Three hours behind: the stream is catching up and the window is capped at
    # MAX_DELAYED_WINDOW. Nothing is in it, but the cursor still moves so an
    # empty hour doesn't stall the catch-up.
    lower_bound = datetime.now(UTC) - timedelta(hours=3)

    out = []
    async for item in fetch_delayed_changes(
        object_name, _make_delayed([]), None, False, log, lower_bound
    ):
        out.append(item)

    assert out == [lower_bound + MAX_DELAYED_WINDOW]
