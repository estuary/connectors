"""Unit tests for the chunked delayed-stream path.

These cover the pieces that make delayed streams checkpoint progress within a
large window:
  - fetch_search_objects splitting a window that exceeds the Search API's 10k
    result cap into time chunks the API can return completely, resuming at
    each chunk's end,
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
from source_hubspot_native.api.search_objects import (
    PEEK_OFFSET,
    SEARCH_PAGE_LIMIT,
    fetch_search_objects,
)
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


def _search_page(
    items: list[tuple[datetime, int]],
    next_after: str | None,
    total: int | None = None,
) -> bytes:
    """Build a SearchPageResult[CustomObjectSearchResult] JSON body. `total` is
    the window's match count as HubSpot reports it, defaulting to the page's
    own size."""
    body: dict[str, Any] = {
        "total": len(items) if total is None else total,
        "results": [
            {"id": id, "properties": {"hs_lastmodifieddate": ts.isoformat()}}
            for ts, id in items
        ],
    }
    if next_after is not None:
        body["paging"] = {"next": {"after": next_after}}
    return json.dumps(body).encode()


MS = timedelta(milliseconds=1)
SINCE = datetime(2026, 1, 1, tzinfo=UTC)
UNTIL = SINCE + timedelta(hours=1)


def _filter(request: dict[str, Any]) -> dict[str, Any]:
    return request["json"]["filters"][0]


def _afters(http: FakeHTTP) -> list[Any]:
    return [r["json"].get("after") for r in http.requests]


def _full_peek_page(
    items: list[tuple[datetime, int]], filler: datetime, total: int = 25_000
) -> bytes:
    """A page at the cap offset holding exactly SEARCH_PAGE_LIMIT records:
    `items` plus filler records stamped `filler`, so the page reads as the cap
    having been reached."""
    padding = [(filler, 20_000 + i) for i in range(SEARCH_PAGE_LIMIT - len(items))]
    return _search_page(items + padding, next_after="10000", total=total)


@pytest.mark.asyncio
async def test_fetch_search_objects_reads_a_window_that_fits_in_one_search():
    t1, t2, t3 = (SINCE + timedelta(minutes=i) for i in (1, 2, 3))
    http = FakeHTTP(
        [
            # Out of order within the page; the reader sorts and never
            # depends on the order HubSpot chose.
            _search_page([(t2, 2), (t1, 1)], next_after="200", total=3),
            _search_page([(t3, 3)], next_after=None, total=3),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert items == [(t1, "1"), (t2, "2"), (t3, "3")]
    assert page is None
    assert _afters(http) == [None, 200]
    f = _filter(http.requests[0])
    assert (f["operator"], f["value"], f["highValue"]) == (
        "BETWEEN", dt_to_str(SINCE), dt_to_str(UNTIL),
    )


@pytest.mark.asyncio
async def test_fetch_search_objects_open_ended_window_searches_forward_from_since():
    http = FakeHTTP([_search_page([(SINCE + MS, 1)], next_after=None)])

    items, page = await fetch_search_objects("deals", log, http, SINCE, None, None)

    assert items == [(SINCE + MS, "1")]
    assert page is None
    f = _filter(http.requests[0])
    assert f["operator"] == "GTE"
    assert "highValue" not in f


@pytest.mark.asyncio
async def test_fetch_search_objects_splits_an_oversized_window_at_the_peeked_boundary():
    boundary = SINCE + timedelta(minutes=30)
    http = FakeHTTP(
        [
            # The window holds far more than the cap. This page's records are
            # discarded; only its total matters.
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            # The last page the cap allows. Its earliest in-window timestamp is
            # where the cap falls, whatever order the page is in.
            _full_peek_page(
                [(boundary + MS, 9801), (boundary, 9800), (boundary + 2 * MS, 9802)],
                filler=boundary + 3 * MS,
            ),
            # A fresh search bounded just below it fits, and is read in full.
            _search_page([(SINCE + 2 * MS, 2), (SINCE + MS, 1)], next_after=None, total=2),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert items == [(SINCE + MS, "1"), (SINCE + 2 * MS, "2")]
    # The chunk ends the millisecond before the boundary and resumes on it.
    assert page == dt_to_str(boundary)
    assert _afters(http) == [None, PEEK_OFFSET, None]
    assert _filter(http.requests[2])["highValue"] == dt_to_str(boundary - MS)


@pytest.mark.asyncio
async def test_fetch_search_objects_ignores_peeked_timestamps_outside_the_window():
    boundary = SINCE + timedelta(minutes=30)
    http = FakeHTTP(
        [
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            # A stale timestamp below the window start and an inflated one past
            # its end are both on the peek page. Neither can bound the chunk.
            _full_peek_page(
                [(SINCE - timedelta(days=1), 9800), (boundary, 9801), (UNTIL + MS, 9802)],
                filler=boundary + MS,
            ),
            _search_page([(SINCE + MS, 1)], next_after=None, total=1),
        ]
    )

    _, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert page == dt_to_str(boundary)
    assert _filter(http.requests[2])["highValue"] == dt_to_str(boundary - MS)


@pytest.mark.asyncio
async def test_fetch_search_objects_halves_when_the_peek_offers_no_boundary():
    http = FakeHTTP(
        [
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            # Every peeked timestamp is past the window's end.
            _full_peek_page([(UNTIL + MS, 9800), (UNTIL + 2 * MS, 9801)], filler=UNTIL + 3 * MS),
            _search_page([(SINCE + MS, 1)], next_after=None, total=1),
        ]
    )

    _, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    midpoint = SINCE + timedelta(minutes=30)
    assert _filter(http.requests[2])["highValue"] == dt_to_str(midpoint)
    assert page == dt_to_str(midpoint + MS)


@pytest.mark.asyncio
async def test_fetch_search_objects_halves_again_when_verification_overflows():
    boundary = SINCE + timedelta(minutes=40)
    http = FakeHTTP(
        [
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            _full_peek_page([(boundary, 9800)], filler=boundary + MS),
            # Disorder at the boundary: the peeked chunk still exceeds the cap.
            _search_page([(SINCE + MS, 1)], next_after="200", total=12_000),
            # Halving from the failed chunk end fits.
            _search_page([(SINCE + MS, 1)], next_after=None, total=1),
        ]
    )

    _, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    first_end = boundary - MS
    # Halving lands on a whole millisecond.
    second_end = SINCE + MS * ((first_end - SINCE) // MS // 2)
    assert _filter(http.requests[2])["highValue"] == dt_to_str(first_end)
    assert _filter(http.requests[3])["highValue"] == dt_to_str(second_end)
    assert page == dt_to_str(second_end + MS)


@pytest.mark.asyncio
async def test_fetch_search_objects_reads_the_window_when_the_peek_is_empty():
    http = FakeHTTP(
        [
            # The total says oversized, but nothing sits at the peek offset.
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            _search_page([], next_after=None, total=25_000),
            _search_page([(SINCE + MS, 1), (SINCE + 2 * MS, 2)], next_after=None, total=2),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert items == [(SINCE + MS, "1"), (SINCE + 2 * MS, "2")]
    assert page is None
    assert _filter(http.requests[2])["highValue"] == dt_to_str(UNTIL)


@pytest.mark.asyncio
async def test_fetch_search_objects_resumes_by_peeking_first():
    resume_at = SINCE + timedelta(minutes=30)
    boundary = SINCE + timedelta(minutes=45)
    http = FakeHTTP(
        [
            # No leading probe page: the remainder of a split window is
            # probably still oversized, so measure it straight away.
            _full_peek_page([(boundary, 9800)], filler=boundary + MS, total=12_000),
            _search_page([(resume_at, 5)], next_after=None, total=1),
        ]
    )

    items, page = await fetch_search_objects(
        "deals", log, http, SINCE, UNTIL, dt_to_str(resume_at)
    )

    assert items == [(resume_at, "5")]
    assert page == dt_to_str(boundary)
    assert _afters(http) == [PEEK_OFFSET, None]
    assert _filter(http.requests[0])["value"] == dt_to_str(resume_at)


@pytest.mark.asyncio
async def test_fetch_search_objects_splits_when_paging_would_cross_the_cap_anyway():
    boundary = SINCE + timedelta(minutes=30)
    http = FakeHTTP(
        [
            # The total says the window fits, but the page after this one would
            # cross the cap. Don't trust the total; split.
            _search_page([(SINCE + MS, 1)], next_after="9900", total=100),
            _full_peek_page([(boundary, 9800)], filler=boundary + MS, total=100),
            _search_page([(SINCE + MS, 1)], next_after=None, total=1),
        ]
    )

    _, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert page == dt_to_str(boundary)
    assert _afters(http) == [None, PEEK_OFFSET, None]


@pytest.mark.asyncio
async def test_fetch_search_objects_returns_records_reported_outside_the_window():
    inside = SINCE + timedelta(minutes=1)
    http = FakeHTTP(
        [
            # HubSpot's filter placed all three in the window; two report a
            # timestamp outside it. All are returned, since the filter decides
            # membership, and none moves the resume cursor.
            _search_page(
                [(SINCE - timedelta(hours=1), 1), (inside, 2), (UNTIL + timedelta(hours=1), 3)],
                next_after=None,
                total=3,
            ),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert [id for _, id in items] == ["1", "2", "3"]
    assert page is None


@pytest.mark.asyncio
async def test_fetch_search_objects_drains_dense_millisecond_cycle():
    instant = SINCE  # every record shares `since`'s millisecond
    http = FakeHTTP(
        [
            _search_page([(instant, i) for i in range(1, 6)], next_after="200", total=25_000),
            # The whole peek page sits at the window start, so advancing by
            # timestamp can't make progress: a cycle.
            _search_page([(instant, i) for i in range(9801, 10001)], next_after="10000", total=25_000),
            # The modified-at drain returns every record at that instant, in
            # ascending hs_object_id order, with no further pages.
            _search_page([(instant, i) for i in range(1, 9)], next_after=None),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    # All ids at the instant are returned, and the cursor steps one ms past it.
    assert {id for _, id in items} == {str(i) for i in range(1, 9)}
    assert page == dt_to_str(instant + MS)
    assert _filter(http.requests[2])["operator"] == "EQ"


@pytest.mark.asyncio
async def test_fetch_search_objects_reads_the_window_when_fewer_than_the_cap_remain():
    http = FakeHTTP(
        [
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            # Only 50 records sit at the cap offset, so the window holds at
            # most 9,850 and is read whole rather than split.
            _search_page(
                [(SINCE + timedelta(minutes=i), 9800 + i) for i in range(50)],
                next_after=None,
                total=25_000,
            ),
            _search_page([(SINCE + MS, 1), (SINCE + 2 * MS, 2)], next_after=None, total=2),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    assert items == [(SINCE + MS, "1"), (SINCE + 2 * MS, "2")]
    assert page is None
    assert _filter(http.requests[2])["highValue"] == dt_to_str(UNTIL)


@pytest.mark.asyncio
async def test_fetch_search_objects_floors_window_bounds_to_whole_milliseconds():
    # Delayed windows carry microseconds; HubSpot timestamps don't.
    since = SINCE + timedelta(microseconds=456)
    until = UNTIL + timedelta(microseconds=789)
    boundary = SINCE + MS  # the first record at the cap is in the next millisecond
    http = FakeHTTP(
        [
            _search_page([(SINCE, 1)], next_after="200", total=25_000),
            _full_peek_page([(boundary, 9800)], filler=boundary + MS),
            # The chunk end can't fall below the floored start, so the
            # verification covers exactly the start's millisecond.
            _search_page([(SINCE, 1)], next_after=None, total=1),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, since, until, None)

    assert items == [(SINCE, "1")]
    assert page == dt_to_str(SINCE + MS)
    first, verify = _filter(http.requests[0]), _filter(http.requests[2])
    assert (first["value"], first["highValue"]) == (dt_to_str(SINCE), dt_to_str(UNTIL))
    assert (verify["value"], verify["highValue"]) == (dt_to_str(SINCE), dt_to_str(SINCE))


@pytest.mark.asyncio
async def test_fetch_search_objects_halves_when_a_verified_chunk_pages_past_the_cap():
    boundary = SINCE + timedelta(minutes=40)
    http = FakeHTTP(
        [
            _search_page([(SINCE + MS, 1)], next_after="200", total=25_000),
            _full_peek_page([(boundary, 9800)], filler=boundary + MS),
            # The total says the chunk fits, but its paging would cross the cap.
            _search_page([(SINCE + MS, 1)], next_after="9900", total=100),
            _search_page([(SINCE + MS, 1)], next_after=None, total=1),
        ]
    )

    _, page = await fetch_search_objects("deals", log, http, SINCE, UNTIL, None)

    first_end = boundary - MS
    second_end = SINCE + MS * ((first_end - SINCE) // MS // 2)
    assert _filter(http.requests[2])["highValue"] == dt_to_str(first_end)
    assert _filter(http.requests[3])["highValue"] == dt_to_str(second_end)
    assert page == dt_to_str(second_end + MS)


@pytest.mark.asyncio
async def test_fetch_search_objects_drains_when_halving_reaches_a_single_millisecond():
    until = SINCE + 3 * MS
    http = FakeHTTP(
        [
            _search_page([(SINCE, 1)], next_after="200", total=25_000),
            # Every peeked timestamp is past the window, so halving is the only
            # tool, and the window is too narrow for it to find a chunk that fits.
            _full_peek_page([(until + MS, 9800)], filler=until + 2 * MS),
            _search_page([(SINCE, 1)], next_after="200", total=25_000),
            _search_page([(SINCE, 1)], next_after="200", total=25_000),
            # The single-millisecond drain, in ascending id order.
            _search_page([(SINCE, i) for i in range(1, 4)], next_after=None),
        ]
    )

    items, page = await fetch_search_objects("deals", log, http, SINCE, until, None)

    assert [_filter(r)["highValue"] for r in http.requests[2:4]] == [
        dt_to_str(SINCE + MS),
        dt_to_str(SINCE),
    ]
    assert _filter(http.requests[4])["operator"] == "EQ"
    assert {id for _, id in items} == {"1", "2", "3"}
    assert page == dt_to_str(SINCE + MS)


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
