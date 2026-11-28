import asyncio
import json
import logging
import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import estuary_cdk.emitted_changes_cache as cache
import pytest

from source_posthog import api
from source_posthog.models import Event, Person

LOG = logging.getLogger("test")
BASE_URL = "https://posthog.test/"
PROJECT_ID = 1
T0 = datetime(2026, 11, 24, 15, 0, tzinfo=UTC)
INTERVAL = timedelta(minutes=5)

_TS = r"toDateTime64\('([^']+)', 6, 'UTC'\)"
_CURSOR = r"(?:COALESCE\([^)]*\)|\w+)"


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


class FakePostHog:
    """Answers the HogQL queries the connector sends from rows held in memory.

    It reads the bounds, the tie-break and the LIMIT back out of each query and
    applies them, so a test checks what the connector asks for, not only what
    it does with an answer.
    """

    def __init__(self, table: str, columns: list[str], rows: list[dict[str, Any]]):
        self.table = table
        self.columns = columns
        self.rows = rows
        self.queries: list[str] = []

    def _cursor(self, row: dict[str, Any]) -> datetime:
        if self.table == "events":
            return datetime.fromisoformat(row["timestamp"])
        return datetime.fromisoformat(row["last_seen_at"] or row["created_at"])

    def _id(self, row: dict[str, Any]) -> str:
        return row["uuid"] if self.table == "events" else row["id"]

    def _answer(self, query: str) -> dict[str, Any]:
        self.queries.append(query)
        if query == f"SELECT * FROM {self.table} LIMIT 0":
            return {"columns": self.columns, "results": []}

        where = query.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]
        limit_match = re.search(r"LIMIT (\d+)$", query)
        assert limit_match
        limit = int(limit_match.group(1))

        start: datetime
        after_id: str | None = None
        inclusive = False
        if m := re.match(
            rf"\({_CURSOR} > {_TS} OR \({_CURSOR} = {_TS} "
            + rf"AND toString\(\w+\) > '([^']*)'\)\)",
            where,
        ):
            start, after_id = _ts(m.group(1)), m.group(3)
        elif m := re.match(rf"{_CURSOR} (>=|>) {_TS}", where):
            start, inclusive = _ts(m.group(2)), m.group(1) == ">="
        else:
            raise AssertionError(f"unexpected WHERE: {where}")

        def lower(row: dict[str, Any]) -> bool:
            c = self._cursor(row)
            if after_id is not None:
                return c > start or (c == start and self._id(row) > after_id)
            return c >= start if inclusive else c > start

        end_match = re.search(rf"<= {_TS}", where)
        end = _ts(end_match.group(1)) if end_match else None

        selected = [
            r
            for r in self.rows
            if lower(r) and (end is None or self._cursor(r) <= end)
        ]
        selected.sort(key=lambda r: (self._cursor(r), self._id(r)))
        results = [[r[c] for c in self.columns] for r in selected[:limit]]
        return {"columns": self.columns, "results": results}

    async def request(self, *_: Any, json: dict[str, Any], **__: Any) -> bytes:
        return _dumps(self._answer(json["query"]["query"]))

    async def request_stream(self, *_: Any, json: dict[str, Any], **__: Any):
        body = _dumps(self._answer(json["query"]["query"]))

        async def gen() -> AsyncGenerator[bytes, None]:
            yield body

        return None, gen


def _dumps(value: Any) -> bytes:
    return json.dumps(value).encode()


def _event(i: int, ts: datetime) -> dict[str, Any]:
    return {"uuid": f"00000000-0000-0000-0000-{i:012d}", "timestamp": ts.isoformat()}


def _person(i: int, created: datetime, last_seen: datetime | None) -> dict[str, Any]:
    return {
        "id": f"00000000-0000-0000-0000-{i:012d}",
        "created_at": created.isoformat(),
        "last_seen_at": last_seen.isoformat() if last_seen else None,
    }


def _config(start_date: datetime) -> Any:
    return SimpleNamespace(
        start_date=start_date, advanced=SimpleNamespace(base_url=BASE_URL)
    )


async def _collect(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return [item async for item in gen]


def _run(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return asyncio.run(_collect(gen))


def _ids(items: list[Any]) -> list[str]:
    return [item.id for item in items if isinstance(item, Event | Person)]


@pytest.fixture(autouse=True)
def small_pages(monkeypatch):
    # Three rows to a page, so a handful of rows spans several pages.
    monkeypatch.setattr(api, "HOGQL_PAGE_SIZE", 3)
    cache.emitted_cache.clear()
    api._hogql_columns_cache.clear()
    yield
    cache.emitted_cache.clear()
    api._hogql_columns_cache.clear()


# Paging


def test_events_page_through_rows_that_share_a_timestamp():
    # Seven events, five of them on one instant, so pages end inside the group.
    rows = [_event(i, T0) for i in range(5)] + [
        _event(5, T0 + timedelta(seconds=1)),
        _event(6, T0 + timedelta(seconds=2)),
    ]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    items = _run(
        api.fetch_project_events(
            fake, _config(T0), PROJECT_ID, None, INTERVAL, LOG, T0 - timedelta(seconds=1)
        )
    )

    assert _ids(items) == [r["uuid"] for r in rows]
    assert items[-1] == T0 + timedelta(seconds=2)


def test_persons_backfill_pages_through_one_hour_of_persons():
    # PostHog writes `last_seen_at` as the start of the hour, so every person
    # seen in one hour shares one cursor value.
    rows = [_person(i, T0 + timedelta(minutes=i), T0) for i in range(8)]
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], rows)

    items = _run(
        api.backfill_persons(
            fake,
            _config(T0 - timedelta(days=1)),
            PROJECT_ID,
            LOG,
            None,
            T0 + timedelta(hours=1),
        )
    )

    assert sorted(_ids(items)) == sorted(r["id"] for r in rows)
    assert len(_ids(items)) == len(rows)


def test_single_cursor_column_is_not_wrapped_in_coalesce():
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], [_event(0, T0)])

    _run(
        api.fetch_project_events(
            fake, _config(T0), PROJECT_ID, None, INTERVAL, LOG, T0 - timedelta(seconds=1)
        )
    )

    data_queries = [q for q in fake.queries if "WHERE" in q]
    assert data_queries
    for query in data_queries:
        assert "COALESCE" not in query
        assert "ORDER BY timestamp ASC, toString(uuid) ASC" in query


# Persons incremental


def _poll_persons(fake: Any, cursor: datetime) -> tuple[list[str], datetime]:
    """Run one Persons poll and check the CDK's cursor rules on the result."""
    items = _run(api.fetch_persons(fake, _config(T0), PROJECT_ID, LOG, cursor))
    cursors = [i for i in items if isinstance(i, datetime)]
    ids = _ids(items)

    # The CDK rejects documents without a final cursor, and any cursor that is
    # not strictly greater than the one before.
    if ids:
        assert cursors and not isinstance(items[-1], Person)
    for c in cursors:
        assert c > cursor
    return ids, cursors[-1] if cursors else cursor


def _this_hour() -> datetime:
    # The Persons cursor is the real time of the poll, so these tests work
    # from the real current hour.
    return datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)


def test_persons_added_later_in_the_cursors_hour_are_captured():
    hour = _this_hour()
    persons: list[dict[str, Any]] = [_person(0, hour, hour)]
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], persons)

    ids, cursor = _poll_persons(fake, hour - timedelta(minutes=55))
    assert ids == [persons[0]["id"]]

    # Added later in the same hour, so PostHog gives it the same `last_seen_at`
    # as the first one.
    persons.append(_person(1, hour, hour))
    ids, _ = _poll_persons(fake, cursor)
    assert persons[1]["id"] in ids


def test_persons_are_emitted_again_by_every_poll_that_reads_their_hour():
    # A person changed later in the hour keeps the same `last_seen_at`, so the
    # connector cannot tell a change apart and emits them again.
    hour = _this_hour()
    person = _person(0, hour, hour)
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], [person])

    ids, cursor = _poll_persons(fake, hour - timedelta(minutes=1))
    assert ids == [person["id"]]
    ids, _ = _poll_persons(fake, cursor)
    assert ids == [person["id"]]
    assert cache.count_for(f"persons/{PROJECT_ID}") == 0


def test_persons_written_late_for_the_previous_hour_are_captured():
    hour = _this_hour()
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], [])
    cursor = datetime.now(tz=UTC) - timedelta(seconds=1)

    # Seen in the previous hour, written after the cursor moved into this one.
    fake.rows.append(_person(0, hour - timedelta(hours=1), hour - timedelta(hours=1)))
    ids, _ = _poll_persons(fake, cursor)
    assert ids == [fake.rows[0]["id"]]


def test_the_persons_cursor_is_the_time_of_the_poll():
    hour = _this_hour()
    fake: Any = FakePostHog(
        "persons", ["id", "created_at", "last_seen_at"], [_person(0, hour, hour)]
    )

    before = datetime.now(tz=UTC)
    _, cursor = _poll_persons(fake, hour - timedelta(minutes=1))
    # Close to the present, so the CDK waits a full interval before the next
    # poll.
    assert before <= cursor <= datetime.now(tz=UTC)

    # A poll that emits nothing leaves the cursor where it was.
    fake.rows.clear()
    ids, same_cursor = _poll_persons(fake, cursor)
    assert ids == []
    assert same_cursor == cursor


# Events dedupe cache


def _events_backfill(fake: Any, start: datetime, cutoff: datetime) -> list[str]:
    """Run an Events backfill to the end, the way the CDK calls it."""
    ids: list[str] = []
    page = None
    while True:
        items = _run(
            api.backfill_project_events(
                fake, _config(start), PROJECT_ID, LOG, page, cutoff
            )
        )
        ids += _ids(items)
        if not items or not isinstance(items[-1], str):
            return ids
        page = items[-1]


def test_events_backfill_caches_only_the_lookback_overlap():
    cutoff = T0
    # One event an hour for two days, plus three in the last hour.
    rows = [_event(i, cutoff - timedelta(hours=48 - i)) for i in range(48)]
    rows += [_event(100 + i, cutoff - timedelta(minutes=10 * (i + 1))) for i in range(3)]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    ids = _events_backfill(fake, cutoff - timedelta(days=3), cutoff)

    assert sorted(ids) == sorted(r["uuid"] for r in rows)
    # Only the three after the overlap start. The event exactly one hour
    # before the cutoff is not in the overlap.
    assert cache.count_for(f"events/{PROJECT_ID}") == 3


def test_events_lookback_skips_what_the_backfill_emitted_and_cleans_up():
    cutoff = T0
    lookback_cursor = cutoff - api.EVENTS_EVENTUAL_CONSISTENCY_HORIZON
    rows = [_event(i, cutoff - timedelta(minutes=10 * (i + 1))) for i in range(3)]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    backfilled = _events_backfill(fake, cutoff - timedelta(days=1), cutoff)
    assert len(backfilled) == 3

    # The lookback subtask runs two hours later and reads the same events.
    lookback = _run(
        api.fetch_project_events(
            fake,
            _config(T0),
            PROJECT_ID,
            api.EVENTS_EVENTUAL_CONSISTENCY_HORIZON,
            INTERVAL,
            LOG,
            lookback_cursor,
        )
    )
    assert _ids(lookback) == []
    assert cache.count_for(f"events/{PROJECT_ID}") == 3

    # Its next poll starts past them, so they are removed.
    next_cursor = lookback[-1]
    _run(
        api.fetch_project_events(
            fake,
            _config(T0),
            PROJECT_ID,
            api.EVENTS_EVENTUAL_CONSISTENCY_HORIZON,
            INTERVAL,
            LOG,
            next_cursor,
        )
    )
    assert cache.count_for(f"events/{PROJECT_ID}") == 0


# Events backfill windows


def _query_bounds(query: str) -> tuple[datetime, datetime]:
    stamps = [_ts(m) for m in re.findall(_TS, query)]
    return stamps[0], stamps[-1]


def test_events_backfill_reads_one_day_per_query_and_ends_at_the_cutoff():
    cutoff = T0
    start = cutoff - timedelta(days=5, hours=6)
    # A few events on some days, none on others, one at the cutoff.
    rows = [
        _event(0, start + timedelta(minutes=1)),
        _event(1, start + timedelta(days=2)),
        _event(2, start + timedelta(days=2)),
        _event(3, cutoff),
    ]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    ids = _events_backfill(fake, start, cutoff)

    assert sorted(ids) == sorted(r["uuid"] for r in rows)
    data_queries = [q for q in fake.queries if "WHERE" in q]
    for query in data_queries:
        low, high = _query_bounds(query)
        assert high - low <= api.EVENTS_BACKFILL_WINDOW
    # Six windows: five whole days and the six hours before the cutoff.
    assert len(data_queries) == 6
    assert _query_bounds(data_queries[-1])[1] == cutoff
    # The columns are looked up one time, not before each window.
    assert len(fake.queries) == len(data_queries) + 1


def test_events_backfill_checkpoints_each_window_and_resumes_from_it():
    cutoff = T0
    start = cutoff - timedelta(days=2)
    rows = [_event(0, start + timedelta(hours=1)), _event(1, cutoff)]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    first = _run(
        api.backfill_project_events(fake, _config(start), PROJECT_ID, LOG, None, cutoff)
    )
    assert _ids(first) == [rows[0]["uuid"]]
    assert first[-1] == (start + timedelta(days=1)).isoformat()

    second = _run(
        api.backfill_project_events(
            fake, _config(start), PROJECT_ID, LOG, first[-1], cutoff
        )
    )
    # The last window ends at the cutoff, so no cursor follows: the backfill
    # is done.
    assert _ids(second) == [rows[1]["uuid"]]
    assert not isinstance(second[-1], str)


def test_a_future_person_does_not_move_the_cursor_past_other_persons():
    now = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    future = _person(0, now - timedelta(hours=1), now + timedelta(hours=20))
    fake: Any = FakePostHog(
        "persons", ["id", "created_at", "last_seen_at"], [future]
    )

    ids, cursor = _poll_persons(fake, now - timedelta(minutes=30))
    assert ids == []
    assert cursor < now + timedelta(hours=1)

    # A person seen in the current hour, after the future one arrived.
    fake.rows.append(_person(1, now + timedelta(minutes=1), now))
    ids, _ = _poll_persons(fake, cursor)
    assert ids == [fake.rows[1]["id"]]


def _drop_second_event(gen: AsyncGenerator[Any, None]) -> list[Any]:
    """Take two items, then stop, the way the timeout wrapper drops the second."""

    async def run() -> list[Any]:
        taken = [await anext(gen), await anext(gen)]
        await gen.aclose()
        return taken

    return asyncio.run(run())


def test_an_event_dropped_by_the_backfill_timeout_is_not_cached():
    cutoff = T0
    rows = [_event(i, cutoff - timedelta(minutes=10 - i)) for i in range(2)]
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], rows)

    inner = api.backfill_project_events.__wrapped__  # type: ignore[attr-defined]
    _drop_second_event(
        inner(fake, _config(cutoff - timedelta(days=1)), PROJECT_ID, LOG, None, cutoff)
    )

    name = f"events/{PROJECT_ID}"
    assert cache.has_as_recent_as(name, rows[0]["uuid"], _ts(rows[0]["timestamp"]))
    assert not cache.has_as_recent_as(name, rows[1]["uuid"], _ts(rows[1]["timestamp"]))


def test_a_future_event_does_not_move_the_realtime_cursor():
    now = datetime.now(tz=UTC)
    future = _event(0, now + timedelta(hours=20))
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], [future])
    cursor = now - timedelta(minutes=10)

    items = _run(
        api.fetch_project_events(fake, _config(T0), PROJECT_ID, None, INTERVAL, LOG, cursor)
    )
    assert items == []

    # An event from now, read by the next poll.
    fake.rows.append(_event(1, now - timedelta(minutes=1)))
    items = _run(
        api.fetch_project_events(fake, _config(T0), PROJECT_ID, None, INTERVAL, LOG, cursor)
    )
    assert _ids(items) == [fake.rows[1]["uuid"]]


def test_a_lookback_poll_shorter_than_the_interval_reads_nothing():
    # The lookback ran a minute ago, so its window would be a minute long.
    horizon = api.EVENTS_EVENTUAL_CONSISTENCY_HORIZON
    cursor = datetime.now(tz=UTC) - horizon - timedelta(minutes=1)
    fake: Any = FakePostHog("events", ["uuid", "timestamp"], [_event(0, cursor)])

    items = _run(
        api.fetch_project_events(
            fake, _config(T0), PROJECT_ID, horizon, INTERVAL, LOG, cursor
        )
    )

    # No cursor, so the CDK sleeps a full interval before the next poll.
    assert items == []
    assert fake.queries == []
