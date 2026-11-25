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
    yield
    cache.emitted_cache.clear()


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
            fake, _config(T0), PROJECT_ID, None, LOG, T0 - timedelta(seconds=1)
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
            fake, _config(T0), PROJECT_ID, None, LOG, T0 - timedelta(seconds=1)
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


def test_persons_added_later_in_the_cursors_hour_are_captured():
    persons: list[dict[str, Any]] = [_person(0, T0 + timedelta(minutes=2), T0)]
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], persons)

    ids, cursor = _poll_persons(fake, T0 - timedelta(minutes=55))
    assert ids == [persons[0]["id"]]
    assert cursor >= T0

    # Added later in the same hour, so PostHog gives it the same `last_seen_at`
    # as the cursor. The connector used to skip these.
    persons.append(_person(1, T0 + timedelta(minutes=20), T0))
    ids, cursor = _poll_persons(fake, cursor)
    assert ids == [persons[1]["id"]]

    # Nothing changed: nothing is emitted, and the cursor stays.
    ids, same_cursor = _poll_persons(fake, cursor)
    assert ids == []
    assert same_cursor == cursor


def test_persons_written_late_for_the_previous_hour_are_captured():
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], [])
    cursor = T0 + timedelta(minutes=5)

    # Seen at 14:50, written after the cursor reached 15:05.
    fake.rows.append(
        _person(0, T0 - timedelta(minutes=10), T0 - timedelta(hours=1))
    )
    ids, _ = _poll_persons(fake, cursor)
    assert ids == [fake.rows[0]["id"]]


def test_persons_seen_in_a_new_hour_are_emitted_again():
    person = _person(0, T0 - timedelta(hours=3), T0)
    fake: Any = FakePostHog("persons", ["id", "created_at", "last_seen_at"], [person])

    ids, cursor = _poll_persons(fake, T0 - timedelta(minutes=1))
    assert ids == [person["id"]]

    person["last_seen_at"] = (T0 + timedelta(hours=1)).isoformat()
    ids, _ = _poll_persons(fake, cursor)
    assert ids == [person["id"]]


def test_persons_cache_keeps_only_what_the_next_poll_can_read():
    fake: Any = FakePostHog(
        "persons",
        ["id", "created_at", "last_seen_at"],
        [_person(i, T0 + timedelta(hours=i), T0 + timedelta(hours=i)) for i in range(5)],
    )

    _, cursor = _poll_persons(fake, T0 - timedelta(minutes=1))

    # The cursor is at 19:00, so the next poll reads from 18:00.
    assert cursor == T0 + timedelta(hours=4)
    assert cache.count_for(f"persons/{PROJECT_ID}") == 2
