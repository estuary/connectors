import asyncio
import json
import logging
import random
from datetime import UTC, datetime, timedelta

import pytest

import source_aircall_native.api as api
from source_aircall_native.models import Webhook

LOG = logging.getLogger("test")
NOW = 2_000_000


class FakeAircall:
    """Serves `rows` like Aircall's list endpoints, including the 10,000-result cap."""

    def __init__(self, response_key, rows, on_request=None):
        self.response_key = response_key
        self.rows = rows
        self.on_request = on_request
        self.pages = []

    async def request(self, log, url, params=None, **kwargs):
        if self.on_request:
            self.on_request(self)

        page, per_page = params["page"], params["per_page"]
        assert page * per_page <= 10_000, "paged past the 10,000-result cap"
        self.pages.append(page)

        rows = self.rows
        if "from" in params:
            if self.response_key == "calls":
                rows = [
                    r for r in rows if params["from"] < r["started_at"] <= params["to"]
                ]
            else:
                rows = [
                    r for r in rows if params["from"] <= r["created_at"] <= params["to"]
                ]

        order_by = params.get(
            "order_by", "started_at" if self.response_key == "calls" else "created_at"
        )
        rows = sorted(
            rows,
            key=lambda r: (r[order_by], r["id"]),
            reverse=params.get("order") == "desc",
        )
        chunk = rows[(page - 1) * per_page : page * per_page]

        return json.dumps(
            {
                self.response_key: chunk,
                "meta": {"total": len(rows), "current_page": page},
            }
        ).encode()


def collect(gen):
    async def run():
        return [item async for item in gen]

    return asyncio.run(run())


def calls_db():
    rng = random.Random(1)
    started = sorted(rng.sample(range(1, 100_000), 25_000) + [50_000] * 30)
    return [{"id": i, "started_at": t} for i, t in enumerate(started)]


def contacts_db(n, burst=0):
    rng = random.Random(2)
    rows = []
    for i in range(n):
        created = rng.randint(0, 1_000_000)
        rows.append(
            {
                "id": i,
                "created_at": created,
                "updated_at": rng.randint(created, 1_900_000),
            }
        )
    for j in range(burst):
        rows.append({"id": n + j, "created_at": 500_000, "updated_at": 500_000})
    return rows


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch):
    monkeypatch.setattr(api, "now", lambda: datetime.fromtimestamp(NOW, tz=UTC))


def test_calls_window_splits_under_cap():
    db = calls_db()
    http = FakeAircall("calls", db)

    calls = collect(api._fetch_calls_window(LOG, http, 0, 100_000))

    assert [c.id for c in calls] == [r["id"] for r in db]


def test_backfill_calls_checkpoints_under_cap(monkeypatch):
    monkeypatch.setattr(api, "CALLS_RETENTION_CLAMP", timedelta(days=10_000))
    db = calls_db()
    http = FakeAircall("calls", db)
    cutoff = datetime.fromtimestamp(100_001, tz=UTC)

    ids, page = [], None
    while True:
        items = collect(
            api.backfill_calls(
                http, datetime.fromtimestamp(0, tz=UTC), LOG, page, cutoff
            )
        )
        docs = [i for i in items if not isinstance(i, int)]
        assert len(docs) < 10_000
        ids += [d.id for d in docs]
        if not isinstance(items[-1], int):
            break
        assert page is None or items[-1] > page
        page = items[-1]

    assert ids == [r["id"] for r in db]


def test_backfill_calls_clamps_to_retention(monkeypatch):
    monkeypatch.setattr(api, "CALLS_RETENTION_CLAMP", timedelta(seconds=NOW - 1_000))
    http = FakeAircall(
        "calls", [{"id": 1, "started_at": 100}, {"id": 2, "started_at": 2_000}]
    )
    cutoff = datetime.fromtimestamp(NOW, tz=UTC)

    calls = collect(
        api.backfill_calls(http, datetime.fromtimestamp(0, tz=UTC), LOG, None, cutoff)
    )

    assert [c.id for c in calls] == [2]


def test_lookback_sleeps_after_checkpoint():
    lag = timedelta(hours=24)
    previous_horizon = datetime.fromtimestamp(NOW - 3, tz=UTC) - lag
    http = FakeAircall("calls", [])

    assert collect(api.fetch_calls(http, lag, LOG, previous_horizon)) == []
    assert http.pages == []


def test_backfill_contacts_drains_same_second_burst():
    db = contacts_db(3000, burst=120)
    http = FakeAircall("contacts", db)
    cutoff = datetime.fromtimestamp(NOW, tz=UTC)

    ids, page = set(), None
    while True:
        items = collect(api.backfill_contacts(http, LOG, page, cutoff))
        ids |= {i.id for i in items if not isinstance(i, int)}
        next_pages = [i for i in items if isinstance(i, int)]
        if not next_pages:
            break
        page = next_pages[-1]

    assert ids == {r["id"] for r in db}


def changed_since(db, cursor):
    return {r["id"] for r in db if cursor < r["updated_at"] <= NOW - 1}


def fetch_contacts(http, cursor):
    items = collect(
        api.fetch_contacts(http, LOG, datetime.fromtimestamp(cursor, tz=UTC))
    )
    return {i.id for i in items if not isinstance(i, datetime)}, items


def test_fetch_contacts_walks_to_cursor():
    db = contacts_db(3000)
    ids, items = fetch_contacts(FakeAircall("contacts", db), 1_890_000)

    assert ids == changed_since(db, 1_890_000)
    assert items[-1] == datetime.fromtimestamp(NOW - 1, tz=UTC)


def test_fetch_contacts_rereads_all_when_capped():
    db = contacts_db(15_000)
    ids, _ = fetch_contacts(FakeAircall("contacts", db), 100)

    assert len(ids) > 10_000
    assert ids == changed_since(db, 100)


def test_fetch_contacts_exactly_at_cap_is_not_capped():
    db = [{"id": i, "created_at": 0, "updated_at": 1_000 + i} for i in range(10_000)]
    http = FakeAircall("contacts", db)
    ids, _ = fetch_contacts(http, 0)

    assert ids == changed_since(db, 0)
    assert len(http.pages) == 200


def test_fetch_contacts_restarts_after_deletion():
    db = contacts_db(2000)

    def delete_newest_once(http):
        if len(http.pages) == 2 and len(http.rows) == len(db):
            newest = max(http.rows, key=lambda r: r["updated_at"])
            http.rows = [r for r in http.rows if r is not newest]

    http = FakeAircall("contacts", list(db), delete_newest_once)
    ids, _ = fetch_contacts(http, 0)

    assert changed_since(http.rows, 0) <= ids


def test_webhook_token_is_never_captured():
    assert (
        "token" not in Webhook.model_validate({"id": 1, "token": "secret"}).model_dump()
    )
