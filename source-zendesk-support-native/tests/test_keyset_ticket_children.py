import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, AsyncGenerator, Callable

import pytest

from source_zendesk_support_native.api.shared import INCREMENTAL_LAG, dt_to_s
from source_zendesk_support_native.api.ticket_children import (
    backfill_incremental_keyset_paginated_ticket_children,
    fetch_incremental_keyset_paginated_ticket_children,
    fetch_metric_events_of_tickets,
    fetch_metric_sets_of_tickets,
)

log = logging.getLogger(__name__)


def _ticket(id: int, updated_at: datetime) -> dict[str, Any]:
    return {"id": id, "updated_at": updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _page(tickets: list[dict[str, Any]], after_cursor: str | None) -> dict[str, Any]:
    return {
        "tickets": tickets,
        "meta": {"has_more": after_cursor is not None, "after_cursor": after_cursor, "before_cursor": None},
    }


class MockHTTP:
    """Queues JSON bodies for http.request_stream and records each request's params."""

    def __init__(self, *pages: dict[str, Any]) -> None:
        self._pages = list(pages)
        self.params: list[dict[str, Any]] = []

    async def request_stream(self, log: Any, url: str, **kwargs: Any) -> tuple[dict[str, str], Callable[[], AsyncGenerator[bytes, None]]]:
        self.params.append(dict(kwargs["params"]))
        body = json.dumps(self._pages.pop(0)).encode()

        async def stream() -> AsyncGenerator[bytes, None]:
            yield body

        return {}, stream


class FakeChildren:
    """Yields one child per ticket id and records each batch it was asked for."""

    def __init__(self) -> None:
        self.batches: list[list[int]] = []

    async def __call__(self, ticket_ids: list[int], log: logging.Logger) -> AsyncGenerator[str, None]:
        self.batches.append(list(ticket_ids))
        for ticket_id in ticket_ids:
            yield f"child-{ticket_id}"


def _pages(log_cursor: datetime, now: datetime) -> MockHTTP:
    second = timedelta(seconds=1)
    return MockHTTP(
        _page([_ticket(1, log_cursor + second), _ticket(2, log_cursor + 2 * second)], after_cursor="c1"),
        _page([_ticket(3, log_cursor + 2 * second), _ticket(4, now)], after_cursor=None),
    )


@pytest.mark.asyncio
async def test_children_precede_each_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("source_zendesk_support_native.api.incremental_keyset_paginated.CHECKPOINT_INTERVAL", 1)

    now = datetime.now(tz=UTC).replace(microsecond=0)
    log_cursor = now - timedelta(hours=1)
    second = timedelta(seconds=1)
    children = FakeChildren()
    http = _pages(log_cursor, now)

    results = [
        r async for r in fetch_incremental_keyset_paginated_ticket_children(
            http,  # type: ignore[arg-type]
            "example", children, INCREMENTAL_LAG, log, log_cursor,
        )
    ]

    assert results == ["child-1", log_cursor + second, "child-2", "child-3", log_cursor + 2 * second]
    assert children.batches == [[1], [2, 3]]
    assert all(params["support_type_scope"] == "all" for params in http.params)


@pytest.mark.asyncio
async def test_backfill_yields_int_pages_and_no_final_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("source_zendesk_support_native.api.incremental_keyset_paginated.CHECKPOINT_INTERVAL", 1)

    now = datetime.now(tz=UTC).replace(microsecond=0)
    start_date = now - timedelta(hours=1)
    children = FakeChildren()

    results = [
        r async for r in backfill_incremental_keyset_paginated_ticket_children(
            _pages(start_date, now),  # type: ignore[arg-type]
            "example", children, start_date, log, None, now - timedelta(minutes=5),
        )
    ]

    assert results == ["child-1", dt_to_s(start_date + timedelta(seconds=1)), "child-2", "child-3"]


@pytest.mark.asyncio
async def test_empty_sweep_yields_nothing() -> None:
    log_cursor = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(hours=1)
    children = FakeChildren()

    results = [
        r async for r in fetch_incremental_keyset_paginated_ticket_children(
            MockHTTP(_page([], after_cursor=None)),  # type: ignore[arg-type]
            "example", children, INCREMENTAL_LAG, log, log_cursor,
        )
    ]

    assert results == []
    assert children.batches == []


class RecordingHTTP:
    """Returns queued JSON bodies from http.request and records each request's params."""

    def __init__(self, *bodies: dict[str, Any]) -> None:
        self._bodies = list(bodies)
        self.params: list[dict[str, Any]] = []

    async def request(self, log: Any, url: str, **kwargs: Any) -> bytes:
        self.params.append(dict(kwargs.get("params") or {}))
        return json.dumps(self._bodies.pop(0)).encode()


@pytest.mark.asyncio
async def test_metric_sets_are_fetched_100_tickets_at_a_time() -> None:
    ticket_ids = list(range(1, 151))
    http = RecordingHTTP(
        {"tickets": [], "metric_sets": [{"id": i, "ticket_id": i} for i in range(1, 101)], "next_page": None},
        {"tickets": [], "metric_sets": [{"id": i, "ticket_id": i} for i in range(101, 151)], "next_page": None},
    )

    results = [
        r async for r in fetch_metric_sets_of_tickets(
            http,  # type: ignore[arg-type]
            "example", ticket_ids, log,
        )
    ]

    assert [r.id for r in results] == ticket_ids
    assert [p["ids"] for p in http.params] == [
        ",".join(str(i) for i in range(1, 101)),
        ",".join(str(i) for i in range(101, 151)),
    ]


@pytest.mark.asyncio
async def test_metric_events_of_a_ticket_are_flattened() -> None:
    http = RecordingHTTP({
        "reply_time": [{"id": 1, "ticket_id": 7, "metric": "reply_time"}, {"id": 2, "ticket_id": 7, "metric": "reply_time"}],
        "resolution_time": [{"id": 3, "ticket_id": 7, "metric": "resolution_time"}],
    })

    results = [
        r async for r in fetch_metric_events_of_tickets(
            http,  # type: ignore[arg-type]
            "example", [7], log,
        )
    ]

    assert [r.id for r in results] == [1, 2, 3]
