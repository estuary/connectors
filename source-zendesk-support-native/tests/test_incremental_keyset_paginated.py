import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, AsyncGenerator, Callable

import pytest

from source_zendesk_support_native.api.incremental_keyset_paginated import (
    _build_seek_cursor,
    fetch_incremental_keyset_paginated_resources,
    fetch_incremental_keyset_paginated_ids_by_page,
    fetch_incremental_keyset_paginated_window,
)
from source_zendesk_support_native.api.shared import s_to_dt
from source_zendesk_support_native.models import IdAndUpdatedAt, OrganizationsListResponse, TimestampedResource

log = logging.getLogger(__name__)


def _org(id: int, updated_at: datetime) -> dict[str, Any]:
    return {"id": id, "updated_at": updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _page(orgs: list[dict[str, Any]], after_cursor: str | None) -> dict[str, Any]:
    return {
        "organizations": orgs,
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


async def _fetch_changes(http: MockHTTP, log_cursor: datetime) -> list[TimestampedResource | Any]:
    return [
        r async for r in fetch_incremental_keyset_paginated_resources(
            http,  # type: ignore[arg-type]
            "example", "organizations", "organizations", OrganizationsListResponse,
            log, log_cursor,
        )
    ]


def test_build_seek_cursor_matches_zendesk() -> None:
    # Bruno "organizations crafted seek" sent this cursor for (1727211934, 0).
    expected = "eyJvIjoidXBkYXRlZF9hdCxpZCIsInYiOiJaSjRwODJZQUFBQUFhUUFBQUFBQUFBQUEifQ=="

    assert _build_seek_cursor(s_to_dt(1727211933)) == expected
    assert _build_seek_cursor(s_to_dt(1727211933) + timedelta(microseconds=500_000)) == expected


@pytest.mark.asyncio
async def test_checkpoints_only_between_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("source_zendesk_support_native.api.incremental_keyset_paginated.CHECKPOINT_INTERVAL", 1)

    now = datetime.now(tz=UTC).replace(microsecond=0)
    log_cursor = now - timedelta(hours=1)
    second = timedelta(seconds=1)

    # The second at log_cursor + 2 spans both pages, so no checkpoint can land between them.
    http = MockHTTP(
        _page([_org(1, log_cursor + second), _org(2, log_cursor + 2 * second), _org(3, log_cursor + 2 * second)], after_cursor="c1"),
        _page([_org(4, log_cursor + 2 * second), _org(5, log_cursor + 3 * second), _org(6, now)], after_cursor="c2"),
    )

    results = await _fetch_changes(http, log_cursor)

    assert [r.id if isinstance(r, TimestampedResource) else r for r in results] == [
        1, log_cursor + second, 2, 3, 4, log_cursor + 2 * second, 5, log_cursor + 3 * second,
    ]

    assert http.params == [
        {"sort": "updated_at", "page[size]": 100, "page[after]": _build_seek_cursor(log_cursor)},
        {"sort": "updated_at", "page[size]": 100, "page[after]": "c1"},
    ]


@pytest.mark.asyncio
async def test_empty_sweep_does_not_checkpoint() -> None:
    log_cursor = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(hours=1)

    http = MockHTTP(_page([], after_cursor=None))

    assert await _fetch_changes(http, log_cursor) == []


@pytest.mark.asyncio
async def test_out_of_order_records_raise() -> None:
    log_cursor = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(hours=1)

    http = MockHTTP(
        _page([_org(1, log_cursor + timedelta(seconds=3)), _org(2, log_cursor + timedelta(seconds=2))], after_cursor=None),
    )

    with pytest.raises(RuntimeError, match="out of order"):
        await _fetch_changes(http, log_cursor)


class ChunkedHTTP:
    """Streams each queued JSON body in small chunks and records whether its stream is still open."""

    def __init__(self, *pages: dict[str, Any]) -> None:
        self._pages = list(pages)
        self.open_streams: list[bool] = []

    def any_open(self) -> bool:
        return any(self.open_streams)

    async def request_stream(self, log: Any, url: str, **kwargs: Any) -> tuple[dict[str, str], Callable[[], AsyncGenerator[bytes, None]]]:
        body = json.dumps(self._pages.pop(0)).encode()
        index = len(self.open_streams)
        self.open_streams.append(True)

        async def stream() -> AsyncGenerator[bytes, None]:
            try:
                for start in range(0, len(body), 8192):
                    yield body[start:start + 8192]
            finally:
                self.open_streams[index] = False

        return {}, stream


@pytest.mark.asyncio
async def test_ids_by_page_never_yields_with_a_response_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("source_zendesk_support_native.api.incremental_keyset_paginated.CHECKPOINT_INTERVAL", 1)

    now = datetime.now(tz=UTC).replace(microsecond=0)
    lower_bound = now - timedelta(hours=1)
    second = timedelta(seconds=1)

    # Records bigger than the JSON parser's 64 KiB reads, so a streaming read
    # yields before it reaches the end of the body.
    def big_org(id: int, updated_at: datetime) -> dict[str, Any]:
        return {**_org(id, updated_at), "notes": "x" * 100_000}

    def pages() -> ChunkedHTTP:
        return ChunkedHTTP(
            _page([big_org(1, lower_bound + second), big_org(2, lower_bound + 2 * second), big_org(3, lower_bound + 2 * second)], after_cursor="c1"),
            _page([big_org(4, lower_bound + 3 * second), big_org(5, now)], after_cursor=None),
        )

    async def run(results: AsyncGenerator[Any, None], http: ChunkedHTTP) -> tuple[list[Any], bool, list[Any]]:
        ids_and_checkpoints: list[Any] = []
        records: list[Any] = []
        yielded_while_open = False
        async for r in results:
            yielded_while_open = yielded_while_open or http.any_open()
            if isinstance(r, datetime):
                ids_and_checkpoints.append(r)
            else:
                ids_and_checkpoints.append(r.id)
                records.append(r)
        return ids_and_checkpoints, yielded_while_open, records

    http = pages()
    streamed, streamed_while_open, _ = await run(fetch_incremental_keyset_paginated_window(
        http,  # type: ignore[arg-type]
        "example", "organizations", "organizations", OrganizationsListResponse, TimestampedResource, lower_bound, now, log,
    ), http)

    http = pages()
    by_page, by_page_while_open, by_page_records = await run(fetch_incremental_keyset_paginated_ids_by_page(
        http,  # type: ignore[arg-type]
        "example", "organizations", "organizations", {}, lower_bound, now, log,
    ), http)

    assert by_page == streamed == [1, lower_bound + second, 2, 3, lower_bound + 2 * second, 4, lower_bound + 3 * second, 5]
    assert streamed_while_open
    assert not by_page_while_open
    assert all(isinstance(r, IdAndUpdatedAt) and not r.model_extra for r in by_page_records)
