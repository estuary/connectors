"""Runs the General Ledger bindings through the real `Sage` class and a fake
HTTP client: session, `lookup`, permission probes, and JSON query pages."""

from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import Any
from unittest.mock import MagicMock

import estuary_cdk.emitted_changes_cache as cache
import pytest

from source_sage_intacct.resources import GL_OBJECTS, all_resources

from .fake_sage_http import TIMESTAMP_FORMAT, FakeSageHTTP
from .test_gl import make_config, open_subtasks

START = datetime(2024, 1, 15, tzinfo=UTC)
CUTOFF = datetime(2025, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def reset_cache():
    for obj in GL_OBJECTS:
        cache.cleanup(obj, datetime.now(UTC))
    yield


async def open_binding(
    monkeypatch, http: FakeSageHTTP, obj: str, **advanced: Any
) -> dict[str, Any]:
    """The functions of the binding of `obj`, as given to `open_binding`."""
    found = await all_resources(
        MagicMock(spec=Logger),
        http,  # type: ignore[arg-type]
        make_config(**advanced),
    )
    resource = next(r for r in found if r.name == obj)
    return await open_subtasks(monkeypatch, resource)


async def drain(generator) -> list[Any]:
    return [item async for item in generator]


async def run_backfill(fetch_page) -> list[Any]:
    """Runs the backfill as the CDK does: one call for each page cursor."""
    documents: list[Any] = []
    page = None
    while True:
        items = await drain(fetch_page(MagicMock(spec=Logger), page, CUTOFF))
        documents += [item for item in items if not isinstance(item, int)]
        if not items or not isinstance(items[-1], int):
            return documents
        page = items[-1]


def changed_rows(count: int) -> list[dict[str, str]]:
    """`count` rows, each modified one second after the row before it."""
    return [
        {
            "RECORDNO": str(n + 1),
            "BATCH_DATE": "01/15/2024",
            "ENTRY_DATE": "01/15/2024",
            "WHENCREATED": START.strftime(TIMESTAMP_FORMAT),
            "WHENMODIFIED": (START + timedelta(seconds=n)).strftime(TIMESTAMP_FORMAT),
        }
        for n in range(count)
    ]


@pytest.mark.asyncio
async def test_discovery_finds_the_gl_bindings():
    found = await all_resources(
        MagicMock(spec=Logger),
        FakeSageHTTP(),  # type: ignore[arg-type]
        make_config(),
    )

    assert GL_OBJECTS <= {r.name for r in found}


@pytest.mark.asyncio
@pytest.mark.parametrize("obj", sorted(GL_OBJECTS))
async def test_idle_subtasks_emit_nothing(monkeypatch, obj):
    opened = await open_binding(monkeypatch, FakeSageHTTP(), obj)
    cursor = datetime.now(UTC) - timedelta(hours=6)

    for fetch in opened["fetch_changes"].values():
        assert await drain(fetch(MagicMock(spec=Logger), cursor)) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("obj", sorted(GL_OBJECTS))
async def test_incremental_reads_changes_across_pages(monkeypatch, obj):
    http = FakeSageHTTP({obj: changed_rows(4500)})
    opened = await open_binding(monkeypatch, http, obj)

    got = await drain(
        opened["fetch_changes"]["realtime"](
            MagicMock(spec=Logger), START - timedelta(seconds=1)
        )
    )

    documents = [item for item in got if not isinstance(item, datetime)]
    assert [doc.RECORDNO for doc in documents] == list(range(1, 4501))
    assert got[-1] == START + timedelta(seconds=4499)


@pytest.mark.asyncio
@pytest.mark.parametrize("obj", sorted(GL_OBJECTS))
async def test_backfill_reads_all_rows_across_pages(monkeypatch, obj):
    http = FakeSageHTTP({obj: changed_rows(4500)})
    opened = await open_binding(monkeypatch, http, obj)

    documents = await run_backfill(opened["fetch_page"])

    assert [doc.RECORDNO for doc in documents] == list(range(1, 4501))
