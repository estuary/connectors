from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, ClassVar
from unittest.mock import AsyncMock, MagicMock, call

import estuary_cdk.emitted_changes_cache as cache
import pytest
from pydantic import AwareDatetime, model_validator

from source_sage_intacct.api import fetch_changes, fetch_creations, fetch_page
from source_sage_intacct.models import (
    CreationRecord,
    IncrementalResource,
)
from source_sage_intacct.sage import PAGE_SIZE, Sage, SageRecord


async def async_iter(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


class MockRecord(SageRecord):
    RECORDNO: int
    WHENMODIFIED: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        # This overrides the value normalization of the SageRecord model to
        # simplify testing the `fetch_changes` function.
        return values


class MockCreationRecord(SageRecord):
    RECORDNO: int
    WHENCREATED: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        return values


class MockMixedRecord(SageRecord):
    """Mirrors a real Sage record where WHENMODIFIED may be null. Used to
    exercise `fetch_page`'s per-record routing between IncrementalResource
    and CreationRecord."""

    RECORDNO: int
    WHENMODIFIED: AwareDatetime | None = None
    WHENCREATED: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        return values


@pytest.fixture
def mock_sage():
    sage = AsyncMock(spec=Sage)
    return sage


@pytest.fixture
def mock_logger():
    return MagicMock(spec=Logger)


@pytest.fixture(autouse=True)
def reset_cache():
    for obj in cache.emitted_cache.keys():
        cache.cleanup(obj, datetime.now(UTC))
    yield


def test_thing():
    MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC))


@pytest.mark.asyncio
async def test_fetch_changes_spans_and_cycles(mock_sage, mock_logger):
    page_size = 3

    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    page1_records = [
        MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]

    # This page is everything later than 2023-1-2, which is a cycle of
    # timestamps at 2023-1-3.
    page2_records = [
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]

    # After everything at 2023-1-3 is read, a page starting at the next highest
    # timestamp.
    page3_records = [
        MockRecord(RECORDNO=7, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        MockRecord(RECORDNO=8, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        MockRecord(RECORDNO=9, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
    ]

    # Only able to completely verify that 2023-1-4 was read, so the 5's are back
    # on the next page.
    page4_records = [
        MockRecord(RECORDNO=8, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        MockRecord(RECORDNO=9, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        MockRecord(RECORDNO=10, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
    ]

    # The final page is smaller than the page size.
    page5_records = [
        MockRecord(RECORDNO=10, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
        MockRecord(RECORDNO=11, WHENMODIFIED=datetime(2023, 1, 7, tzinfo=UTC)),
    ]

    # When breaking the cycle at 2023-1-3, there are two pages, sorted by
    # RECORDNO.
    cycle_page1_records = [
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]

    cycle_page2_records = [
        MockRecord(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=6, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]

    mock_sage.fetch_since.side_effect = [
        async_iter(page1_records),
        async_iter(page2_records),
        async_iter(page3_records),
        async_iter(page4_records),
        async_iter(page5_records),
    ]

    mock_sage.fetch_at.side_effect = [
        async_iter(cycle_page1_records),
        async_iter(cycle_page2_records),
    ]

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, page_size, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 5
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
        call("customer", datetime(2023, 1, 2, tzinfo=UTC)),
        call("customer", datetime(2023, 1, 3, tzinfo=UTC)),
        call("customer", datetime(2023, 1, 4, tzinfo=UTC)),
        call("customer", datetime(2023, 1, 5, tzinfo=UTC)),
    ]
    assert mock_sage.fetch_at.call_count == 2
    assert mock_sage.fetch_at.mock_calls == [
        call("customer", datetime(2023, 1, 3, tzinfo=UTC), None),
        call("customer", datetime(2023, 1, 3, tzinfo=UTC), 4),
    ]

    assert len(results) == 11 + 5  # 11 records + 5 checkpoints
    assert results == [
        IncrementalResource(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        datetime(2023, 1, 2, tzinfo=UTC),
        IncrementalResource(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=6, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        datetime(2023, 1, 3, tzinfo=UTC),
        IncrementalResource(RECORDNO=7, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        IncrementalResource(RECORDNO=8, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        IncrementalResource(RECORDNO=9, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        datetime(2023, 1, 4, tzinfo=UTC),
        IncrementalResource(RECORDNO=10, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
        datetime(2023, 1, 5, tzinfo=UTC),
        IncrementalResource(RECORDNO=11, WHENMODIFIED=datetime(2023, 1, 7, tzinfo=UTC)),
        datetime(2023, 1, 7, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_changes_no_records(mock_sage, mock_logger):
    mock_sage.fetch_since.return_value = async_iter([])
    cursor = datetime(2023, 1, 1, tzinfo=UTC)

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, PAGE_SIZE, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 1
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
    ]
    assert mock_sage.fetch_at.call_count == 0

    assert len(results) == 0


@pytest.mark.asyncio
async def test_fetch_changes_single_page(mock_sage, mock_logger):
    mock_sage.fetch_since.return_value = async_iter([])
    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    records = [
        MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]
    mock_sage.fetch_since.return_value = async_iter(records)

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, PAGE_SIZE, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 1
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
    ]
    assert mock_sage.fetch_at.call_count == 0

    assert len(results) == 4
    assert results == [
        IncrementalResource(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        datetime(2023, 1, 3, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_changes_multiple_pages(mock_sage, mock_logger):
    page_size = 3

    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    page1_records = [
        MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
    ]
    page2_records = [
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        MockRecord(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        MockRecord(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
    ]
    page3_records = [
        MockRecord(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
    ]

    mock_sage.fetch_since.side_effect = [
        async_iter(page1_records),
        async_iter(page2_records),
        async_iter(page3_records),
    ]

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, page_size, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 3
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
        call("customer", datetime(2023, 1, 3, tzinfo=UTC)),
        call("customer", datetime(2023, 1, 5, tzinfo=UTC)),
    ]
    assert mock_sage.fetch_at.call_count == 0

    assert len(results) == 8
    assert results == [
        IncrementalResource(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        datetime(2023, 1, 3, tzinfo=UTC),
        IncrementalResource(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        IncrementalResource(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
        datetime(2023, 1, 5, tzinfo=UTC),
        datetime(2023, 1, 6, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_changes_multiple_pages_exact(mock_sage, mock_logger):
    page_size = 3

    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    page1_records = [
        MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
    ]
    page2_records = [
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        MockRecord(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        MockRecord(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
    ]

    mock_sage.fetch_since.side_effect = [
        async_iter(page1_records),
        async_iter(page2_records),
        async_iter([]),
    ]

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, page_size, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 3
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
        call("customer", datetime(2023, 1, 3, tzinfo=UTC)),
        call("customer", datetime(2023, 1, 5, tzinfo=UTC)),
    ]
    assert mock_sage.fetch_at.call_count == 0

    assert len(results) == 7
    assert results == [
        IncrementalResource(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 3, tzinfo=UTC)),
        IncrementalResource(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 4, tzinfo=UTC)),
        datetime(2023, 1, 3, tzinfo=UTC),
        IncrementalResource(RECORDNO=4, WHENMODIFIED=datetime(2023, 1, 5, tzinfo=UTC)),
        IncrementalResource(RECORDNO=5, WHENMODIFIED=datetime(2023, 1, 6, tzinfo=UTC)),
        datetime(2023, 1, 5, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_changes_single_page_cycle(mock_sage, mock_logger):
    page_size = 3

    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    records = [
        MockRecord(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockRecord(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
    ]

    mock_sage.fetch_since.side_effect = [
        async_iter(records),
        async_iter([]),
    ]

    mock_sage.fetch_at.side_effect = [
        async_iter(records),
        async_iter([]),
    ]

    results = [
        item
        async for item in fetch_changes(
            "customer", mock_sage, None, page_size, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_since.call_count == 2
    assert mock_sage.fetch_since.mock_calls == [
        call("customer", cursor),
        call("customer", datetime(2023, 1, 2, tzinfo=UTC)),
    ]
    assert mock_sage.fetch_at.call_count == 2
    assert mock_sage.fetch_at.mock_calls == [
        call("customer", datetime(2023, 1, 2, tzinfo=UTC), None),
        call("customer", datetime(2023, 1, 2, tzinfo=UTC), 3),
    ]

    assert len(results) == 4
    assert results == [
        IncrementalResource(RECORDNO=1, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=2, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        IncrementalResource(RECORDNO=3, WHENMODIFIED=datetime(2023, 1, 2, tzinfo=UTC)),
        datetime(2023, 1, 2, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_creations_basic(mock_sage, mock_logger):
    cursor = datetime(2023, 1, 1, tzinfo=UTC)
    records = [
        MockCreationRecord(RECORDNO=1, WHENCREATED=datetime(2023, 1, 2, tzinfo=UTC)),
        MockCreationRecord(RECORDNO=2, WHENCREATED=datetime(2023, 1, 3, tzinfo=UTC)),
    ]
    mock_sage.fetch_created_since.return_value = async_iter(records)

    results = [
        item
        async for item in fetch_creations(
            "customer", mock_sage, None, PAGE_SIZE, mock_logger, cursor
        )
    ]

    assert mock_sage.fetch_created_since.call_count == 1
    assert mock_sage.fetch_created_since.mock_calls == [call("customer", cursor)]
    assert mock_sage.fetch_created_at.call_count == 0

    assert results == [
        CreationRecord(RECORDNO=1, WHENCREATED=datetime(2023, 1, 2, tzinfo=UTC)),
        CreationRecord(RECORDNO=2, WHENCREATED=datetime(2023, 1, 3, tzinfo=UTC)),
        datetime(2023, 1, 3, tzinfo=UTC),
    ]


@pytest.mark.asyncio
async def test_fetch_page_routes_by_whenmodified(mock_sage, mock_logger):
    # fetch_page must route records with WHENMODIFIED to IncrementalResource
    # and records without WHENMODIFIED to CreationRecord. The cutoff
    # comparison uses cursor_value(), which falls back to WHENCREATED when
    # WHENMODIFIED is absent.
    cutoff = datetime(2024, 1, 1, tzinfo=UTC)
    records = [
        MockMixedRecord(
            RECORDNO=1,
            WHENMODIFIED=datetime(2023, 6, 1, tzinfo=UTC),
            WHENCREATED=datetime(2023, 1, 1, tzinfo=UTC),
        ),
        MockMixedRecord(
            RECORDNO=2,
            WHENMODIFIED=None,
            WHENCREATED=datetime(2023, 7, 1, tzinfo=UTC),
        ),
        # This record is after the cutoff (via its WHENCREATED) and should
        # be skipped — leaves the door open for the incremental sub-tasks
        # to capture it.
        MockMixedRecord(
            RECORDNO=3,
            WHENMODIFIED=None,
            WHENCREATED=datetime(2024, 6, 1, tzinfo=UTC),
        ),
    ]
    mock_sage.fetch_all.return_value = async_iter(records)

    results = [
        item
        async for item in fetch_page(
            "customer", mock_sage, PAGE_SIZE, mock_logger, None, cutoff
        )
    ]

    assert len(results) == 2
    assert isinstance(results[0], IncrementalResource)
    assert results[0].RECORDNO == 1
    assert results[0].WHENMODIFIED == datetime(2023, 6, 1, tzinfo=UTC)
    assert isinstance(results[1], CreationRecord)
    assert results[1].RECORDNO == 2
    assert results[1].WHENCREATED == datetime(2023, 7, 1, tzinfo=UTC)
