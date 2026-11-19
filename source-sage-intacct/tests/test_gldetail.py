import functools
from datetime import UTC, date, datetime, timedelta, timezone
from logging import Logger
from typing import Any
from unittest.mock import MagicMock

import estuary_cdk.emitted_changes_cache as cache
import pytest
from estuary_cdk.capture.backfill_signals import BackfillCoverage

from source_sage_intacct.api import (
    backfill_gldetail,
    fetch_gldetail,
)
from source_sage_intacct.models import (
    GLDetailBackfillRecord,
    GLDetailDocument,
    GLDetailRecord,
)
from source_sage_intacct.resources import (
    LOOKBACK,
    LOOKBACK_LAG_GLDETAIL,
    REALTIME,
    REALTIME_LAG,
    all_resources,
)
from source_sage_intacct.sage import (
    get_gldetail_batch_request,
    get_gldetail_page_request,
)

from .sage_fixtures import fixture_rows, lookup_fields
from .test_gl import fake_sage, make_config, make_sage, open_subtasks  # noqa: F401
from .test_xml_requests import (
    assert_one_condition_under_filter,
    cfg,
    fields,
    sanitize_xml,
    session_id,
)

CUTOFF = datetime(2024, 6, 1, tzinfo=UTC)
OLD = datetime(2024, 1, 1, tzinfo=UTC)

# Standard GLDETAIL fields with the types that `lookup` reports.
GLDETAIL_LOOKUP = [
    ("RECORDNO", "TEXT"),
    ("BATCHKEY", "INTEGER"),
    ("GLENTRYKEY", "INTEGER"),
    ("ENTRY_DATE", "DATE"),
    ("BOOKID", "TEXT"),
    ("ADJ", "BOOLEAN"),
    ("AMOUNT", "DECIMAL"),
    ("WHENCREATED", "DATE"),
    ("WHENMODIFIED", "TIMESTAMP"),
]


def row(batch: int, line: int, modified: datetime | None = OLD) -> MagicMock:
    """A record as `Sage` returns it: only `model_dump` is used."""
    values: dict[str, Any] = {
        "RECORDNO": f"{batch}-{line}---A",
        "BATCHKEY": batch,
        "GLENTRYKEY": line,
    }
    if modified is not None:
        values["WHENMODIFIED"] = modified
    rec = MagicMock()
    rec.model_dump.return_value = values
    return rec


class PagedSage:
    """Serves GLDETAIL rows as Sage does: filtered, sorted, one page a call."""

    def __init__(self, rows: list[Any], page_size: int):
        self.rows = rows
        self.page_size = page_size
        self.calls: list[tuple] = []

    async def fetch_gldetail_page(self, from_batch_key: int | None):
        self.calls.append(("page", from_batch_key))
        matched = sorted(
            (
                values
                for values in (r.model_dump() for r in self.rows)
                if from_batch_key is None or values["BATCHKEY"] >= from_batch_key
            ),
            key=lambda v: v["BATCHKEY"],
        )
        for values in matched[: self.page_size]:
            yield row_from(values)

    async def fetch_gldetail_batch(self, batch_key: int, after: str | None):
        self.calls.append(("batch", batch_key, after))
        matched = sorted(
            (
                values
                for values in (r.model_dump() for r in self.rows)
                if values["BATCHKEY"] == batch_key
                and (after is None or values["RECORDNO"] > after)
            ),
            key=lambda v: v["RECORDNO"],
        )
        for values in matched[: self.page_size]:
            yield row_from(values)


def row_from(values: dict[str, Any]) -> MagicMock:
    rec = MagicMock()
    rec.model_dump.return_value = values
    return rec


async def run_backfill(sage: PagedSage, cutoff: datetime = CUTOFF) -> list[str]:
    """Runs the backfill as the CDK does: one call for each page cursor.
    Returns the RECORDNO of each emitted document, in order."""
    emitted: list[str] = []
    page: int | None = None
    for _ in range(100):
        next_page = None
        async for item in backfill_gldetail(
            sage,  # type: ignore[arg-type]
            sage.page_size,
            MagicMock(spec=Logger),
            page,
            cutoff,
        ):
            if isinstance(item, GLDetailBackfillRecord):
                assert next_page is None, "a document followed the page cursor"
                emitted.append(item.RECORDNO)
            else:
                assert isinstance(item, int)
                assert page is None or item > page
                next_page = item
        if next_page is None:
            return emitted
        page = next_page
    raise AssertionError("the backfill did not end")


@pytest.fixture(autouse=True)
def reset_cache():
    cache.cleanup("GLDETAIL", datetime.now(UTC))
    yield


@pytest.mark.asyncio
async def test_backfill_emits_whole_batches_and_repeats_nothing():
    # Page size 4. Batch 2 crosses the first page, batch 3 the second.
    rows = [
        row(1, 1),
        row(1, 2),
        row(2, 3),
        row(2, 4),
        row(2, 5),
        row(3, 6),
        row(3, 7),
        row(4, 8),
    ]
    sage = PagedSage(rows, page_size=4)

    emitted = await run_backfill(sage)

    assert emitted == [r.model_dump()["RECORDNO"] for r in rows]
    # Each page starts at the batch that the page before it did not finish.
    assert sage.calls == [("page", None), ("page", 2), ("page", 3)]


@pytest.mark.asyncio
async def test_backfill_page_with_one_row_fewer_than_a_page_ends():
    rows = [row(1, 1), row(2, 2), row(2, 3)]
    sage = PagedSage(rows, page_size=4)

    assert await run_backfill(sage) == ["1-1---A", "2-2---A", "2-3---A"]
    assert sage.calls == [("page", None)]


@pytest.mark.asyncio
async def test_backfill_exactly_full_last_page_reads_its_last_batch_again():
    rows = [row(1, 1), row(1, 2), row(2, 3), row(2, 4)]
    sage = PagedSage(rows, page_size=4)

    assert await run_backfill(sage) == ["1-1---A", "1-2---A", "2-3---A", "2-4---A"]
    assert sage.calls == [("page", None), ("page", 2)]


@pytest.mark.asyncio
async def test_backfill_reads_a_batch_larger_than_a_page_by_recordno():
    # Batch 2 has 9 rows, more than two pages of 4.
    rows = [row(1, 1)] + [row(2, n) for n in range(10, 19)] + [row(3, 30)]
    sage = PagedSage(rows, page_size=4)

    emitted = await run_backfill(sage)

    assert sorted(emitted) == sorted(r.model_dump()["RECORDNO"] for r in rows)
    assert len(emitted) == len(set(emitted))
    assert sage.calls == [
        ("page", None),
        ("page", 2),
        ("batch", 2, None),
        ("batch", 2, "2-13---A"),
        ("batch", 2, "2-17---A"),
        ("page", 3),
    ]


@pytest.mark.asyncio
async def test_backfill_stops_when_a_large_batch_makes_no_progress():
    # A source that ignores the RECORDNO condition returns the same page again.
    rows = [row(2, n) for n in range(10, 19)]
    sage = PagedSage(rows, page_size=4)

    async def same_page(batch_key: int, after: str | None):
        sage.calls.append(("batch", batch_key, after))
        for rec in rows[:4]:
            yield rec

    sage.fetch_gldetail_batch = same_page  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="no new row"):
        await run_backfill(sage)
    assert len(sage.calls) == 3


@pytest.mark.asyncio
async def test_backfill_stops_when_a_page_is_not_in_batchkey_order():
    # A page cursor that does not move forward would repeat the request.
    sage = PagedSage([], page_size=4)

    async def unsorted_page(from_batch_key):
        for rec in (row(7, 1), row(7, 2), row(6, 3), row(5, 4)):
            yield rec

    sage.fetch_gldetail_page = unsorted_page  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="not in BATCHKEY order"):
        _ = [
            item
            async for item in backfill_gldetail(
                sage,  # type: ignore[arg-type]
                4,
                MagicMock(spec=Logger),
                5,
                CUTOFF,
            )
        ]


@pytest.mark.asyncio
async def test_backfill_skips_rows_at_or_after_the_cutoff():
    rows = [
        row(1, 1, CUTOFF - timedelta(seconds=1)),
        row(1, 2, CUTOFF),
        row(1, 3, CUTOFF + timedelta(days=1)),
        # A row without WHENMODIFIED has no other sure reader, so it is kept.
        row(1, 4, None),
    ]
    sage = PagedSage(rows, page_size=10)

    assert await run_backfill(sage) == ["1-1---A", "1-4---A"]


@pytest.mark.asyncio
async def test_backfill_fails_on_a_row_without_batchkey():
    rec = MagicMock()
    rec.model_dump.return_value = {"RECORDNO": "x", "WHENMODIFIED": OLD}
    sage = PagedSage([], page_size=4)

    async def page(from_batch_key):
        yield rec

    sage.fetch_gldetail_page = page  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await run_backfill(sage)


@pytest.mark.asyncio
async def test_fetch_gldetail_keeps_text_recordno_and_moves_the_cursor():
    cursor = datetime(2024, 1, 1, tzinfo=UTC)
    first = datetime(2024, 1, 2, tzinfo=UTC)
    second = datetime(2024, 1, 3, tzinfo=UTC)
    sage = MagicMock()
    calls: list[tuple[str, datetime]] = []

    async def fetch_since(obj: str, since: datetime):
        calls.append((obj, since))
        for rec in (row(5501, 90001, first), row(5501, 90002, second)):
            yield rec

    sage.fetch_since = fetch_since

    got = [
        item
        async for item in fetch_gldetail(
            sage, None, 2000, MagicMock(spec=Logger), cursor
        )
    ]

    assert calls == [("GLDETAIL", cursor)]
    assert [type(item) for item in got] == [GLDetailRecord, GLDetailRecord, datetime]
    assert isinstance(got[0], GLDetailRecord)
    assert got[0].RECORDNO == "5501-90001---A"
    assert got[-1] == second


@pytest.mark.asyncio
async def test_fetch_gldetail_lookback_holds_rows_newer_than_its_lag():
    now = datetime.now(UTC)
    cursor = now - timedelta(days=1)
    settled = now - LOOKBACK_LAG_GLDETAIL - timedelta(minutes=5)
    recent = now - LOOKBACK_LAG_GLDETAIL + timedelta(minutes=5)
    sage = MagicMock()

    async def fetch_since(obj: str, since: datetime):
        for rec in (row(1, 1, settled), row(1, 2, recent)):
            yield rec

    sage.fetch_since = fetch_since

    got = [
        item
        async for item in fetch_gldetail(
            sage, LOOKBACK_LAG_GLDETAIL, 2000, MagicMock(spec=Logger), cursor
        )
    ]

    assert [getattr(item, "RECORDNO", item) for item in got] == ["1-1---A", settled]


@pytest.mark.asyncio
async def test_fetch_gldetail_does_not_emit_an_unchanged_row_twice():
    cursor = datetime(2024, 1, 1, tzinfo=UTC)
    modified = datetime(2024, 1, 2, tzinfo=UTC)
    sage = MagicMock()

    async def fetch_since(obj: str, since: datetime):
        yield row(1, 1, modified)

    sage.fetch_since = fetch_since
    log = MagicMock(spec=Logger)

    realtime = [i async for i in fetch_gldetail(sage, None, 2000, log, cursor)]
    lookback = [i async for i in fetch_gldetail(sage, None, 2000, log, cursor)]

    assert len(realtime) == 2
    # The lookback subtask reads the row again and only moves its cursor.
    assert lookback == [modified]


@pytest.mark.asyncio
async def test_gldetail_fixture_rows_parse():
    sage = make_sage(make_config(), {"GLDETAIL": lookup_fields("GLDETAIL")})
    model = await sage.get_model("GLDETAIL")
    rows = [
        row
        for name in (
            "full-gldetail",
            "json-gldetail",
            "page-gldetail",
            "page-gldetail-2",
        )
        for row in fixture_rows(name, "GLDETAIL")
    ]
    assert len(rows) == 4118

    for values in rows:
        dumped = model.model_validate(values).model_dump()
        doc = GLDetailBackfillRecord.model_validate(dumped)
        assert doc.RECORDNO == values["RECORDNO"]
        assert doc.RECORDNO.startswith(f"{doc.BATCHKEY}-{dumped['GLENTRYKEY']}-")
        assert GLDetailRecord.model_validate(dumped).WHENMODIFIED is not None
        assert isinstance(dumped["ADJ"], bool)


@pytest.mark.asyncio
async def test_backfill_over_the_two_fixture_pages_loses_and_repeats_nothing():
    sage = make_sage(make_config(), {"GLDETAIL": lookup_fields("GLDETAIL")})
    model = await sage.get_model("GLDETAIL")
    by_key = {
        str(values["RECORDNO"]): model.model_validate(values)
        for name in ("page-gldetail", "page-gldetail-2")
        for values in fixture_rows(name, "GLDETAIL")
    }
    # The second page reads the last batch of the first page again.
    assert len(by_key) == 3999
    paged = PagedSage(list(by_key.values()), page_size=2000)

    emitted = await run_backfill(paged, cutoff=datetime(2099, 1, 1, tzinfo=UTC))

    assert sorted(emitted) == sorted(by_key)
    assert len(paged.calls) == 3


def test_gldetail_lag_is_three_hours():
    assert LOOKBACK_LAG_GLDETAIL == timedelta(hours=3)


@pytest.mark.asyncio
async def test_gldetail_resource(monkeypatch, fake_sage):  # noqa: F811
    config = make_config()
    found = await all_resources(MagicMock(spec=Logger), MagicMock(), config)
    resource = next(r for r in found if r.name == "GLDETAIL")

    assert resource.key == ["/RECORDNO"]
    assert resource.model is GLDetailDocument
    assert resource.backfill_coverage == BackfillCoverage.FULL
    assert not resource.disable
    # No creation subtasks: each query is billed, and no row seen so far
    # lacks a WHENMODIFIED.
    names = {REALTIME, LOOKBACK}
    assert set(resource.initial_state.inc) == names
    cursors = {name: inc.cursor for name, inc in resource.initial_state.inc.items()}
    assert cursors[REALTIME] == cursors[LOOKBACK]

    opened = await open_subtasks(monkeypatch, resource)

    subtasks: dict[str, functools.partial] = opened["fetch_changes"]
    assert set(subtasks) == names
    assert subtasks[REALTIME].func is fetch_gldetail
    assert subtasks[REALTIME].args[1] == REALTIME_LAG
    assert subtasks[LOOKBACK].args[1] == LOOKBACK_LAG_GLDETAIL
    assert opened["fetch_page"].func is backfill_gldetail


@pytest.mark.asyncio
async def test_gldetail_values_parse_like_other_objects():
    sage = make_sage(make_config(), {"GLDETAIL": GLDETAIL_LOOKUP})
    model = await sage.get_model("GLDETAIL")

    # Sage returns each value as a string.
    rec = model.model_validate(
        {
            "RECORDNO": "5501-90001-7001--A",
            "BATCHKEY": "5501",
            "GLENTRYKEY": "90001",
            "ENTRY_DATE": "01/31/2024",
            "BOOKID": "ACCRUAL",
            "ADJ": "F",
            "AMOUNT": "-12.50",
            "WHENCREATED": "",
            "WHENMODIFIED": "02/01/2024 13:14:15",
        }
    )
    doc = GLDetailBackfillRecord.model_validate(rec.model_dump())

    assert doc.RECORDNO == "5501-90001-7001--A"
    assert doc.BATCHKEY == 5501
    assert doc.WHENMODIFIED == datetime(2024, 2, 1, 13, 14, 15, tzinfo=UTC)
    dumped = rec.model_dump()
    assert dumped["ADJ"] is False
    assert dumped["AMOUNT"] == "-12.50"
    assert model.sourced_schema()["properties"]["RECORDNO"] == {"type": "string"}


@pytest.mark.asyncio
async def test_gldetail_queries_filter_from_1900_and_show_private():
    sage = make_sage(make_config(), {"GLDETAIL": GLDETAIL_LOOKUP})

    async for _ in sage.fetch_gldetail_page(None):
        pass
    async for _ in sage.fetch_gldetail_page(5501):
        pass
    async for _ in sage.fetch_gldetail_batch(5501, None):
        pass
    async for _ in sage.fetch_since("GLDETAIL", datetime(2024, 5, 1, tzinfo=UTC)):
        pass

    sent = sage.sent  # type: ignore[attr-defined]
    assert len(sent) == 4
    for request in sent:
        assert "<showprivate>true</showprivate>" in request
        assert_one_condition_under_filter(request)
        for name, _ in GLDETAIL_LOOKUP:
            assert f"<field>{name}</field>" in request
    for request in sent[:3]:
        assert "<field>ENTRY_DATE</field>\n                <value>01/01/1900" in request
        # A filter on WHENMODIFIED drops the rows where it is empty.
        assert "WHENMODIFIED</field>\n                <value>" not in request
