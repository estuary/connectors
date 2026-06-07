"""Unit tests for the capture logic in source_zuora.api.

These exercise cursor advancement, intermediate checkpointing, the LAG settle
delay, the window-shrinking bisect on too-large exports, and query construction —
none of which the spec/discover snapshot tests touch. A FakeManager stands in for
ExportManager so tests run without a live Zuora tenant.
"""

import logging
import re
from datetime import UTC, datetime, timedelta, timezone

import pytest

from estuary_cdk.incremental_csv_processor import BaseCSVRow

from source_zuora import api
from source_zuora.export_manager import ExportTooLargeError
from source_zuora.models import (
    TransactionDateDocument,
    UpdatedDateDocument,
    ZuoraDocument,
)

_FMT = "%Y-%m-%dT%H:%M:%SZ"
_LOG = logging.getLogger(__name__)


def _fmt(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(_FMT)


def _parse(s: str) -> datetime:
    return datetime.strptime(s, _FMT).replace(tzinfo=UTC)


class FakeManager:
    """Mimics ExportManager.export_rows: parses the query's cursor window (keyed
    on cursor_field, default UpdatedDate), yields matching records sorted
    ascending (as ORDER BY <cursor_field> would), and optionally raises
    ExportTooLargeError for windows wider than too_large_over.
    """

    def __init__(
        self,
        records: list[tuple[str, datetime]] | None = None,
        too_large_over: timedelta | None = None,
        emit_millis: bool = False,
        cursor_field: str = "UpdatedDate",
    ):
        self.records = sorted(records or [], key=lambda r: r[1])
        self.too_large_over = too_large_over
        # When set, output cursor values carry milliseconds, as Zuora's export
        # does. The query filter is still parsed at whole-second precision (as
        # Export ZOQL only accepts second literals), so a sub-second record sits
        # above a second-granular >= bound — the realistic mismatch the
        # second-aligned checkpoint logic has to handle.
        self.emit_millis = emit_millis
        self.cursor_field = cursor_field
        self.queries: list[str] = []

    async def export_rows(self, query: str):
        self.queries.append(query)
        lo = re.search(rf"{self.cursor_field} >= '([^']+)'", query)
        hi = re.search(rf"{self.cursor_field} < '([^']+)'", query)
        start = _parse(lo.group(1)) if lo else None
        end = _parse(hi.group(1)) if hi else None

        if (
            self.too_large_over is not None
            and start is not None
            and end is not None
            and (end - start) > self.too_large_over
        ):
            raise ExportTooLargeError("too large")

        out_fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if self.emit_millis else _FMT
        for rid, dt in self.records:
            if (start is None or dt >= start) and (end is None or dt < end):
                yield {"Id": rid, self.cursor_field: dt.astimezone(UTC).strftime(out_fmt)}


async def _collect(agen) -> list:
    return [item async for item in agen]


def _docs(items: list) -> list:
    return [i for i in items if isinstance(i, (ZuoraDocument, BaseCSVRow))]


def _cursors(items: list) -> list:
    return [i for i in items if not isinstance(i, (ZuoraDocument, BaseCSVRow))]


# --- build_query ---------------------------------------------------------------


def test_build_query_no_bounds_is_unordered_full_table():
    q = api.build_query("Product", ["Id", "Name"])
    assert q == "SELECT Id, Name FROM Product"


def test_build_query_bounds_add_where_and_order_by():
    q = api.build_query(
        "Account",
        ["Id"],
        after=datetime(2020, 1, 1, tzinfo=UTC),
        before=datetime(2020, 2, 1, tzinfo=UTC),
    )
    assert q == (
        "SELECT Id FROM Account WHERE UpdatedDate >= '2020-01-01T00:00:00Z' "
        "AND UpdatedDate < '2020-02-01T00:00:00Z' ORDER BY UpdatedDate"
    )


def test_build_query_uses_named_cursor_field():
    q = api.build_query(
        "PaymentTransactionLog",
        ["Id"],
        "TransactionDate",
        after=datetime(2020, 1, 1, tzinfo=UTC),
        before=datetime(2020, 2, 1, tzinfo=UTC),
    )
    assert q == (
        "SELECT Id FROM PaymentTransactionLog "
        "WHERE TransactionDate >= '2020-01-01T00:00:00Z' "
        "AND TransactionDate < '2020-02-01T00:00:00Z' ORDER BY TransactionDate"
    )


def test_build_query_normalizes_non_utc_bounds_to_utc():
    plus_ten = timezone(timedelta(hours=10))
    q = api.build_query(
        "Account", ["Id"], after=datetime(2020, 1, 1, tzinfo=plus_ten)
    )
    assert "UpdatedDate >= '2019-12-31T14:00:00Z'" in q


# --- fetch_changes -------------------------------------------------------------


async def _run_changes(manager, cursor, model=UpdatedDateDocument, object_name="Account"):
    return await _collect(
        api.fetch_changes(
            object_name, ["Id", model.CURSOR_FIELD], model, manager, _LOG, cursor
        )
    )


@pytest.mark.asyncio
async def test_fetch_changes_cursor_is_max_updated_plus_one_second():
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=10)
    records = [("1", base), ("2", base + timedelta(seconds=30))]
    out = await _run_changes(FakeManager(records), base - timedelta(seconds=5))

    assert [d.Id for d in _docs(out)] == ["1", "2"]
    cursors = _cursors(out)
    assert cursors[-1] == base + timedelta(seconds=31)  # max UpdatedDate + 1s
    assert cursors[-1] > base - timedelta(seconds=5)  # strictly increasing


@pytest.mark.asyncio
async def test_fetch_changes_emits_intermediate_checkpoints(monkeypatch):
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=10)
    # seconds 0,0,10,20 — checkpoints land only at second boundaries
    records = [(str(i), base + timedelta(seconds=s)) for i, s in enumerate([0, 0, 10, 20])]
    out = await _run_changes(FakeManager(records), base - timedelta(seconds=5))

    cursors = _cursors(out)
    assert len(cursors) >= 2  # at least one intermediate + the final
    assert cursors == sorted(cursors) and len(set(cursors)) == len(cursors)
    # every checkpoint is a whole-second boundary (a record second + 1s)
    for c in cursors:
        assert (c - base).total_seconds() in {1, 11, 21}


@pytest.mark.asyncio
async def test_fetch_changes_no_checkpoint_splits_a_single_second(monkeypatch):
    # Four distinct sub-second values all within second 5. Even though the count
    # exceeds CHECKPOINT_INTERVAL, no intermediate checkpoint may split the second
    # (the whole-second-only filter couldn't resume inside it), so only the final
    # cursor is emitted — and it floors the ms max (5.900) to its second + 1s.
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=10)
    records = [
        ("a", base + timedelta(seconds=5, milliseconds=100)),
        ("b", base + timedelta(seconds=5, milliseconds=400)),
        ("c", base + timedelta(seconds=5, milliseconds=700)),
        ("d", base + timedelta(seconds=5, milliseconds=900)),
    ]
    out = await _run_changes(FakeManager(records, emit_millis=True), base)

    assert [d.Id for d in _docs(out)] == ["a", "b", "c", "d"]
    cursors = _cursors(out)
    assert cursors == [base + timedelta(seconds=6)]


@pytest.mark.asyncio
async def test_fetch_changes_checkpoints_are_second_aligned_on_ms_data(monkeypatch):
    # Sub-second values in second 5 then a jump to second 9. Crossing the second
    # boundary (with the interval reached) emits an intermediate cursor floored to
    # floor(5.800)+1s = second 6; the final floors the ms max (9.600) to second 10.
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=10)
    records = [
        ("a", base + timedelta(seconds=5, milliseconds=200)),
        ("b", base + timedelta(seconds=5, milliseconds=800)),
        ("c", base + timedelta(seconds=9, milliseconds=100)),
        ("d", base + timedelta(seconds=9, milliseconds=600)),
    ]
    out = await _run_changes(FakeManager(records, emit_millis=True), base)

    cursors = _cursors(out)
    # Every emitted cursor is on a whole-second boundary so it round-trips the
    # second-granular filter exactly (the property that makes resume dupe-free).
    assert all(c.microsecond == 0 for c in cursors)
    assert base + timedelta(seconds=6) in cursors  # intermediate
    assert cursors[-1] == base + timedelta(seconds=10)  # final


@pytest.mark.asyncio
async def test_fetch_changes_empty_full_window_advances():
    # log_cursor > MAX_EXPORT_WINDOW behind now-LAG -> full window, empty -> advance
    cursor = datetime.now(UTC).replace(microsecond=0) - timedelta(days=60)
    out = await _run_changes(FakeManager([]), cursor)
    assert out == [cursor + api.MAX_EXPORT_WINDOW]


@pytest.mark.asyncio
async def test_fetch_changes_empty_trailing_window_does_not_advance():
    # within MAX_EXPORT_WINDOW of now-LAG -> window_end == now-LAG -> no cursor
    cursor = datetime.now(UTC).replace(microsecond=0) - timedelta(days=2)
    out = await _run_changes(FakeManager([]), cursor)
    assert out == []


@pytest.mark.asyncio
async def test_fetch_changes_holds_back_within_lag():
    # cursor inside the LAG window -> nothing settled to read yet
    cursor = datetime.now(UTC).replace(microsecond=0) - (api.LAG // 2)
    manager = FakeManager([])
    out = await _run_changes(manager, cursor)
    assert out == []
    assert manager.queries == []  # no export attempted


@pytest.mark.asyncio
async def test_fetch_changes_transaction_date_cursor():
    # An append-only log cursors on TransactionDate: the query must filter and
    # order by TransactionDate, docs validate as TransactionDateDocument, and the
    # cursor advances off get_cursor() just as the UpdatedDate path does.
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=10)
    records = [("1", base), ("2", base + timedelta(seconds=30))]
    manager = FakeManager(records, cursor_field="TransactionDate")
    out = await _run_changes(
        manager, base - timedelta(seconds=5),
        model=TransactionDateDocument, object_name="PaymentTransactionLog",
    )

    assert [d.Id for d in _docs(out)] == ["1", "2"]
    assert all(isinstance(d, TransactionDateDocument) for d in _docs(out))
    assert "TransactionDate >= " in manager.queries[0]
    assert "ORDER BY TransactionDate" in manager.queries[0]
    assert _cursors(out)[-1] == base + timedelta(seconds=31)  # max cursor + 1s


# --- fetch_page ----------------------------------------------------------------


async def _run_page(manager, start_date, page, cutoff, model=UpdatedDateDocument):
    return await _collect(
        api.fetch_page(
            "Account", ["Id", model.CURSOR_FIELD], model, manager, start_date, _LOG, page, cutoff
        )
    )


@pytest.mark.asyncio
async def test_fetch_page_none_starts_at_start_date():
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=5)
    manager = FakeManager([("1", start + timedelta(days=1))])
    await _run_page(manager, start, None, cutoff)
    assert f"UpdatedDate >= '{_fmt(start)}'" in manager.queries[0]


@pytest.mark.asyncio
async def test_fetch_page_resumes_from_page_cursor():
    start = datetime(2020, 1, 1, tzinfo=UTC)
    resume = datetime(2020, 1, 10, tzinfo=UTC)
    cutoff = datetime(2020, 3, 1, tzinfo=UTC)
    manager = FakeManager([])
    await _run_page(manager, start, resume.isoformat(), cutoff)
    assert f"UpdatedDate >= '{_fmt(resume)}'" in manager.queries[0]


@pytest.mark.asyncio
async def test_fetch_page_non_final_window_ends_with_page_cursor():
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=100)  # window_end = start+30d < cutoff
    out = await _run_page(FakeManager([("1", start + timedelta(days=1))]), start, None, cutoff)
    assert out[-1] == (start + api.MAX_EXPORT_WINDOW).isoformat()


@pytest.mark.asyncio
async def test_fetch_page_final_window_ends_on_documents():
    # window reaches cutoff -> ends on docs (no PageCursor) so backfill completes
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=5)
    out = await _run_page(FakeManager([("1", start + timedelta(days=1))]), start, None, cutoff)
    assert isinstance(out[-1], ZuoraDocument)


@pytest.mark.asyncio
async def test_fetch_page_empty_final_window_completes():
    # Reaches cutoff with no rows -> no docs, no PageCursor -> backfill complete.
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=5)  # window_end == cutoff
    out = await _run_page(FakeManager([]), start, None, cutoff)
    assert out == []


@pytest.mark.asyncio
async def test_fetch_page_window_start_at_cutoff_returns_nothing():
    start = datetime(2020, 1, 1, tzinfo=UTC)
    out = await _run_page(FakeManager([]), start, None, start)  # cutoff == start
    assert out == []


@pytest.mark.asyncio
async def test_fetch_page_intermediate_cursors_are_isoformat_strings(monkeypatch):
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=5)
    records = [(str(i), start + timedelta(seconds=s)) for i, s in enumerate([0, 0, 10, 20])]
    out = await _run_page(FakeManager(records), start, None, cutoff)
    intermediate = [x for x in out if isinstance(x, str)]
    assert intermediate  # at least one intermediate PageCursor
    for c in intermediate:
        datetime.fromisoformat(c)  # parses as an ISO string


@pytest.mark.asyncio
async def test_fetch_page_intermediate_cursor_is_second_aligned_on_ms_data(monkeypatch):
    # Sub-second values in second 1 then a jump to second 4. The intermediate
    # PageCursor floors floor(1.800)+1s = second 2, second-aligned so it resumes
    # the whole-second filter exactly and re-reads nothing committed.
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=5)
    records = [
        ("a", start + timedelta(seconds=1, milliseconds=200)),
        ("b", start + timedelta(seconds=1, milliseconds=800)),
        ("c", start + timedelta(seconds=4, milliseconds=100)),
    ]
    out = await _run_page(FakeManager(records, emit_millis=True), start, None, cutoff)

    intermediate = [datetime.fromisoformat(x) for x in out if isinstance(x, str)]
    assert intermediate
    assert all(c.microsecond == 0 for c in intermediate)
    assert start + timedelta(seconds=2) in intermediate


# --- bisect on ExportTooLargeError --------------------------------------------


@pytest.mark.asyncio
async def test_fetch_page_shrinks_to_first_fitting_window_and_returns_early():
    # 30d window 403s; data at day 2. Shrinks 30->15->7.5d (fits), processes ONLY
    # that window, returns a continue-cursor well before the full 30d.
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=100)
    manager = FakeManager([("1", start + timedelta(days=2))], too_large_over=timedelta(days=8))
    out = await _run_page(manager, start, None, cutoff)

    assert [d.Id for d in _docs(out)] == ["1"]
    eff_end = datetime.fromisoformat(out[-1])
    assert eff_end < start + api.MAX_EXPORT_WINDOW  # returned early
    assert len(manager.queries) == 3  # 30d(403) + 15d(403) + 7.5d(ok)


@pytest.mark.asyncio
async def test_fetch_page_empty_after_shrink_still_advances():
    # data at day 20 but the fitting sub-window [start, ~7.5d) is empty; must still
    # advance so the next call reaches the data.
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(days=100)
    manager = FakeManager([("1", start + timedelta(days=20))], too_large_over=timedelta(days=8))
    out = await _run_page(manager, start, None, cutoff)
    assert not _docs(out)
    eff_end = datetime.fromisoformat(out[-1])
    assert start < eff_end < start + timedelta(days=15)


@pytest.mark.asyncio
async def test_fetch_changes_bisects_too_large_window():
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(days=40)  # full 30d window
    manager = FakeManager(
        [("1", base + timedelta(days=1))], too_large_over=timedelta(days=8)
    )
    out = await _run_changes(manager, base)
    assert [d.Id for d in _docs(out)] == ["1"]
    assert _cursors(out)[-1] > base


@pytest.mark.asyncio
async def test_export_too_large_single_second_raises():
    # An unsplittable one-second window that stays too large must fail loudly,
    # naming the object and window rather than an opaque Zuora file id.
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = start + timedelta(seconds=1)
    manager = FakeManager([], too_large_over=timedelta(0))  # everything is "too large"
    with pytest.raises(ExportTooLargeError) as exc:
        await _run_page(manager, start, None, cutoff)
    message = str(exc.value)
    assert "Account" in message
    assert "2020-01-01T00:00:00Z" in message


# --- fetch_snapshot ------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_snapshot_full_table_no_bounds():
    manager = FakeManager([("1", datetime(2020, 1, 1, tzinfo=UTC))])
    out = await _collect(api.fetch_snapshot("Product", ["Id", "Name"], manager, _LOG))  # type: ignore[arg-type]
    assert all(isinstance(d, BaseCSVRow) for d in out)
    assert "WHERE" not in manager.queries[0]  # full table, unbounded


@pytest.mark.asyncio
async def test_fetch_snapshot_too_large_propagates():
    # A snapshot has no time cursor, so it can't be bisected — a too-large export
    # surfaces loudly rather than being silently truncated.
    class TooBig:
        async def export_rows(self, query):
            raise ExportTooLargeError("too big")
            yield  # unreachable; makes this an async generator

    with pytest.raises(ExportTooLargeError):
        await _collect(api.fetch_snapshot("Product", ["Id"], TooBig(), _LOG))  # type: ignore[arg-type]
