"""Offline tests for the statistics report backfill and incremental sweeps.

The report window arithmetic is the only place in this connector where
correctness depends on logic rather than on the API's own shape, so it is
covered without a live Criteo account: a stub session records the windows that
were requested and streams back canned rows.
"""

import json
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any, AsyncGenerator

import pytest
from estuary_cdk.capture.common import LogCursor, PageCursor
from source_criteo import api
from source_criteo.api import backfill_report, fetch_report
from source_criteo.models import (
    ReportConfig,
    ReportRow,
    report_document_model,
)

LOG = getLogger(__name__)

REPORT = ReportConfig(
    name="MyReport",
    dimensions=["CampaignId"],
    metrics=["Clicks"],
    currency="GBP",
)
DOCUMENT_MODEL = report_document_model(REPORT)


def _rows_for(first: datetime, last: datetime) -> list[dict[str, Any]]:
    rows = []
    day = first
    while day <= last:
        rows.append({"Day": day.date().isoformat(), "CampaignId": "1", "Clicks": 1})
        day += timedelta(days=1)
    return rows


class StubSession:
    """Answers each report window with one row per day in the window."""

    def __init__(self) -> None:
        self.windows: list[tuple[str, str]] = []
        self.bodies: list[dict[str, Any]] = []

    def payload(self, body: dict[str, Any]) -> dict[str, Any]:
        first = datetime.fromisoformat(body["startDate"])
        last = datetime.fromisoformat(body["endDate"])
        return {"Rows": _rows_for(first, last)}

    async def request(self, log, url: str, **kwargs) -> bytes:
        # Only reached when `advertiser_ids` is unset and the portfolio is
        # resolved from /advertisers/me.
        return _encode({"data": []})

    async def request_stream(
        self,
        log,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ):
        assert json is not None
        self.windows.append((json["startDate"], json["endDate"]))
        self.bodies.append(json)
        encoded = _encode(self.payload(json))

        async def body() -> AsyncGenerator[bytes, None]:
            yield encoded

        return {}, body


def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode()


async def _sweep(
    session: StubSession,
    start_date: datetime,
    log_cursor: datetime,
    window_size: int = 4,
    lookback_days: int = 30,
) -> tuple[list[ReportRow], list[LogCursor]]:
    documents: list[ReportRow] = []
    cursors: list[LogCursor] = []

    async for item in fetch_report(
        session,  # type: ignore[arg-type]
        REPORT,
        DOCUMENT_MODEL,
        ["123"],
        start_date,
        window_size,
        lookback_days,
        LOG,
        log_cursor,
    ):
        if isinstance(item, ReportRow):
            documents.append(item)
        else:
            cursors.append(item)

    return documents, cursors


async def _backfill(
    session: StubSession,
    start_date: datetime,
    cutoff: datetime,
    page_cursor: PageCursor,
    window_size: int = 4,
) -> tuple[list[ReportRow], list[PageCursor]]:
    documents: list[ReportRow] = []
    cursors: list[PageCursor] = []

    async for item in backfill_report(
        session,  # type: ignore[arg-type]
        REPORT,
        DOCUMENT_MODEL,
        ["123"],
        start_date,
        window_size,
        LOG,
        page_cursor,
        cutoff,
    ):
        if isinstance(item, ReportRow):
            documents.append(item)
        else:
            cursors.append(item)

    return documents, cursors


def _days(window: tuple[str, str]) -> tuple[datetime, datetime]:
    return datetime.fromisoformat(window[0]), datetime.fromisoformat(window[1])


def _floor(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


# --- incremental sweep -------------------------------------------------------


@pytest.mark.asyncio
async def test_windows_tile_the_range_without_gaps_or_overlap():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=10)

    await _sweep(session, start, start, window_size=4, lookback_days=0)

    first_start, _ = _days(session.windows[0])
    assert first_start == _floor(start)

    for earlier, later in zip(session.windows, session.windows[1:]):
        _, earlier_end = _days(earlier)
        later_start, _ = _days(later)
        assert later_start == earlier_end + timedelta(days=1)

    for window in session.windows:
        window_start, window_end = _days(window)
        assert window_end >= window_start
        assert window_end - window_start <= timedelta(days=3)

    _, final_end = _days(session.windows[-1])
    assert final_end == _floor(datetime.now(tz=UTC))


@pytest.mark.asyncio
async def test_every_requested_day_is_emitted_exactly_once():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=9)

    documents, _ = await _sweep(session, start, start, window_size=4, lookback_days=0)

    captured_days = [document.Day for document in documents]  # type: ignore[attr-defined]
    assert len(captured_days) == len(set(captured_days)) == 10


@pytest.mark.asyncio
async def test_cursors_are_strictly_increasing_and_end_at_now():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=30)

    before = datetime.now(tz=UTC)
    _, cursors = await _sweep(session, start, start, window_size=4, lookback_days=0)
    after = datetime.now(tz=UTC)

    assert cursors == sorted(cursors)
    assert len(cursors) == len(set(cursors))
    assert before <= cursors[-1] <= after


@pytest.mark.asyncio
async def test_documents_are_always_followed_by_a_cursor():
    # The CDK raises if fetch_changes yields documents without a trailing cursor,
    # so the *last* item of the stream must be a cursor, not merely present.
    now = datetime.now(tz=UTC)
    log_cursor = now - timedelta(seconds=1)

    for lookback in (1, 2, 30):
        items = [
            item
            async for item in fetch_report(
                StubSession(),  # type: ignore[arg-type]
                REPORT,
                DOCUMENT_MODEL,
                ["123"],
                now - timedelta(days=90),
                4,
                lookback,
                LOG,
                log_cursor,
            )
        ]

        assert any(isinstance(item, ReportRow) for item in items)
        assert not isinstance(items[-1], ReportRow)
        assert items[-1] > log_cursor  # type: ignore[operator]


@pytest.mark.asyncio
async def test_lookback_rewinds_behind_the_cursor():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=365)
    cursor = datetime.now(tz=UTC) - timedelta(minutes=1)

    await _sweep(session, start, cursor, window_size=4, lookback_days=30)

    first_start, _ = _days(session.windows[0])
    assert first_start == _floor(cursor - timedelta(days=30))


@pytest.mark.asyncio
async def test_lookback_never_rewinds_past_the_start_date():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=3)
    cursor = datetime.now(tz=UTC) - timedelta(minutes=1)

    await _sweep(session, start, cursor, window_size=4, lookback_days=30)

    first_start, _ = _days(session.windows[0])
    assert first_start == _floor(start)


@pytest.mark.asyncio
async def test_a_cursor_in_the_future_is_a_no_op():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=10)
    cursor = datetime.now(tz=UTC) + timedelta(days=1)

    documents, cursors = await _sweep(session, start, cursor)

    assert session.windows == []
    assert documents == []
    assert cursors == []


@pytest.mark.asyncio
async def test_request_body_carries_the_report_configuration():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=1)

    await _sweep(session, start, start, window_size=4, lookback_days=0)

    body = session.bodies[0]
    assert body["advertiserIds"] == "123"
    assert body["dimensions"] == ["Day", "CampaignId"], "the grain leads the request"
    assert body["metrics"] == ["Clicks"]
    assert body["currency"] == "GBP"
    assert body["timezone"] == "UTC"
    assert body["format"] == "json"


@pytest.mark.asyncio
async def test_an_unexpected_response_envelope_fails_loudly():
    class ColumnarSession(StubSession):
        def payload(self, body: dict[str, Any]) -> dict[str, Any]:
            return {"columns": ["Day"], "data": [["2026-01-01"]], "rows": 1}

    start = datetime.now(tz=UTC) - timedelta(days=1)

    with pytest.raises(RuntimeError, match="did not return a `Rows` array"):
        await _sweep(ColumnarSession(), start, start, window_size=4, lookback_days=0)


@pytest.mark.asyncio
async def test_an_empty_report_is_not_an_error():
    class EmptySession(StubSession):
        def payload(self, body: dict[str, Any]) -> dict[str, Any]:
            return {"Rows": []}

    start = datetime.now(tz=UTC) - timedelta(days=1)
    documents, cursors = await _sweep(
        EmptySession(), start, start, window_size=4, lookback_days=0
    )

    assert documents == []
    assert cursors


class CappedSession(StubSession):
    """Emits `rows_per_day` rows per day, truncated at the row cap like Criteo.

    A capped response is therefore a genuine prefix of the full result set, and a
    window small enough to fit comes back whole and under the cap.
    """

    def __init__(self, rows_per_day: int) -> None:
        super().__init__()
        self.rows_per_day = rows_per_day

    def payload(self, body: dict[str, Any]) -> dict[str, Any]:
        first = datetime.fromisoformat(body["startDate"])
        last = datetime.fromisoformat(body["endDate"])

        rows = []
        day = first
        while day <= last:
            rows.extend(
                {"Day": day.date().isoformat(), "CampaignId": str(n), "Clicks": 1}
                for n in range(self.rows_per_day)
            )
            day += timedelta(days=1)

        return {"Rows": rows[: api.REPORT_ROW_LIMIT]}


@pytest.fixture
def small_row_limit(monkeypatch):
    """Shrink the row cap so splitting is exercised with a handful of rows."""
    monkeypatch.setattr(api, "REPORT_ROW_LIMIT", 7)


@pytest.mark.asyncio
async def test_a_capped_window_halves_until_it_fits(small_row_limit):
    # 3 rows/day against a cap of 7: windows of 3+ days come back capped, 2 fits.
    session = CappedSession(rows_per_day=3)
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=8)

    documents, _ = await _backfill(session, start, cutoff, None, window_size=8)

    widths = [(_days(w)[1] - _days(w)[0]).days + 1 for w in session.windows]
    assert widths == [8, 4, 2, 2, 4, 2, 2]

    captured = {d.Day for d in documents}  # type: ignore[attr-defined]
    assert captured == {
        (start + timedelta(days=offset)).date().isoformat() for offset in range(8)
    }


@pytest.mark.asyncio
async def test_a_window_that_fits_is_not_split(small_row_limit):
    session = CappedSession(rows_per_day=1)
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=4)

    await _backfill(session, start, cutoff, None, window_size=4)

    assert len(session.windows) == 1


@pytest.mark.asyncio
async def test_a_single_day_over_the_cap_fails_loudly(small_row_limit):
    # Criteo's date parameters bottom out at one day, so there is nothing left to
    # split and a possibly-truncated response must not be accepted.
    session = CappedSession(rows_per_day=8)
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=1)

    with pytest.raises(RuntimeError, match="single day"):
        await _backfill(session, start, cutoff, None, window_size=1)


@pytest.mark.asyncio
async def test_the_incremental_sweep_also_splits(small_row_limit):
    session = CappedSession(rows_per_day=3)
    start = datetime.now(tz=UTC) - timedelta(days=4)

    documents, cursors = await _sweep(
        session, start, start, window_size=4, lookback_days=1
    )

    widths = [(_days(w)[1] - _days(w)[0]).days + 1 for w in session.windows]
    assert min(widths) <= 2, "a capped window was narrowed"
    assert documents and cursors


@pytest.mark.asyncio
async def test_report_level_errors_are_propagated():
    class ErroringSession(StubSession):
        def payload(self, body: dict[str, Any]) -> dict[str, Any]:
            return {
                "Rows": [],
                "errors": [{"detail": "no access to advertiser 123"}],
            }

    start = datetime.now(tz=UTC) - timedelta(days=1)

    with pytest.raises(RuntimeError, match="no access to advertiser 123"):
        await _sweep(ErroringSession(), start, start, window_size=4, lookback_days=1)


@pytest.mark.asyncio
async def test_reports_without_advertisers_fail_loudly():
    session = StubSession()
    start = datetime.now(tz=UTC) - timedelta(days=1)

    with pytest.raises(RuntimeError, match="no advertisers are configured"):
        async for _ in fetch_report(
            session,  # type: ignore[arg-type]
            REPORT,
            DOCUMENT_MODEL,
            [],
            start,
            4,
            0,
            LOG,
            start,
        ):
            pass


# --- backfill ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_stops_one_day_before_the_cutoff():
    session = StubSession()
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=3)

    await _backfill(session, start, cutoff, None, window_size=10)

    first_start, first_end = _days(session.windows[0])
    assert first_start == start
    assert first_end == cutoff - timedelta(days=1)


@pytest.mark.asyncio
async def test_backfill_resumes_from_its_page_cursor():
    session = StubSession()
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=10)
    resume = (cutoff - timedelta(days=4)).isoformat()

    _, cursors = await _backfill(session, start, cutoff, resume, window_size=2)

    first_start, first_end = _days(session.windows[0])
    assert first_start == cutoff - timedelta(days=4)
    assert first_end == cutoff - timedelta(days=3)
    assert cursors == [(cutoff - timedelta(days=2)).isoformat()]


@pytest.mark.asyncio
async def test_backfill_walks_every_day_once_and_terminates():
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=9)

    page_cursor: PageCursor = None
    seen: list[str] = []
    invocations = 0

    while True:
        session = StubSession()
        documents, cursors = await _backfill(
            session, start, cutoff, page_cursor, window_size=4
        )
        seen.extend(document.Day for document in documents)  # type: ignore[attr-defined]
        invocations += 1
        if not cursors:
            break
        page_cursor = cursors[-1]
        assert invocations < 20, "backfill did not terminate"

    # Days [cutoff - 9, cutoff - 1] — nine days, none repeated, today excluded.
    assert len(seen) == len(set(seen)) == 9
    assert max(seen) == (cutoff - timedelta(days=1)).date().isoformat()
    assert min(seen) == start.date().isoformat()


@pytest.mark.asyncio
async def test_backfill_is_a_no_op_once_it_reaches_the_cutoff():
    session = StubSession()
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=5)

    documents, cursors = await _backfill(session, start, cutoff, cutoff.isoformat())

    assert session.windows == []
    assert documents == []
    assert cursors == []


@pytest.mark.asyncio
async def test_backfill_and_incremental_seam_is_gapless():
    # The backfill covers every day up to cutoff - 1; the incremental sweep
    # starting at the cutoff must pick up exactly at the cutoff day.
    cutoff = _floor(datetime.now(tz=UTC))
    start = cutoff - timedelta(days=5)

    backfill_session = StubSession()
    page_cursor: PageCursor = None
    backfilled: list[str] = []
    while True:
        session = StubSession()
        documents, cursors = await _backfill(
            session, start, cutoff, page_cursor, window_size=2
        )
        backfill_session.windows.extend(session.windows)
        backfilled.extend(document.Day for document in documents)  # type: ignore[attr-defined]
        if not cursors:
            break
        page_cursor = cursors[-1]

    incremental_session = StubSession()
    documents, _ = await _sweep(
        incremental_session, start, cutoff, window_size=4, lookback_days=0
    )
    incremental = [document.Day for document in documents]  # type: ignore[attr-defined]

    assert max(backfilled) == (cutoff - timedelta(days=1)).date().isoformat()
    assert min(incremental) == cutoff.date().isoformat()
    assert set(backfilled).isdisjoint(incremental)
