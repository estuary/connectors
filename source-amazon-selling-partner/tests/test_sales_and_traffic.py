import asyncio
import json
import logging
import tracemalloc
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from estuary_cdk.http import HTTPError

from source_amazon_selling_partner import api, reports
from source_amazon_selling_partner.api import (
    backfill_sales_and_traffic_by_date,
    fetch_sales_and_traffic_by_date,
)
from source_amazon_selling_partner.models import SalesAndTrafficByDate, sortable_utc
from source_amazon_selling_partner.reports import (
    ReportEncodingError,
    ReportMismatch,
    ReportRequest,
    ReportRoleMissing,
    create_report,
    request_report,
)
from source_amazon_selling_partner.client import Operation, Priority

from .fake_amazon import (
    SALES_AND_TRAFFIC_REPORT_TYPE,
    FakeAmazon,
    FakeDocument,
    iso,
    sales_and_traffic_day,
    sales_and_traffic_json,
)
from .test_all_orders import client_with as _client_with
from .test_all_orders import move_clock, requested
from .test_orders import NOW, FakeTime, collect
from .test_resources import SteppingTime

LOG = logging.getLogger("test")
DAY = timedelta(days=1)
TODAY = datetime(2026, 3, 1, tzinfo=UTC)
MARKETPLACES = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2"]
OPTIONS = {"dateGranularity": "DAY", "asinGranularity": "CHILD"}
FAR_BACK = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)
    monkeypatch.setattr(reports, "now", lambda: NOW)


def client_with(fake: FakeAmazon):
    # Many paced waits run here, so every sleep moves the clock.
    return _client_with(fake, SteppingTime())


def days(first: datetime, last: datetime) -> list[str]:
    """Every UTC day from `first` through `last`, as YYYY-MM-DD."""
    return [(first + i * DAY).date().isoformat() for i in range((last - first) // DAY + 1)]


def fake_with_days(first: datetime, last: datetime, marketplaces: list[str] = MARKETPLACES) -> FakeAmazon:
    fake = FakeAmazon(now=NOW, orders=[])
    for i, marketplace in enumerate(marketplaces):
        fake.sales_and_traffic[marketplace] = [
            sales_and_traffic_day(d, sales=100.0 + i) for d in days(first, last)
        ]
    return fake


def rows(out: list[Any]) -> list[SalesAndTrafficByDate]:
    return [r for r in out if isinstance(r, SalesAndTrafficByDate)]


def keys(out: list[Any]) -> list[tuple[str, str]]:
    return [(r.marketplaceId, r.date) for r in rows(out)]


def cursors(out: list[Any]) -> list:
    return [r for r in out if not isinstance(r, SalesAndTrafficByDate)]


def ranges(fake: FakeAmazon) -> list[tuple[list[str], str, str]]:
    return [(r["marketplaceIds"], r["dataStartTime"], r["dataEndTime"]) for r in fake.create_requests]


@pytest.mark.asyncio
async def test_incremental_reads_the_last_30_complete_days_per_marketplace():
    cursor = TODAY - DAY
    fake = fake_with_days(TODAY - 60 * DAY, TODAY)
    client, _ = client_with(fake)

    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, cursor))

    first, last = iso(TODAY - 30 * DAY), iso(TODAY - DAY)
    assert ranges(fake) == [([m], first, last) for m in MARKETPLACES]
    assert all(r["reportOptions"] == OPTIONS for r in fake.create_requests)
    assert keys(out) == [(m, d) for m in MARKETPLACES for d in days(TODAY - 30 * DAY, TODAY - DAY)]
    # The new cursor is today, which is not over.
    assert cursors(out) == [TODAY]

    row = rows(out)[0]
    assert row.reportId == "requested-1"
    assert (row.model_extra or {})["salesByDate"]["orderedProductSales"]["amount"] == 100.0
    # Serialized as the CDK does, fractional metrics stay JSON numbers.
    emitted = json.loads(row.model_dump_json(by_alias=True, exclude_unset=True))
    assert emitted["salesByDate"]["orderedProductSales"]["amount"] == 100.0
    assert emitted["trafficByDate"]["unitSessionPercentage"] == 12.5
    created = next(r["createdTime"] for r in fake.reports if r["reportId"] == "requested-1")
    assert emitted["reportCreatedTime"] == sortable_utc(datetime.fromisoformat(created))
    assert rows(out)[-1].reportId == "requested-2"
    assert (rows(out)[-1].model_extra or {})["salesByDate"]["orderedProductSales"]["amount"] == 101.0


@pytest.mark.asyncio
async def test_incremental_waits_for_a_new_complete_day():
    fake = fake_with_days(TODAY - 60 * DAY, TODAY)
    client, _ = client_with(fake)

    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, TODAY))

    assert out == []
    assert fake.create_requests == []
    assert fake.report_requests == []


@pytest.mark.asyncio
async def test_paused_incremental_catches_up_a_window_per_call():
    cursor = TODAY - 70 * DAY
    fake = fake_with_days(TODAY - 100 * DAY, TODAY, marketplaces=MARKETPLACES[:1])
    client, _ = client_with(fake)

    seen: list[tuple[str, str]] = []
    ends: list[datetime] = []
    while True:
        out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, cursor))
        if not out:
            break
        seen.extend(keys(out))
        (cursor,) = cursors(out)
        ends.append(cursor)

    assert ends == [TODAY - 40 * DAY, TODAY - 10 * DAY, TODAY]
    assert [(s, e) for _, s, e in ranges(fake)] == [
        (iso(TODAY - 70 * DAY), iso(TODAY - 41 * DAY)),
        (iso(TODAY - 40 * DAY), iso(TODAY - 11 * DAY)),
        # The last call reads the 30 days before today, 20 of them again.
        (iso(TODAY - 30 * DAY), iso(TODAY - DAY)),
    ]
    assert sorted(set(seen)) == [(MARKETPLACES[0], d) for d in days(TODAY - 70 * DAY, TODAY - DAY)]


@pytest.mark.asyncio
async def test_incremental_clamps_its_start_to_two_years_and_start_date():
    fake = fake_with_days(TODAY - 800 * DAY, TODAY, marketplaces=MARKETPLACES[:1])
    client, _ = client_with(fake)

    # A cursor more than two years old: the window's start is clamped to a
    # day at least a day inside Amazon's two years, which it doesn't cancel.
    cursor = TODAY - 740 * DAY
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, cursor))
    assert ranges(fake)[-1][1:] == (iso(TODAY - 728 * DAY), iso(TODAY - 711 * DAY))
    assert cursors(out) == [TODAY - 710 * DAY]
    assert len(rows(out)) == 18

    # start_date's UTC day is read whole; days before it are not.
    start_date = TODAY - 10 * DAY + timedelta(hours=15)
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], start_date, LOG, TODAY - DAY))
    assert ranges(fake)[-1][1:] == (iso(TODAY - 10 * DAY), iso(TODAY - DAY))
    assert [r.date for r in rows(out)] == days(TODAY - 10 * DAY, TODAY - DAY)

    # A window wholly before the two years, or ending where they start,
    # requests nothing and still moves the cursor on.
    requested_before = len(fake.create_requests)
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - 800 * DAY))
    assert out == [TODAY - 770 * DAY]
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - 758 * DAY))
    assert out == [TODAY - 728 * DAY]
    assert len(fake.create_requests) == requested_before


@pytest.mark.asyncio
async def test_a_later_start_date_never_skips_days_from_the_cursor_on():
    fake = fake_with_days(TODAY - 60 * DAY, TODAY, marketplaces=MARKETPLACES[:1])
    client, _ = client_with(fake)

    # A blank start_date is 30 days before each restart, so after a pause
    # it can be later than the cursor.
    cursor = TODAY - 45 * DAY
    seen: list[str] = []
    while out := await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], TODAY - 30 * DAY, LOG, cursor)):
        seen.extend(r.date for r in rows(out))
        (cursor,) = cursors(out)
    assert sorted(set(seen)) == days(TODAY - 45 * DAY, TODAY - DAY)

    # As with orders, days after the cutoff and before a later start_date
    # are captured.
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], TODAY + 7 * DAY, LOG, TODAY - DAY))
    assert [r.date for r in rows(out)] == days(TODAY - DAY, TODAY - DAY)


@pytest.mark.asyncio
async def test_a_range_computed_before_midnight_is_not_cancelled_after_it(monkeypatch: pytest.MonkeyPatch):
    fake = fake_with_days(TODAY - 800 * DAY, TODAY)
    move_clock(monkeypatch, fake, TODAY - timedelta(minutes=1))
    client, _ = client_with(fake)
    request_stream = fake.request_stream

    async def crossing_midnight(log: logging.Logger, url: str, method: str = "GET", params: dict[str, Any] | None = None, **kwargs: Any):
        # The first marketplace's request is sent before midnight, the
        # second's after it.
        result = await request_stream(log, url, method, params, **kwargs)
        if method == "POST":
            fake.now = TODAY + timedelta(minutes=1)
        return result

    fake.request_stream = crossing_midnight  # type: ignore[method-assign]

    out = await collect(backfill_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, None, TODAY - DAY))

    assert len(fake.create_requests) == 2
    assert {m for m, _ in keys(out)} == set(MARKETPLACES)


@pytest.mark.asyncio
async def test_incremental_restart_the_same_day_reuses_its_reports():
    cursor = TODAY - DAY
    fake = fake_with_days(TODAY - 40 * DAY, TODAY)
    client, _ = client_with(fake)

    first = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, cursor))
    again = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, cursor))

    assert len(fake.create_requests) == 2
    assert keys(again) == keys(first)
    assert [r.reportId for r in rows(again)] == [r.reportId for r in rows(first)]


@pytest.mark.asyncio
async def test_backfill_walks_30_day_windows_up_to_the_cutoff_and_meets_incremental(monkeypatch: pytest.MonkeyPatch):
    start_date = datetime(2025, 12, 1, 8, tzinfo=UTC)
    cutoff = TODAY
    fake = fake_with_days(start_date - 10 * DAY, TODAY + DAY)
    client, _ = client_with(fake)

    page = None
    pages: list = []
    backfilled: list[tuple[str, str]] = []
    while True:
        out = await collect(backfill_sales_and_traffic_by_date(client, MARKETPLACES, start_date, LOG, page, cutoff))
        backfilled.extend(keys(out))
        if not cursors(out):
            break
        (page,) = cursors(out)
        pages.append(page)

    assert pages == ["2025-12-31T00:00:00Z", "2026-01-30T00:00:00Z"]
    assert [(m, s, e) for (m,), s, e in ranges(fake)] == [
        (m, iso(s), iso(e))
        for s, e in [
            (datetime(2025, 12, 1, tzinfo=UTC), datetime(2025, 12, 30, tzinfo=UTC)),
            (datetime(2025, 12, 31, tzinfo=UTC), datetime(2026, 1, 29, tzinfo=UTC)),
            (datetime(2026, 1, 30, tzinfo=UTC), datetime(2026, 2, 28, tzinfo=UTC)),
        ]
        for m in MARKETPLACES
    ]
    # Every day from start_date's through the day before the cutoff, once.
    expected = [(m, d) for m in MARKETPLACES for d in days(datetime(2025, 12, 1, tzinfo=UTC), cutoff - DAY)]
    assert sorted(backfilled) == sorted(expected)

    # Incremental starts at the cutoff: the day after it ends reads the
    # cutoff's day, the first the backfill didn't.
    move_clock(monkeypatch, fake, NOW + DAY)
    out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, start_date, LOG, cutoff))
    assert cursors(out) == [cutoff + DAY]
    assert (MARKETPLACES[0], cutoff.date().isoformat()) in keys(out)
    assert sorted(set(backfilled) | set(keys(out))) == sorted(
        (m, d) for m in MARKETPLACES for d in days(datetime(2025, 12, 1, tzinfo=UTC), cutoff)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("page", [None, api.to_iso(TODAY - 740 * DAY)])
async def test_backfill_starts_inside_two_years(caplog: pytest.LogCaptureFixture, page: str | None):
    fake = fake_with_days(TODAY - 800 * DAY, TODAY, marketplaces=MARKETPLACES[:1])
    client, _ = client_with(fake)

    out = await collect(backfill_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, page, TODAY))

    assert ranges(fake) == [([MARKETPLACES[0]], iso(TODAY - 728 * DAY), iso(TODAY - 699 * DAY))]
    assert len(rows(out)) == 30
    assert cursors(out) == ["2024-04-02T00:00:00Z"]
    assert "about two years only" in caplog.text


@pytest.mark.asyncio
async def test_backfill_with_start_date_on_the_cutoffs_day_requests_nothing():
    fake = fake_with_days(TODAY - 60 * DAY, TODAY)
    client, _ = client_with(fake)

    out = await collect(backfill_sales_and_traffic_by_date(client, MARKETPLACES, TODAY + timedelta(hours=8), LOG, None, TODAY))

    assert out == []
    assert fake.create_requests == []


@pytest.mark.asyncio
async def test_a_range_cancelled_twice_is_skipped_and_the_cursor_moves_on(caplog: pytest.LogCaptureFixture):
    fetches = [
        lambda client: fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, TODAY - DAY),
        lambda client: backfill_sales_and_traffic_by_date(client, MARKETPLACES, TODAY - 60 * DAY, LOG, None, TODAY),
    ]
    for fetch, cursor in zip(fetches, [TODAY, "2026-01-30T00:00:00Z"]):
        fake = fake_with_days(TODAY - 60 * DAY, TODAY)
        fake.lifecycles = [["CANCELLED"], ["CANCELLED"]]
        client, _ = client_with(fake)
        caplog.clear()

        out = await collect(fetch(client))

        assert {m for m, _ in keys(out)} == {MARKETPLACES[1]}
        assert cursors(out) == [cursor]
        assert "cancelled the report twice" in caplog.text


@pytest.mark.asyncio
async def test_create_report_paces_sales_and_traffic_to_three_per_five_minutes():
    marketplaces = [f"MARKETPLACE{i}" for i in range(5)]
    fake = fake_with_days(TODAY - 100 * DAY, TODAY, marketplaces=marketplaces)
    client, time = client_with(fake)
    sent: list[float] = []
    request_stream = fake.request_stream

    async def recording(log: logging.Logger, url: str, method: str = "GET", params: dict[str, Any] | None = None, **kwargs: Any):
        if method == "POST":
            sent.append(time.clock())
        return await request_stream(log, url, method, params, **kwargs)

    fake.request_stream = recording  # type: ignore[method-assign]

    page = None
    while True:
        out = await collect(
            backfill_sales_and_traffic_by_date(client, marketplaces, TODAY - 60 * DAY, LOG, page, TODAY)
        )
        if not cursors(out):
            break
        (page,) = cursors(out)
    # An idle spell doesn't let five incremental requests through at once.
    time.t += 3600
    # A start_date that makes its requests differ from the backfill's last.
    await collect(fetch_sales_and_traffic_by_date(client, marketplaces, TODAY - 20 * DAY, LOG, TODAY - DAY))

    assert len(sent) == 15
    for t in sent:
        assert len([s for s in sent if t <= s < t + 300]) <= 3


class ConcurrentTime(FakeTime):
    """Fake time for concurrent tasks: once every task sleeps, the clock moves to the earliest wake-up."""

    def __init__(self) -> None:
        super().__init__()
        self.sleepers: list[tuple[float, asyncio.Future]] = []

    async def sleep(self, seconds: float) -> None:
        wake = asyncio.get_running_loop().create_future()
        self.sleepers.append((self.t + seconds + 1e-3, wake))
        await wake

    async def run(self, *coros: Any):
        tasks = [asyncio.ensure_future(c) for c in coros]
        while not all(t.done() for t in tasks):
            for _ in range(10_000):
                await asyncio.sleep(0)
                if len(self.sleepers) == len([t for t in tasks if not t.done()]):
                    break
            else:
                raise AssertionError("a task waits on something other than the clock")
            if self.sleepers:
                self.sleepers.sort(key=lambda s: s[0])
                at, wake = self.sleepers.pop(0)
                self.t = max(self.t, at)
                wake.set_result(None)
        return [t.result() for t in tasks]


@pytest.mark.asyncio
async def test_concurrent_backfill_and_incremental_keep_to_three_per_five_minutes():
    marketplaces = [f"MARKETPLACE{i}" for i in range(5)]
    fake = fake_with_days(TODAY - 100 * DAY, TODAY, marketplaces=marketplaces)
    time = ConcurrentTime()
    client, _ = _client_with(fake, time)
    # The backfill lists reports at once, and its createReport then waits
    # for that operation's reserve while the incremental requests go.
    client.bucket(Operation.GET_REPORTS).tokens = 10
    sent: list[float] = []
    request_stream = fake.request_stream

    async def recording(log: logging.Logger, url: str, method: str = "GET", params: dict[str, Any] | None = None, **kwargs: Any):
        if method == "POST":
            sent.append(time.clock())
        return await request_stream(log, url, method, params, **kwargs)

    fake.request_stream = recording  # type: ignore[method-assign]

    async def incremental():
        # Starts just after the backfill asks first.
        await time.sleep(1)
        return await collect(fetch_sales_and_traffic_by_date(client, marketplaces[1:], FAR_BACK, LOG, TODAY - DAY))

    await time.run(
        collect(backfill_sales_and_traffic_by_date(client, marketplaces[:1], TODAY - 60 * DAY, LOG, None, TODAY)),
        incremental(),
    )

    assert len(sent) == 5
    for t in sent:
        assert len([s for s in sent if t <= s < t + 300]) <= 3


@pytest.mark.asyncio
async def test_other_report_types_are_not_paced_and_send_no_options():
    fake = FakeAmazon(now=NOW, orders=[])
    client, time = client_with(fake)
    request = ReportRequest(
        "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL", (MARKETPLACES[0],), TODAY - 2 * DAY, TODAY - DAY
    )

    for _ in range(4):
        await create_report(LOG, client, request, Priority.INCREMENTAL)

    assert "reportOptions" not in request.body()
    assert all("reportOptions" not in r for r in fake.create_requests)
    assert client.report_type_buckets == {}
    assert time.t < 300


@pytest.mark.asyncio
async def test_a_403_for_a_report_type_without_a_role_stays_an_http_error():
    fake = FakeAmazon(now=NOW, orders=[])
    fake.forbidden_report_types = {"GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"}
    client, _ = client_with(fake)
    request = ReportRequest(
        "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL", (MARKETPLACES[0],), TODAY - 2 * DAY, TODAY - DAY
    )

    with pytest.raises(HTTPError) as err:
        await create_report(LOG, client, request, Priority.INCREMENTAL)
    assert err.value.code == 403


def st_request(start: datetime, end: datetime, marketplaces: list[str] = MARKETPLACES[:1], options: dict[str, str] = OPTIONS) -> ReportRequest:
    return ReportRequest(SALES_AND_TRAFFIC_REPORT_TYPE, tuple(marketplaces), start, end, report_options=options)


@pytest.mark.asyncio
async def test_a_sales_and_traffic_create_also_spends_a_create_report_token():
    fake = FakeAmazon(now=NOW, orders=[])
    client, time = client_with(fake)
    client.bucket(Operation.CREATE_REPORT).tokens = 0

    await create_report(LOG, client, st_request(TODAY - 5 * DAY, TODAY - DAY), Priority.INCREMENTAL)

    # Its own type's bucket starts with a token, so the wait is createReport's
    # refill, about 60 seconds.
    assert 59 < time.t < 61


@pytest.mark.asyncio
async def test_fake_create_report_checks_sales_and_traffic_arguments():
    fake = FakeAmazon(now=NOW, orders=[])
    client, _ = client_with(fake)

    with pytest.raises(HTTPError, match="invalid report option"):
        await create_report(
            LOG, client, st_request(TODAY - 5 * DAY, TODAY - DAY, options={"dateGranularity": "HOUR"}), Priority.INCREMENTAL
        )
    with pytest.raises(HTTPError, match="exactly one marketplace"):
        await create_report(LOG, client, st_request(TODAY - 5 * DAY, TODAY - DAY, MARKETPLACES), Priority.INCREMENTAL)

    # A start more than two years back is cancelled, twice, and read as no data.
    report = await request_report(LOG, client, st_request(TODAY - 731 * DAY, TODAY - 710 * DAY), Priority.INCREMENTAL)
    assert report is None
    assert len(fake.create_requests) == 4


@pytest.mark.asyncio
async def test_create_report_without_brand_analytics_says_so():
    fake = fake_with_days(TODAY - 40 * DAY, TODAY)
    fake.brand_analytics = False
    client, _ = client_with(fake)

    with pytest.raises(ReportRoleMissing, match="lacks the Brand Analytics role") as err:
        await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES, FAR_BACK, LOG, TODAY - DAY))
    assert "disable the binding" in str(err.value)


def reusable(fake: FakeAmazon, start: datetime, end: datetime, document: FakeDocument) -> None:
    """A DONE report for the stream's first request, which getReports lists for reuse."""
    listed = requested("earlier-1", st_request(start, end), "DONE", NOW - timedelta(hours=1))
    listed["reportDocumentId"] = "doc-earlier-1"
    fake.reports.append(listed)
    fake.documents["doc-earlier-1"] = document


def spec(**changes: Any) -> dict:
    return {
        "reportType": SALES_AND_TRAFFIC_REPORT_TYPE,
        "reportOptions": OPTIONS,
        "dataStartTime": (TODAY - 30 * DAY).date().isoformat(),
        "dataEndTime": (TODAY - DAY).date().isoformat(),
        "marketplaceIds": MARKETPLACES[:1],
        **changes,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"reportOptions": {"dateGranularity": "DAY", "asinGranularity": "SKU"}},
        {"reportOptions": {"dateGranularity": "WEEK", "asinGranularity": "CHILD"}},
        {"marketplaceIds": ["A2EUQ1WTGCTBG2"]},
        {"marketplaceIds": [MARKETPLACES[0], "A2EUQ1WTGCTBG2"]},
        {"dataStartTime": "2026-01-29"},
        {"dataEndTime": "2026-03-01"},
    ],
)
async def test_a_report_specified_otherwise_than_requested_fails(changes: dict[str, Any]):
    fake = fake_with_days(TODAY - 40 * DAY, TODAY)
    document = sales_and_traffic_json(spec(**changes), [sales_and_traffic_day("2026-02-01")])
    reusable(fake, TODAY - 30 * DAY, TODAY - DAY, FakeDocument(document.encode(), compressed=True))
    client, _ = client_with(fake)

    with pytest.raises(ReportMismatch, match="report earlier-1 covers"):
        await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - DAY))
    assert fake.create_requests == []


@pytest.mark.asyncio
async def test_a_report_without_by_date_entries_fails():
    fake = fake_with_days(TODAY - 40 * DAY, TODAY)
    document = json.dumps({"reportSpecification": spec(), "salesAndTrafficByDay": [sales_and_traffic_day("2026-02-01")]})
    reusable(fake, TODAY - 30 * DAY, TODAY - DAY, FakeDocument(document.encode(), compressed=True))
    client, _ = client_with(fake)

    with pytest.raises(RuntimeError, match="report earlier-1 has no salesAndTrafficByDate section"):
        await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - DAY))
    assert fake.create_requests == []


@pytest.mark.asyncio
async def test_by_asin_entries_are_not_kept_in_memory():
    fake = FakeAmazon(now=NOW, orders=[])
    metrics = {f"metric{i}": i * 1.5 for i in range(34)}
    by_asin = [{"parentAsin": f"P{i:09}", "childAsin": f"C{i:09}", "trafficByAsin": metrics} for i in range(2000)]
    by_date = [sales_and_traffic_day(d) for d in days(TODAY - 30 * DAY, TODAY - DAY)]
    document = json.dumps(
        {"reportSpecification": spec(), "salesAndTrafficByDate": by_date, "salesAndTrafficByAsin": by_asin}
    ).encode()
    reusable(fake, TODAY - 30 * DAY, TODAY - DAY, FakeDocument(document, content_type="application/json"))
    fake.download_chunk_size = 4096
    client, _ = client_with(fake)

    tracemalloc.start()
    try:
        out = await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - DAY))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert len(rows(out)) == 30
    assert peak < len(document) / 4, (peak, len(document))


@pytest.mark.asyncio
async def test_a_report_with_a_day_twice_fails():
    fake = fake_with_days(TODAY - 40 * DAY, TODAY, marketplaces=MARKETPLACES[:1])
    fake.sales_and_traffic[MARKETPLACES[0]].append(sales_and_traffic_day("2026-02-10", sales=1.0))
    client, _ = client_with(fake)

    with pytest.raises(RuntimeError, match="more than one entry for 2026-02-10"):
        await collect(fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - DAY))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type, encoding, trailing, readable",
    [
        ("application/json; charset=ISO-8859-1", "latin-1", b"", True),
        ("application/json", "latin-1", b"", False),
        # é's two bytes arrive in separate chunks.
        ("application/json; charset=UTF-8", "utf-8", b"", True),
        # A document that ends inside a character.
        ("application/json", "utf-8", b"\xc3", False),
    ],
)
async def test_a_document_is_decoded_with_its_content_type_charset(content_type: str, encoding: str, trailing: bytes, readable: bool):
    fake = FakeAmazon(now=NOW, orders=[])
    fake.download_chunk_size = 1
    day = {**sales_and_traffic_day("2026-02-10"), "note": "Café"}
    document = sales_and_traffic_json(spec(), [day]).encode(encoding) + trailing
    reusable(fake, TODAY - 30 * DAY, TODAY - DAY, FakeDocument(document, content_type=content_type))
    client, _ = client_with(fake)

    fetch = fetch_sales_and_traffic_by_date(client, MARKETPLACES[:1], FAR_BACK, LOG, TODAY - DAY)
    if readable:
        out = await collect(fetch)
        assert [(r.model_extra or {})["note"] for r in rows(out)] == ["Café"]
    else:
        with pytest.raises(ReportEncodingError, match="isn't utf-8-sig text"):
            await collect(fetch)


def test_a_day_keeps_the_copy_from_the_newest_report_whatever_the_commit_order():
    # The backfill and the incremental task read the same recent days from
    # separate reports, and either can commit last. The non-delete reduction
    # keeps the copy from the report created last, which Flow compares as a
    # string; Amazon gives report times with milliseconds.
    schema = SalesAndTrafficByDate.model_json_schema(mode="serialization")
    assert schema["else"] == {"reduce": {"strategy": "maximize", "key": ["/reportCreatedTime"]}}

    def emitted(created: str) -> str:
        row = SalesAndTrafficByDate.model_validate(
            {"marketplaceId": "m", "date": "2026-03-01", "reportId": "r", "reportCreatedTime": created}
        )
        return json.loads(row.model_dump_json())["reportCreatedTime"]

    assert emitted("2026-03-01T10:00:00Z") < emitted("2026-03-01T10:00:00.677Z")
