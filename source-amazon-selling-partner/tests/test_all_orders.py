import gzip
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from estuary_cdk.flow import CollectionSpec, ValidationError
from estuary_cdk.http import HTTPError

import source_amazon_selling_partner
from estuary_cdk.capture import request
from source_amazon_selling_partner import Connector, api, reports, resources
from source_amazon_selling_partner.api import (
    ALL_ORDERS_HISTORY_LIMIT,
    ALL_ORDERS_LAG,
    ALL_ORDERS_MAX_WINDOW,
    ALL_ORDERS_OVERLAP,
    MIN_INCREMENTAL_WINDOW,
    backfill_all_orders,
    fetch_all_orders,
)
from source_amazon_selling_partner.client import DEFAULT_LIMITS, Operation, Priority, SPAPIClient
from source_amazon_selling_partner.models import (
    AllOrdersRow,
    EndpointConfig,
    OrdersResourceConfig,
    ReportDocument,
    default_start_date,
)
from source_amazon_selling_partner.reports import (
    REPORT_TIMEOUT,
    ReportFailed,
    ReportMismatch,
    ReportRequest,
    ReportTimedOut,
    create_report,
    request_report,
)

from .fake_amazon import (
    ALL_ORDERS_REPORT_TYPE,
    MAX_ORDER_REPORT_RANGE,
    FakeAmazon,
    FakeDocument,
    iso,
    order_line,
    parse,
)
from .test_financial_transactions import record_operations
from .test_orders import NOW, FakeTime, make_config, FrozenDatetime, collect
from .test_resources import SteppingTime

LOG = logging.getLogger("test")
DAY = timedelta(days=1)
MARKETPLACES = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2"]
HORIZON = NOW - ALL_ORDERS_LAG


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)
    monkeypatch.setattr(reports, "now", lambda: NOW)


def move_clock(monkeypatch: pytest.MonkeyPatch, fake: FakeAmazon, to: datetime) -> None:
    """Moves now() on, as a restart some time later sees it."""
    monkeypatch.setattr(api, "now", lambda: to)
    monkeypatch.setattr(reports, "now", lambda: to)
    fake.now = to


def spans(fake: FakeAmazon) -> list[timedelta]:
    return [parse(r["dataEndTime"]) - parse(r["dataStartTime"]) for r in fake.create_requests]


def client_with(fake: FakeAmazon, time: FakeTime | None = None) -> tuple[SPAPIClient, FakeTime]:
    time = time or FakeTime()
    client = SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]
    return client, time


def rows(out: list[Any]) -> list[AllOrdersRow]:
    return [r for r in out if isinstance(r, AllOrdersRow)]


def keys(out: list[Any]) -> list[tuple[str, int]]:
    return [(r.reportId, r.rowNumber) for r in rows(out)]


def order_ids(out: list[Any]) -> list[str]:
    return [(r.model_extra or {})["amazon-order-id"] for r in rows(out)]


def cursors(out: list[Any]) -> list:
    return [r for r in out if not isinstance(r, AllOrdersRow)]


def request_for(start: datetime, end: datetime, marketplaces: list[str] = MARKETPLACES) -> ReportRequest:
    return ReportRequest(ALL_ORDERS_REPORT_TYPE, tuple(marketplaces), start, end)


def requested(report_id: str, request: ReportRequest, status: str, created: datetime) -> dict:
    """A report as getReports lists one made by createReport."""
    return {
        "reportId": report_id,
        "reportType": request.report_type,
        "processingStatus": status,
        "createdTime": created.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "dataStartTime": iso(request.data_start),
        "dataEndTime": iso(request.data_end),
        "marketplaceIds": list(request.marketplace_ids),
    }


@pytest.mark.asyncio
async def test_incremental_requests_one_report_and_emits_every_line():
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    fake.order_lines = [
        # In the overlap: read again.
        order_line("111-1", cursor - timedelta(minutes=30)),
        # The same SKU twice in one order: both lines kept.
        order_line("111-2", cursor + timedelta(hours=3)),
        order_line("111-2", cursor + timedelta(hours=3)),
        # Before the overlap.
        order_line("111-3", cursor - timedelta(hours=2)),
    ]
    client, time = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert fake.create_requests == [
        {
            "reportType": ALL_ORDERS_REPORT_TYPE,
            "marketplaceIds": MARKETPLACES,
            "dataStartTime": api.to_iso(cursor - ALL_ORDERS_OVERLAP),
            "dataEndTime": api.to_iso(HORIZON),
        }
    ]
    assert keys(out) == [("requested-1", 1), ("requested-1", 2), ("requested-1", 3)]
    assert order_ids(out) == ["111-1", "111-2", "111-2"]
    first = rows(out)[0]
    assert first.dataStartTime == cursor - ALL_ORDERS_OVERLAP
    assert first.dataEndTime == HORIZON
    assert first.marketplaceIds == MARKETPLACES
    assert first.reportDocumentId == "doc-requested-1"
    assert (first.model_extra or {})["item-price"] == "10.00"
    assert out[-1] == HORIZON and cursors(out) == [HORIZON]
    # Queued when created, in progress at the first check, done at the second.
    assert time.slept == [15, 30]


@pytest.mark.asyncio
async def test_incremental_waits_for_a_window_of_the_minimum_width():
    fake = FakeAmazon(now=NOW, orders=[])
    client, _ = client_with(fake)

    out = await collect(
        fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - MIN_INCREMENTAL_WINDOW + timedelta(seconds=1))
    )

    assert out == []
    assert fake.create_requests == [] and fake.report_requests == []


@pytest.mark.asyncio
async def test_incremental_window_spans_at_most_the_maximum_range():
    cursor = HORIZON - timedelta(days=60)
    fake = FakeAmazon(now=NOW, orders=[])
    client, _ = client_with(fake)

    first = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))
    second = await collect(fetch_all_orders(client, MARKETPLACES, LOG, first[-1]))

    start = cursor - ALL_ORDERS_OVERLAP
    assert first == [start + ALL_ORDERS_MAX_WINDOW]
    assert second == [start + ALL_ORDERS_MAX_WINDOW - ALL_ORDERS_OVERLAP + ALL_ORDERS_MAX_WINDOW]
    assert [(r["dataStartTime"], r["dataEndTime"]) for r in fake.create_requests] == [
        (api.to_iso(start), api.to_iso(start + ALL_ORDERS_MAX_WINDOW)),
        (api.to_iso(first[-1] - ALL_ORDERS_OVERLAP), api.to_iso(second[-1])),
    ]
    # A day under Amazon's limit, since what Amazon does at it is unverified.
    assert max(spans(fake)) <= timedelta(days=29)

    # The fake refuses a longer range.
    with pytest.raises(HTTPError, match="limited to 30 days"):
        await create_report(
            client,
            request_for(start, start + MAX_ORDER_REPORT_RANGE + timedelta(seconds=1)),
            Priority.INCREMENTAL,
            LOG,
        )


@pytest.mark.asyncio
async def test_polls_on_schedule_and_times_out_then_a_restart_waits_on_the_same_report(monkeypatch: pytest.MonkeyPatch):
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "IN_PROGRESS"]])
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]
    client, time = client_with(fake)

    with pytest.raises(ReportTimedOut) as err:
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert time.slept[:4] == [15, 30, 60, 60]
    assert set(time.slept[2:]) == {60}
    assert REPORT_TIMEOUT.total_seconds() <= time.t < REPORT_TIMEOUT.total_seconds() + 60
    message = str(err.value)
    assert "requested-1" in message
    assert ALL_ORDERS_REPORT_TYPE in message
    assert api.to_iso(cursor - ALL_ORDERS_OVERLAP) in message and api.to_iso(HORIZON) in message

    # The restart, with a horizon moved on by the timeout, finds the report
    # still in progress and waits on it.
    later = NOW + REPORT_TIMEOUT
    move_clock(monkeypatch, fake, later)
    fake.script("requested-1", ["DONE"])
    client, time = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert len(fake.create_requests) == 1
    assert keys(out) == [("requested-1", 1)]
    # The new cursor is the end of the report read, not the later horizon.
    assert cursors(out) == [HORIZON]
    assert time.slept == [15]
    assert fake.report_requests[-1] == {
        "reportTypes": ALL_ORDERS_REPORT_TYPE,
        "processingStatuses": "IN_QUEUE,IN_PROGRESS,DONE",
        "createdSince": api.to_iso(later - DAY),
        "pageSize": 100,
    }


@pytest.mark.asyncio
async def test_a_restart_reads_the_done_report_again_without_requesting_another(monkeypatch: pytest.MonkeyPatch):
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]

    client, _ = client_with(fake)
    before = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))
    move_clock(monkeypatch, fake, NOW + timedelta(seconds=10))
    client, time = client_with(fake)
    after = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert len(fake.create_requests) == 1
    assert keys(after) == keys(before) == [("requested-1", 1)]
    assert cursors(after) == [HORIZON]
    assert time.slept == []


@pytest.mark.asyncio
@pytest.mark.parametrize("echoed_shift", ["echoed_start_shift", "echoed_end_shift"])
@pytest.mark.parametrize("seconds", [1, -1])
async def test_a_report_amazon_lists_with_another_range_fails(echoed_shift: str, seconds: int):
    # A report this call created must end where it was asked to, even
    # though a report requested before may end short of the request.
    shift: dict[str, Any] = {echoed_shift: timedelta(seconds=seconds)}
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["CANCELLED"]] * 3, **shift)
    client, _ = client_with(fake)

    with pytest.raises(ReportMismatch, match="requested-1"):
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
async def test_never_reuses_a_cancelled_or_aborted_report_or_another_request():
    request = request_for(HORIZON - DAY, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    fake.reports += [
        requested("cancelled", request, "CANCELLED", NOW - timedelta(hours=1)),
        requested("fatal", request, "FATAL", NOW - timedelta(hours=1)),
        requested("other-range", request_for(HORIZON - DAY, HORIZON - timedelta(hours=1)), "DONE", NOW),
        requested("other-start", request_for(HORIZON - DAY + timedelta(hours=1), HORIZON), "DONE", NOW),
        requested("other-marketplaces", request_for(HORIZON - DAY, HORIZON, ["ATVPDKIKX0DER"]), "DONE", NOW),
        requested("too-old", request, "DONE", NOW - DAY - timedelta(minutes=1)),
    ]
    client, _ = client_with(fake)

    report = await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert report is not None and report.reportId == "requested-1"
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [
    ["marketplaceIds"], ["dataStartTime"], ["dataEndTime"],
    ["marketplaceIds", "dataStartTime", "dataEndTime"],
])
@pytest.mark.parametrize("reuse_candidate", [False, True])
async def test_created_report_with_missing_echo_proceeds_once_warned(monkeypatch, caplog, missing, reuse_candidate):
    request = ReportRequest("GET_MERCHANT_LISTINGS_ALL_DATA", (MARKETPLACES[0],), NOW, NOW)
    fake = FakeAmazon(now=NOW, orders=[])
    if reuse_candidate:
        earlier = requested("earlier", request, "DONE", NOW - timedelta(minutes=1))
        for name in missing:
            earlier.pop(name)
        fake.reports.append(earlier)
    get_report = reports.get_report

    async def without_echo(*args):
        report = await get_report(*args)
        return report.model_copy(update={name: None for name in missing})

    monkeypatch.setattr(reports, "get_report", without_echo)
    client, _ = client_with(fake)

    report = await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert report is not None and report.reportId == "requested-1"
    assert report.marketplaceIds == list(request.marketplace_ids)
    assert report.dataStartTime == request.data_start
    assert report.dataEndTime == request.data_end
    assert len(fake.create_requests) == 1
    assert fake.get_report_requests == ["requested-1", "requested-1"]
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.message == "created report omits request fields; trusting its reportId"
    assert warning.args == {"report_id": "requested-1", "missing_fields": missing}


@pytest.mark.asyncio
@pytest.mark.parametrize("backfill", [False, True])
@pytest.mark.parametrize("missing", [
    ["dataEndTime"], ["marketplaceIds", "dataStartTime", "dataEndTime"],
])
async def test_all_orders_created_report_missing_echo_advances_cursor(backfill, missing, caplog):
    class MissingEchoAmazon(FakeAmazon):
        def _respond(self, body):
            if "processingStatus" in body:
                body = {name: value for name, value in body.items() if name not in missing}
            return super()._respond(body)

    start = HORIZON - timedelta(days=40) if backfill else HORIZON - DAY
    fake = MissingEchoAmazon(now=NOW, orders=[])
    fake.order_lines = [order_line("111-1", start + timedelta(hours=1))]
    client, _ = client_with(fake)

    if backfill:
        out = await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, None, HORIZON))
        end = start + ALL_ORDERS_MAX_WINDOW
        expected_cursor = api.to_iso(end - ALL_ORDERS_OVERLAP)
    else:
        out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, start))
        end = HORIZON
        expected_cursor = end
        assert await collect(fetch_all_orders(client, MARKETPLACES, LOG, out[-1])) == []

    assert keys(out) == [("requested-1", 1)]
    assert order_ids(out) == ["111-1"]
    assert cursors(out) == [expected_cursor]
    assert rows(out)[0].dataEndTime == end
    assert rows(out)[0].dataStartTime == (start if backfill else start - ALL_ORDERS_OVERLAP)
    assert rows(out)[0].marketplaceIds == MARKETPLACES
    assert len(fake.create_requests) == 1
    assert len(fake.downloads) == 1
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.message == "created report omits request fields; trusting its reportId"
    assert warning.args == {"report_id": "requested-1", "missing_fields": missing}


@pytest.mark.asyncio
async def test_created_report_missing_echo_still_rejects_a_different_start(monkeypatch):
    request = request_for(HORIZON - DAY, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    get_report = reports.get_report

    async def changed_echo(*args):
        report = await get_report(*args)
        update: dict[str, Any] = {"marketplaceIds": None}
        if len(fake.get_report_requests) == 2:
            update["dataStartTime"] = request.data_start + timedelta(seconds=1)
        return report.model_copy(update=update)

    monkeypatch.setattr(reports, "get_report", changed_echo)
    client, _ = client_with(fake)

    with pytest.raises(ReportMismatch, match="requested-1"):
        await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert fake.get_report_requests == ["requested-1", "requested-1"]


@pytest.mark.asyncio
async def test_created_report_missing_echo_still_rejects_different_marketplaces(monkeypatch):
    request = request_for(HORIZON - DAY, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    get_report = reports.get_report

    async def changed_echo(*args):
        report = await get_report(*args)
        return report.model_copy(update={
            "marketplaceIds": [MARKETPLACES[0]],
            "dataStartTime": None,
            "dataEndTime": None,
        })

    monkeypatch.setattr(reports, "get_report", changed_echo)
    client, _ = client_with(fake)

    with pytest.raises(ReportMismatch, match="requested-1"):
        await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert len(fake.create_requests) == 1
    assert fake.get_report_requests == ["requested-1"]


@pytest.mark.asyncio
async def test_created_report_missing_end_still_counts_cancellations(monkeypatch):
    request = request_for(HORIZON - DAY, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["CANCELLED"], ["CANCELLED"]])
    get_report = reports.get_report

    async def without_end(*args):
        report = await get_report(*args)
        return report.model_copy(update={"dataEndTime": None})

    monkeypatch.setattr(reports, "get_report", without_end)
    client, _ = client_with(fake)

    assert await request_report(client, request, Priority.INCREMENTAL, LOG) is None
    assert len(fake.create_requests) == 2


class StaleListing(FakeAmazon):
    """Lists report "stale" as in progress, while getReport says it is cancelled."""

    def _get_report(self, report_id: str, params: dict[str, Any]):
        if report_id != "stale":
            return super()._get_report(report_id, params)
        self.get_report_requests.append(report_id)
        if len(self.get_report_requests) > 5:
            raise RuntimeError("getReport keeps being asked for the cancelled report")
        stale = next(r for r in self.reports if r["reportId"] == "stale")
        return self._respond({**stale, "processingStatus": "CANCELLED"})


@pytest.mark.asyncio
async def test_never_reuses_a_report_seen_cancelled_while_still_listed_in_progress():
    request = request_for(HORIZON - DAY, HORIZON)
    fake = StaleListing(now=NOW, orders=[])
    fake.reports.append(requested("stale", request, "IN_PROGRESS", NOW - timedelta(hours=1)))
    client, _ = client_with(fake)

    report = await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert report is not None and report.reportId == "requested-1"
    assert fake.get_report_requests.count("stale") == 1
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
async def test_incremental_never_reuses_a_report_ending_at_the_cursor():
    # The backfill's last window ends at the cursor and starts where the
    # incremental's first window does.
    cursor = HORIZON - DAY
    backfilled = request_for(cursor - ALL_ORDERS_OVERLAP, cursor)
    fake = FakeAmazon(now=NOW, orders=[])
    fake.reports.append({**requested("backfilled", backfilled, "DONE", NOW - timedelta(hours=1)), "reportDocumentId": "doc-backfilled"})
    fake.documents["doc-backfilled"] = FakeDocument(b"amazon-order-id\n111-0\n")
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]
    client, _ = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert [(r["dataStartTime"], r["dataEndTime"]) for r in fake.create_requests] == [
        (api.to_iso(cursor - ALL_ORDERS_OVERLAP), api.to_iso(HORIZON))
    ]
    assert keys(out) == [("requested-1", 1)]
    assert cursors(out) == [HORIZON]


@pytest.mark.asyncio
async def test_reuses_the_newest_matching_report_whatever_the_marketplace_order():
    request = request_for(HORIZON - DAY, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    fake.reports += [
        requested("older", request, "DONE", NOW - timedelta(hours=2)),
        requested("newer", request_for(HORIZON - DAY, HORIZON, MARKETPLACES[::-1]), "IN_QUEUE", NOW - timedelta(hours=1)),
    ]
    fake.script("newer", ["DONE"])
    client, _ = client_with(fake)

    report = await request_report(client, request, Priority.INCREMENTAL, LOG)

    assert report is not None and report.reportId == "newer"
    assert fake.create_requests == []


@pytest.mark.asyncio
async def test_a_cancelled_report_is_requested_once_more_and_its_data_read():
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "CANCELLED"]])
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]
    client, time = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert keys(out) == [("requested-2", 1)]
    assert len(fake.create_requests) == 2
    assert fake.create_requests[0] == fake.create_requests[1]
    # Before the second request, Amazon's list is checked for a twin again.
    assert len(fake.report_requests) == 2
    # The second getReports waits for its bucket.
    assert time.slept == [15, pytest.approx(30, abs=0.1), 300, 15, 30]


@pytest.mark.asyncio
async def test_a_cancellation_waits_on_a_twin_request_still_in_progress():
    cursor = HORIZON - DAY
    # The first createReport's response is lost and the request sent again;
    # Amazon cancels the second as too soon and finishes the first.
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        lifecycles=[["IN_QUEUE", "IN_PROGRESS", "DONE"], ["IN_QUEUE", "CANCELLED"]],
        lost_create_responses=1,
    )
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]
    client, time = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert keys(out) == [("requested-1", 1)]
    assert cursors(out) == [HORIZON]
    assert len(fake.create_requests) == 2
    assert time.slept == [15, pytest.approx(30, abs=0.1), 15, 30]


@pytest.mark.asyncio
async def test_two_cancellations_in_a_row_make_an_empty_window_logged_as_a_warning(caplog: pytest.LogCaptureFixture):
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "CANCELLED"], ["CANCELLED"]])
    fake.order_lines = [order_line("111-1", cursor + timedelta(hours=1))]
    client, _ = client_with(fake)

    with caplog.at_level(logging.DEBUG):
        out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert out == [HORIZON]
    assert fake.document_requests == []
    [record] = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert record.message.startswith("Amazon cancelled the report twice")
    fields = record.args
    assert isinstance(fields, dict)
    assert fields["report_ids"] == ["requested-1", "requested-2"]
    assert fields["report_type"] == ALL_ORDERS_REPORT_TYPE
    assert fields["data_start_time"] == cursor - ALL_ORDERS_OVERLAP
    assert fields["data_end_time"] == HORIZON


@pytest.mark.asyncio
async def test_a_cancelled_shorter_reused_report_does_not_count_toward_two_cancellations(monkeypatch: pytest.MonkeyPatch):
    cursor = HORIZON - DAY
    # Before a restart, a report ending an hour after the cursor was in
    # flight; the restart's horizon has moved on.
    earlier = request_for(cursor - ALL_ORDERS_OVERLAP, cursor + timedelta(hours=1))
    fake = FakeAmazon(
        now=NOW, orders=[], lifecycles=[["IN_QUEUE", "CANCELLED"], ["IN_QUEUE", "CANCELLED"]]
    )
    fake.reports.append(requested("earlier", earlier, "IN_PROGRESS", NOW - timedelta(hours=1)))
    fake.script("earlier", ["CANCELLED"])
    client, time = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    # The whole range is requested twice before it is taken to be empty.
    assert out == [HORIZON]
    assert len(fake.create_requests) == 2
    assert fake.create_requests[0]["dataEndTime"] == api.to_iso(HORIZON)
    assert time.slept.count(300) == 2


@pytest.mark.asyncio
async def test_an_empty_order_report_is_logged_at_debug(caplog: pytest.LogCaptureFixture):
    fake = FakeAmazon(now=NOW, orders=[])
    client, _ = client_with(fake)

    with caplog.at_level(logging.DEBUG):
        out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    assert out == [HORIZON]
    [record] = [r for r in caplog.records if r.message == "report document has no data rows"]
    assert record.levelno == logging.DEBUG


@pytest.mark.asyncio
async def test_an_aborted_report_fails_quoting_its_diagnostic_document():
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "FATAL"]])
    diagnostic = b'\xff{"errorDetails": "' + b"x" * 3000 + b'"}'
    fake.fatal_document = FakeDocument(diagnostic, compressed=True, content_type="application/json")
    client, _ = client_with(fake)

    with pytest.raises(ReportFailed) as err:
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    message = str(err.value)
    assert "requested-1" in message and ALL_ORDERS_REPORT_TYPE in message
    quoted = message.split("reads:\n", 1)[1]
    assert len(quoted) == 2000
    assert quoted.startswith('�{"errorDetails": "xxx')
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
async def test_an_aborted_report_whose_diagnostic_cant_be_read_still_fails_as_aborted():
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "FATAL"]])
    fake.fatal_document = FakeDocument(b"error", content_type="text/plain;charset=no-such-charset")
    client, _ = client_with(fake)

    with pytest.raises(ReportFailed) as err:
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    message = str(err.value)
    assert "requested-1" in message and "FATAL" in message
    assert "could not be read" in message and "no-such-charset" in message


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["not gzip", "bad checksum", "malformed document"])
async def test_an_aborted_report_whose_diagnostic_is_corrupt_still_fails_as_aborted(monkeypatch: pytest.MonkeyPatch, case: str):
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "FATAL"]])
    compressed = gzip.compress(b"error")
    served = {
        "not gzip": b"not gzip",
        # The trailer's CRC-32, its eight bytes before the last four, flipped.
        "bad checksum": compressed[:-8] + bytes(b ^ 0xFF for b in compressed[-8:-4]) + compressed[-4:],
        "malformed document": compressed,
    }[case]
    fake.fatal_document = FakeDocument(b"error", compressed=True, served=served)
    if case == "malformed document":

        async def malformed(*args: Any):
            return ReportDocument.model_validate_json(b"{}")

        monkeypatch.setattr(reports, "fetch_report_document", malformed)
    client, _ = client_with(fake)

    with pytest.raises(ReportFailed) as err:
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    message = str(err.value)
    assert "requested-1" in message and "FATAL" in message and "could not be read" in message


@pytest.mark.asyncio
async def test_an_aborted_report_without_a_document_fails():
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["FATAL"]])
    client, _ = client_with(fake)

    with pytest.raises(ReportFailed, match="no diagnostic document"):
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, HORIZON - DAY))

    assert fake.document_requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "priority, reserve",
    [(Priority.INCREMENTAL, 0), (Priority.BACKFILL, 2)],
)
async def test_create_report_is_paced_by_its_own_bucket(priority: Priority, reserve: int):
    limit = DEFAULT_LIMITS[Operation.CREATE_REPORT]
    assert (limit.rate, limit.burst) == (0.0167, 15)
    fake = FakeAmazon(now=NOW, orders=[])
    client, time = client_with(fake, SteppingTime())

    for _ in range(2):
        await create_report(client, request_for(HORIZON - DAY, HORIZON), priority, LOG)

    assert len(fake.create_requests) == 2
    assert list(client.buckets) == [Operation.CREATE_REPORT]
    # The bucket starts with one token; a backfill also leaves the reserve.
    expected = ([reserve / limit.rate] if reserve else []) + [1 / limit.rate]
    assert time.slept == [pytest.approx(s, abs=0.01) for s in expected]


@pytest.mark.asyncio
@pytest.mark.parametrize("exclusive", [False, True])
async def test_seam_between_backfill_and_incremental_is_covered_either_way(monkeypatch: pytest.MonkeyPatch, exclusive: bool):
    # Build the resource's real initial state a day before NOW.
    cutoff = HORIZON - DAY
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(NOW - DAY + timedelta(microseconds=250_000)))
    fake = FakeAmazon(now=NOW, orders=[], exclusive_data_bounds=exclusive)
    fake.order_lines = [
        order_line("before", cutoff - timedelta(seconds=1)),
        order_line("at-cutoff", cutoff),
        order_line("after", cutoff + timedelta(seconds=1)),
    ]
    client, _ = client_with(fake)
    state = resources.all_orders(LOG, client, make_config(MARKETPLACES)).initial_state
    assert isinstance(state.inc, resources.ResourceState.Incremental) and state.backfill is not None
    assert state.inc.cursor == state.backfill.cutoff == cutoff

    backfilled = await collect(
        backfill_all_orders(client, MARKETPLACES, cutoff - timedelta(days=10), LOG, None, state.backfill.cutoff)
    )
    incremental = await collect(fetch_all_orders(client, MARKETPLACES, LOG, state.inc.cursor))

    assert cursors(backfilled) == []
    assert fake.create_requests[0]["dataEndTime"] == api.to_iso(cutoff)
    assert fake.create_requests[1]["dataStartTime"] == api.to_iso(cutoff - ALL_ORDERS_OVERLAP)
    assert set(order_ids(backfilled) + order_ids(incremental)) == {"before", "at-cutoff", "after"}


@pytest.mark.asyncio
async def test_backfill_walks_overlapping_windows_and_a_restart_reads_the_same_report(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    cutoff = HORIZON - DAY
    start = cutoff - timedelta(days=45, microseconds=500)
    window_start = start.replace(microsecond=0)
    boundary = window_start + ALL_ORDERS_MAX_WINDOW
    # With both bounds excluded, a line on the seam is still read.
    fake = FakeAmazon(now=NOW, orders=[], exclusive_data_bounds=True)
    fake.order_lines = [
        order_line("first", window_start + DAY),
        order_line("on-seam", boundary),
        order_line("last", cutoff - DAY),
    ]
    client, _ = client_with(fake)

    with caplog.at_level(logging.WARNING):
        first = await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, None, cutoff))

    assert not [r for r in caplog.records if r.message.startswith("Amazon serves order reports")]
    assert order_ids(first) == ["first"]
    assert cursors(first) == [api.to_iso(boundary - ALL_ORDERS_OVERLAP)]
    assert fake.create_requests[0]["dataStartTime"] == api.to_iso(window_start)
    assert fake.create_requests[0]["dataEndTime"] == api.to_iso(boundary)

    # A restart before the page cursor is saved reads the same report again.
    move_clock(monkeypatch, fake, NOW + timedelta(minutes=10))
    client, _ = client_with(fake)
    again = await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, None, cutoff))
    assert keys(again) == keys(first)
    assert len(fake.create_requests) == 1

    second = await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, cursors(first)[0], cutoff))

    assert order_ids(second) == ["on-seam", "last"]
    assert cursors(second) == []
    assert fake.create_requests[1]["dataStartTime"] == api.to_iso(boundary - ALL_ORDERS_OVERLAP)
    assert fake.create_requests[1]["dataEndTime"] == api.to_iso(cutoff)
    assert max(spans(fake)) <= timedelta(days=29)


@pytest.mark.asyncio
async def test_backfill_runs_every_request_at_backfill_priority():
    cutoff = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    fake.order_lines = [order_line("111-1", cutoff - timedelta(hours=1))]
    client, _ = client_with(fake)
    seen = record_operations(client)
    post = client.post

    async def recording_post(log: logging.Logger, operation: Operation, path: str, body: dict[str, Any], priority: Priority, **kwargs: Any):
        seen.append((operation, priority))
        return await post(log, operation, path, body, priority, **kwargs)

    client.post = recording_post  # type: ignore[method-assign]

    out = await collect(backfill_all_orders(client, MARKETPLACES, cutoff - DAY, LOG, None, cutoff))

    assert order_ids(out) == ["111-1"]
    assert {operation for operation, _ in seen} == {
        Operation.GET_REPORTS,
        Operation.CREATE_REPORT,
        Operation.GET_REPORT,
        Operation.GET_REPORT_DOCUMENT,
    }
    assert {priority for _, priority in seen} == {Priority.BACKFILL}


@pytest.mark.asyncio
async def test_backfill_starts_no_earlier_than_amazon_serves_order_reports(caplog: pytest.LogCaptureFixture):
    cutoff = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    # Less than two years old: the two years before NOW include 29 February.
    fake.order_lines = [order_line("111-1", NOW - timedelta(days=731) + timedelta(hours=1))]
    client, _ = client_with(fake)

    with caplog.at_level(logging.WARNING):
        out = await collect(
            backfill_all_orders(client, MARKETPLACES, cutoff - timedelta(days=5 * 365), LOG, None, cutoff)
        )

    # NOW - ALL_ORDERS_HISTORY_LIMIT is 2024-02-28T12:00:00Z; the start is
    # the next UTC midnight.
    history_start = datetime(2024, 2, 29, tzinfo=UTC)
    assert NOW - ALL_ORDERS_HISTORY_LIMIT < history_start < NOW - timedelta(days=731)
    assert fake.create_requests[0]["dataStartTime"] == api.to_iso(history_start)
    assert order_ids(out) == ["111-1"]
    assert cursors(out) == [api.to_iso(history_start + ALL_ORDERS_MAX_WINDOW - ALL_ORDERS_OVERLAP)]
    [record] = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert record.message.startswith("Amazon serves order reports for about two years only")


@pytest.mark.asyncio
@pytest.mark.parametrize("page", [None, "old"])
async def test_a_backfill_resumed_long_after_its_cutoff_starts_no_earlier_than_amazon_serves(monkeypatch: pytest.MonkeyPatch, page: str | None):
    cutoff = HORIZON - DAY
    start = cutoff - ALL_ORDERS_HISTORY_LIMIT
    later = NOW + timedelta(days=30)
    fake = FakeAmazon(now=NOW, orders=[])
    move_clock(monkeypatch, fake, later)
    client, _ = client_with(fake)
    saved = api.to_iso(start + timedelta(days=10)) if page else None

    await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, saved, cutoff))

    # later - ALL_ORDERS_HISTORY_LIMIT is 2024-03-29T12:00:00Z.
    assert fake.create_requests[0]["dataStartTime"] == "2024-03-30T00:00:00Z"


@pytest.mark.asyncio
async def test_a_restart_in_the_first_window_clamped_to_two_years_waits_on_the_same_report(monkeypatch: pytest.MonkeyPatch):
    cutoff = HORIZON - DAY
    start = cutoff - timedelta(days=3 * 365)
    fake = FakeAmazon(now=NOW, orders=[], lifecycles=[["IN_QUEUE", "IN_PROGRESS"]])
    fake.order_lines = [order_line("111-1", datetime(2024, 3, 1, tzinfo=UTC))]
    client, _ = client_with(fake)

    with pytest.raises(ReportTimedOut):
        await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, None, cutoff))

    # The restart, two hours later the same day, finds the report still in
    # progress and waits on it.
    move_clock(monkeypatch, fake, NOW + REPORT_TIMEOUT)
    fake.script("requested-1", ["DONE"])
    client, _ = client_with(fake)

    out = await collect(backfill_all_orders(client, MARKETPLACES, start, LOG, None, cutoff))

    assert len(fake.create_requests) == 1
    assert keys(out) == [("requested-1", 1)]


def test_the_default_start_date_is_a_utc_midnight():
    start = default_start_date()

    assert start == start.replace(hour=0, minute=0, second=0, microsecond=0)
    assert start.tzinfo == UTC


@pytest.mark.asyncio
async def test_incremental_requests_whole_seconds_on_a_clock_between_seconds(monkeypatch: pytest.MonkeyPatch):
    cursor = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    move_clock(monkeypatch, fake, NOW + timedelta(microseconds=250_000))
    client, _ = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert fake.create_requests[0]["dataEndTime"] == api.to_iso(HORIZON)
    assert cursors(out) == [HORIZON]


@pytest.mark.asyncio
async def test_backfill_clamped_to_two_years_requests_whole_seconds_on_a_clock_between_seconds(monkeypatch: pytest.MonkeyPatch):
    cutoff = HORIZON - DAY
    fake = FakeAmazon(now=NOW, orders=[])
    move_clock(monkeypatch, fake, NOW + timedelta(microseconds=250_000))
    client, _ = client_with(fake)

    out = await collect(
        backfill_all_orders(client, MARKETPLACES, cutoff - timedelta(days=5 * 365), LOG, None, cutoff)
    )

    assert fake.create_requests[0]["dataStartTime"] == "2024-02-29T00:00:00Z"
    assert cursors(out) == [api.to_iso(datetime(2024, 2, 29, tzinfo=UTC) + ALL_ORDERS_MAX_WINDOW - ALL_ORDERS_OVERLAP)]


@pytest.mark.asyncio
async def test_an_update_amazon_records_late_is_in_the_first_window_or_the_next(monkeypatch: pytest.MonkeyPatch):
    cursor = HORIZON - DAY
    # Amazon's order report page gives a delay of up to 30 minutes.
    fake = FakeAmazon(now=NOW, orders=[], record_delay=timedelta(minutes=30))
    fake.order_lines = [
        order_line("at-horizon", HORIZON - timedelta(seconds=1)),
        order_line("after-horizon", HORIZON + timedelta(seconds=1)),
    ]
    client, _ = client_with(fake)

    first = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    assert order_ids(first) == ["at-horizon"]

    move_clock(monkeypatch, fake, NOW + DAY)
    client, _ = client_with(fake)
    second = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursors(first)[0]))

    assert "after-horizon" in order_ids(second)


@pytest.mark.asyncio
async def test_backfill_ends_at_once_when_start_date_is_after_the_cutoff():
    fake = FakeAmazon(now=NOW, orders=[])
    client, _ = client_with(fake)

    out = await collect(backfill_all_orders(client, MARKETPLACES, HORIZON, LOG, None, HORIZON - DAY))

    assert out == [] and fake.create_requests == []


@pytest.mark.asyncio
async def test_a_column_named_like_a_row_field_fails():
    cursor = HORIZON - DAY
    request = request_for(cursor - ALL_ORDERS_OVERLAP, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    fake.reports.append({**requested("r1", request, "DONE", NOW), "reportDocumentId": "doc-r1"})
    fake.documents["doc-r1"] = FakeDocument(b"amazon-order-id\trowNumber\n111-1\t7\n")
    client, _ = client_with(fake)

    with pytest.raises(RuntimeError, match=r"columns named like the connector's own fields: \['rowNumber'\]"):
        await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))


@pytest.mark.asyncio
async def test_every_column_is_kept_including_the_buyer_taxpayer_number():
    cursor = HORIZON - DAY
    request = request_for(cursor - ALL_ORDERS_OVERLAP, HORIZON)
    fake = FakeAmazon(now=NOW, orders=[])
    fake.reports.append({**requested("r1", request, "DONE", NOW), "reportDocumentId": "doc-r1"})
    fake.documents["doc-r1"] = FakeDocument(
        b"amazon-order-id\tship-city\tship-state\tship-postal-code\tship-country\tcpf\n"
        b"111-1\tCity\tST\t00000\tXX\t000.000.000-00\n"
    )
    client, _ = client_with(fake)

    out = await collect(fetch_all_orders(client, MARKETPLACES, LOG, cursor))

    [row] = rows(out)
    assert row.model_extra == {
        "amazon-order-id": "111-1",
        "ship-city": "City",
        "ship-state": "ST",
        "ship-postal-code": "00000",
        "ship-country": "XX",
        "cpf": "000.000.000-00",
    }


async def validate(monkeypatch: pytest.MonkeyPatch, marketplaces: list[str], *bindings: OrdersResourceConfig):
    async def no_check(*_):
        pass

    monkeypatch.setattr(source_amazon_selling_partner, "validate_credentials", no_check)
    return await Connector().validate(
        LOG,
        request.Validate[EndpointConfig, OrdersResourceConfig](
            name="test/capture",
            connectorType="IMAGE",
            config=make_config(marketplaces),
            bindings=[
                request.ValidateBinding(
                    collection=CollectionSpec(name=f"test/{b.name}", key=["/reportId"], writeSchema={}),
                    resourceConfig=b,
                )
                for b in bindings
            ],
        ),
    )


@pytest.mark.asyncio
async def test_all_orders_refuses_more_marketplaces_than_create_report_takes(monkeypatch: pytest.MonkeyPatch):
    marketplaces = [f"MARKETPLACE{i}" for i in range(26)]

    with pytest.raises(ValidationError, match="at most 25 marketplace IDs; 26 are configured"):
        await validate(monkeypatch, marketplaces, OrdersResourceConfig(name="all_orders", interval=DAY))

    # Without all_orders, the same marketplaces are accepted.
    await validate(monkeypatch, marketplaces, OrdersResourceConfig(name="orders"))


@pytest.mark.asyncio
async def test_all_orders_refuses_a_backfill_schedule(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(ValidationError, match="cannot be re-backfilled on a schedule"):
        await validate(monkeypatch, MARKETPLACES, OrdersResourceConfig(name="all_orders", interval=DAY, schedule="0 0 * * 0"))


@pytest.mark.asyncio
async def test_all_orders_refuses_an_interval_under_an_hour(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(ValidationError, match="all_orders needs an interval of at least 1:00:00"):
        await validate(monkeypatch, MARKETPLACES, OrdersResourceConfig(name="all_orders", interval=timedelta(minutes=5)))

    await validate(monkeypatch, MARKETPLACES, OrdersResourceConfig(name="all_orders", interval=DAY))
