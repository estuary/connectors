import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from estuary_cdk.http import HTTPError
from pydantic import ValidationError as PydanticValidationError

from source_amazon_selling_partner import api, resources
from source_amazon_selling_partner.api import (
    PaginationTokenRejected,
    backfill_financial_transactions,
    fetch_financial_transactions,
)
from source_amazon_selling_partner.client import (
    DEFAULT_LIMITS,
    Operation,
    OperationLimit,
    Priority,
    SPAPIClient,
)
from source_amazon_selling_partner.models import (
    ConnectorState,
    FinancialTransaction,
    ResourceState,
)

from .fake_amazon import FakeAmazon, transaction
from .test_orders import NOW, make_config, FrozenDatetime, client_for, collect

LOG = logging.getLogger("test")
MS = timedelta(milliseconds=1)
DAY = timedelta(days=1)


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)


def ids(out: list[Any]) -> list[str]:
    return [t.transactionId for t in out if isinstance(t, FinancialTransaction)]


def cursors(out: list[Any]) -> list:
    return [t for t in out if not isinstance(t, FinancialTransaction)]


def record_operations(client: SPAPIClient) -> list[tuple[Operation, Priority]]:
    """Records the operation and priority of every request the client makes."""
    seen: list[tuple[Operation, Priority]] = []
    get = client.get

    async def recording_get(log: logging.Logger, operation: Operation, path: str, params: dict[str, Any], priority: Priority):
        seen.append((operation, priority))
        return await get(log, operation, path, params, priority)

    client.get = recording_get  # type: ignore[method-assign]
    return seen


def reject_transaction_pages(fake: FakeAmazon, monkeypatch: pytest.MonkeyPatch, *, once: bool = False):
    list_transactions = fake._list_transactions
    rejected = False

    def rejecting(params: dict[str, Any]):
        nonlocal rejected
        if "nextToken" in params and (not once or not rejected):
            rejected = True
            raise HTTPError("400 Bad Request: InvalidInput", 400)
        return list_transactions(params)

    monkeypatch.setattr(fake, "_list_transactions", rejecting)


@pytest.mark.parametrize("backfill", [True, False])
@pytest.mark.asyncio
async def test_rejected_token_retries_halved_transactions_window(backfill: bool, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    end = start + timedelta(days=2)
    smaller_end = start + DAY
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(str(i), start + timedelta(minutes=i)) for i in range(1, 4)],
    )
    reject_transaction_pages(fake, monkeypatch, once=True)
    client = client_for(fake)
    cutoff = NOW - timedelta(hours=1)

    with caplog.at_level(logging.WARNING, logger=LOG.name):
        if backfill:
            out = await collect(backfill_financial_transactions(client, start, 2 * DAY, LOG, None, cutoff))
        else:
            out = await collect(fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, 2 * DAY, LOG, start))

    assert ids(out) == ["1", "2", "1", "2", "3"]
    assert cursors(out) == [api.to_iso(smaller_end) if backfill else smaller_end]
    assert len(fake.requests) == 4
    assert fake.requests[0]["postedBefore"] == api.to_iso(end)
    assert "nextToken" in fake.requests[1]
    assert "nextToken" not in fake.requests[2]
    assert fake.requests[2]["postedAfter"] == fake.requests[0]["postedAfter"]
    assert fake.requests[2]["postedBefore"] == api.to_iso(smaller_end)
    assert "nextToken" in fake.requests[3]
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert caplog.records[0].msg == "shrinking a financial transactions window after Amazon rejected its page token"
    assert caplog.records[0].args == {"window_start": start, "window_end": end, "new_window_end": smaller_end}

    if backfill:
        await collect(backfill_financial_transactions(client, start, 2 * DAY, LOG, out[-1], cutoff))
    else:
        await collect(fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, 2 * DAY, LOG, out[-1]))
    assert fake.requests[4]["postedAfter"] == api.to_iso(smaller_end)


@pytest.mark.parametrize("backfill", [True, False])
@pytest.mark.parametrize("span", [timedelta(hours=8, seconds=7), timedelta(hours=1), timedelta(minutes=30)])
@pytest.mark.asyncio
async def test_rejected_tokens_shrink_transactions_window_until_floor(backfill: bool, span: timedelta, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(str(i), start + timedelta(minutes=i)) for i in range(1, 4)],
    )
    reject_transaction_pages(fake, monkeypatch)
    expected_ends = [start + span]
    if span > timedelta(hours=1):
        expected_ends += [
            start + timedelta(hours=4, seconds=3),
            start + timedelta(hours=2, seconds=1),
            start + timedelta(hours=1),
        ]

    with caplog.at_level(logging.WARNING, logger=LOG.name):
        with pytest.raises(PaginationTokenRejected, match="already tried smaller windows; reduce the Window Size") as exc:
            if backfill:
                await collect(backfill_financial_transactions(client_for(fake), start, span, LOG, None, NOW - timedelta(hours=1)))
            else:
                await collect(fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, span, LOG, start))

    assert isinstance(exc.value.__cause__, HTTPError)
    assert [r["postedBefore"] for r in fake.requests if "nextToken" not in r] == [api.to_iso(end) for end in expected_ends]
    assert len(fake.requests) == 2 * len(expected_ends)
    assert len(caplog.records) == len(expected_ends) - 1


@pytest.mark.parametrize("backfill", [True, False])
@pytest.mark.parametrize("page,code,message", [
    (1, 400, "400 Bad Request: InvalidInput"),
    (2, 400, f"400 Bad Request: {api.LWA_TOKEN_URL} invalid_grant"),
    (2, 403, "403 Access denied"),
])
@pytest.mark.asyncio
async def test_other_transactions_errors_are_unchanged(backfill: bool, page: int, code: int, message: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(str(i), start + timedelta(minutes=i)) for i in range(1, 4)],
    )
    error = HTTPError(message, code)
    list_transactions = fake._list_transactions

    def failing(params: dict[str, Any]):
        if len(fake.requests) == page:
            raise error
        return list_transactions(params)

    monkeypatch.setattr(fake, "_list_transactions", failing)
    with pytest.raises(HTTPError) as exc:
        if backfill:
            await collect(backfill_financial_transactions(client_for(fake), start, DAY, LOG, None, NOW - timedelta(hours=1)))
        else:
            await collect(fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, start))
    assert exc.value is error
    assert len(fake.requests) == page
    assert not caplog.records


@pytest.mark.asyncio
async def test_backfill_waits_only_before_first_transactions_attempt(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(seconds=30)
    start = cutoff - timedelta(hours=4)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(str(i), start + timedelta(minutes=i)) for i in range(1, 4)],
    )
    reject_transaction_pages(fake, monkeypatch, once=True)
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        assert fake.requests == []
        slept.append(seconds)
        fake.now = NOW + timedelta(seconds=seconds)
        monkeypatch.setattr(api, "now", lambda: fake.now)

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    out = await collect(backfill_financial_transactions(client_for(fake), start, DAY, LOG, None, cutoff))
    assert slept == [(cutoff + api.TRANSACTIONS_REALTIME_LAG - NOW).total_seconds()]
    assert out[-1] == api.to_iso(start + timedelta(hours=2))


@pytest.mark.asyncio
async def test_realtime_pages_through_empty_pages_with_half_open_window():
    cursor = NOW - timedelta(hours=1)
    horizon = NOW - api.TRANSACTIONS_REALTIME_LAG
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[
            transaction("at-cursor", cursor),
            transaction("other-marketplace", cursor + timedelta(minutes=1), marketplace="A2EUQ1WTGCTBG2"),
            transaction("mid", cursor + timedelta(minutes=10)),
            transaction("just-before-horizon", horizon - MS),
            transaction("at-horizon", horizon),
            transaction("before-cursor", cursor - MS),
        ],
        empty_transaction_pages=True,
    )
    client = client_for(fake)
    operations = record_operations(client)

    out = await collect(
        fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )

    assert sorted(ids(out)) == ["at-cursor", "just-before-horizon", "mid", "other-marketplace"]
    # The cursor comes last, after every transaction it covers.
    assert out[-1] == horizon
    assert all(isinstance(x, FinancialTransaction) for x in out[:-1])
    assert operations == [(Operation.LIST_TRANSACTIONS, Priority.INCREMENTAL)] * 4
    # listTransactions has its own bucket rather than sharing searchOrders'.
    assert set(client.buckets) == {Operation.LIST_TRANSACTIONS}
    assert cursors(out) == [horizon]
    first = fake.requests[0]
    assert first == {"postedAfter": "2026-03-01T11:00:00Z", "postedBefore": "2026-03-01T11:45:00Z"}
    # Four transactions in pages of two, each behind an empty page.
    assert len(fake.requests) == 4
    for req in fake.requests[1:]:
        assert req["nextToken"].startswith("txn-token-")
        assert {k: v for k, v in req.items() if k != "nextToken"} == first


@pytest.mark.asyncio
async def test_pages_without_a_transactions_array_keep_paging():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(f"t{i}", cursor + timedelta(minutes=i)) for i in range(3)],
        empty_transaction_pages=True,
        omit_empty_transactions=True,
    )
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert sorted(ids(out)) == ["t0", "t1", "t2"]
    assert cursors(out) == [NOW - api.TRANSACTIONS_REALTIME_LAG]


@pytest.mark.asyncio
async def test_pages_with_a_null_transactions_array_keep_paging():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction(f"t{i}", cursor + timedelta(minutes=i)) for i in range(3)],
        empty_transaction_pages=True,
        null_empty_transactions=True,
    )
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert sorted(ids(out)) == ["t0", "t1", "t2"]
    assert len(fake.requests) == 4
    assert cursors(out) == [NOW - api.TRANSACTIONS_REALTIME_LAG]


@pytest.mark.asyncio
async def test_keeps_paging_when_every_page_is_empty():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[], empty_transaction_pages=True)
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert ids(out) == []
    assert len(fake.requests) == 2
    assert cursors(out) == [NOW - api.TRANSACTIONS_REALTIME_LAG]


@pytest.mark.asyncio
async def test_realtime_waits_when_caught_up():
    fake = FakeAmazon(now=NOW, orders=[])
    cursor = NOW - api.TRANSACTIONS_REALTIME_LAG - api.MIN_INCREMENTAL_WINDOW + timedelta(seconds=1)
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert out == []
    assert fake.requests == []


@pytest.mark.asyncio
async def test_window_is_capped_by_window_size():
    cursor = NOW - timedelta(days=40)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, timedelta(days=30), LOG, cursor)
    )
    assert out == [cursor + timedelta(days=30)]


@pytest.mark.asyncio
async def test_lookback_rereads_late_postings_and_released_deferrals(monkeypatch: pytest.MonkeyPatch):
    lookback = timedelta(days=30)
    window = timedelta(hours=1)
    start = NOW - timedelta(hours=2)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction("deferred", start + timedelta(minutes=10), status="DEFERRED")],
        page_size=10,
    )
    client = client_for(fake)

    # The realtime pass reads the window while the transaction is deferred.
    realtime = await collect(fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, window, LOG, start))
    assert ids(realtime) == ["deferred"]
    assert realtime[0].model_extra["transactionStatus"] == "DEFERRED"
    realtime_cursor = cursors(realtime)[-1]
    assert realtime_cursor == start + window

    # A lookback later, Amazon has released it and posted another
    # transaction into the window the realtime pass already read.
    later = NOW + lookback
    monkeypatch.setattr(api, "now", lambda: later)
    fake.now = later
    fake.transactions[0]["transactionStatus"] = "DEFERRED_RELEASED"
    fake.transactions.append(transaction("late", start + timedelta(minutes=40)))

    # The realtime pass has moved on and never sees either again.
    realtime = await collect(
        fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, window, LOG, realtime_cursor)
    )
    assert ids(realtime) == []

    rereads = await collect(fetch_financial_transactions(client, lookback, window, LOG, start))
    by_id = {t.transactionId: t for t in rereads if isinstance(t, FinancialTransaction)}
    assert sorted(by_id) == ["deferred", "late"]
    assert (by_id["deferred"].model_extra or {})["transactionStatus"] == "DEFERRED_RELEASED"
    assert cursors(rereads) == [start + window]


@pytest.mark.asyncio
async def test_lookback_stops_a_lookback_behind_the_present():
    lookback = timedelta(days=30)
    cursor = NOW - lookback - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(fetch_financial_transactions(client_for(fake), lookback, DAY, LOG, cursor))
    assert out == [NOW - lookback]


@pytest.mark.asyncio
async def test_backfill_walks_half_open_windows_up_to_cutoff():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    cutoff = datetime(2026, 1, 3, 12, tzinfo=UTC)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[
            transaction("d1-start", start),
            transaction("d1-end", start + DAY - MS),
            transaction("d2-start", start + DAY),
            transaction("before-cutoff", cutoff - MS),
            transaction("at-cutoff", cutoff),
        ],
        page_size=10,
    )
    client = client_for(fake)
    operations = record_operations(client)

    page = None
    emitted: list[str] = []
    pages: list[str] = []
    while True:
        out = await collect(backfill_financial_transactions(client, start, DAY, LOG, page, cutoff))
        emitted += ids(out)
        if not cursors(out):
            break
        page = cursors(out)[-1]
        pages.append(page)
        # The page cursor checkpoints the next window, so it must follow
        # every transaction of this one.
        assert out[-1] == page
        assert all(isinstance(x, FinancialTransaction) for x in out[:-1])

    assert emitted == ["d1-start", "d1-end", "d2-start", "before-cutoff"]
    assert pages == ["2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"]
    assert [(r["postedAfter"], r["postedBefore"]) for r in fake.requests] == [
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
        ("2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
        ("2026-01-03T00:00:00Z", "2026-01-03T12:00:00Z"),
    ]
    assert all("marketplaceId" not in r for r in fake.requests)
    assert operations == [(Operation.LIST_TRANSACTIONS, Priority.BACKFILL)] * 3


@pytest.mark.asyncio
async def test_backfill_keeps_a_transaction_in_a_fractional_start_seconds_second():
    start = datetime(2026, 1, 1, 0, 0, 0, 500_000, tzinfo=UTC)
    cutoff = datetime(2026, 1, 2, tzinfo=UTC)
    fake = FakeAmazon(now=NOW, orders=[], transactions=[transaction("in-start-second", start + 250 * MS)], page_size=10)
    out = await collect(backfill_financial_transactions(client_for(fake), start, DAY, LOG, None, cutoff))
    assert ids(out) == ["in-start-second"]
    assert fake.requests[0]["postedAfter"] == "2026-01-01T00:00:00Z"


@pytest.mark.asyncio
async def test_backfill_never_requests_more_than_180_days():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(backfill_financial_transactions(client_for(fake), start, timedelta(days=365), LOG, None, cutoff))
    assert out == [api.to_iso(start + api.MAX_TRANSACTIONS_WINDOW)]


@pytest.mark.asyncio
@pytest.mark.parametrize("start_offset", [timedelta(), timedelta(days=2)])
async def test_backfill_with_start_date_at_or_after_cutoff_requests_nothing(start_offset: timedelta):
    # The CDK still runs the backfill from page None when start_date is at
    # or after the cutoff; the realtime pass covers that time instead.
    cutoff = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(
        backfill_financial_transactions(client_for(fake), cutoff + start_offset, DAY, LOG, None, cutoff)
    )
    assert out == []
    assert fake.requests == []


@pytest.mark.asyncio
async def test_open_wires_a_backfill_that_ends_when_start_date_is_after_cutoff(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff))
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(cutoff + timedelta(days=2)))
    fake = FakeAmazon(now=NOW, orders=[])
    resource = resources.financial_transactions(LOG, client_for(fake), config)
    await resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]

    backfill = resource.initial_state.backfill
    assert await collect(recorded["fetch_page"](LOG, backfill.next_page, backfill.cutoff)) == []
    assert fake.requests == []


@pytest.mark.asyncio
async def test_backfill_waits_out_a_recent_cutoff(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(seconds=30)
    fake = FakeAmazon(now=NOW, orders=[])
    events: list[str] = []
    slept: list[float] = []

    # Sleeping moves both clocks, so a request sent before the wait reaches
    # the fake while postedBefore is still within two minutes of its now.
    async def fake_sleep(seconds: float) -> None:
        events.append("sleep")
        slept.append(seconds)
        later = NOW + timedelta(seconds=seconds)
        monkeypatch.setattr(api, "now", lambda: later)
        fake.now = later

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    client = client_for(fake)
    get = client.get

    async def recording_get(*args: Any, **kwargs: Any):
        events.append("request")
        return await get(*args, **kwargs)

    client.get = recording_get  # type: ignore[method-assign]

    await collect(backfill_financial_transactions(client, cutoff - timedelta(hours=1), DAY, LOG, None, cutoff))
    # Read no sooner than the realtime pass would read the same time.
    assert events == ["sleep", "request"]
    assert slept[0] == pytest.approx((cutoff + api.TRANSACTIONS_REALTIME_LAG - NOW).total_seconds())
    assert fake.requests[0]["postedBefore"] == api.to_iso(cutoff)


@pytest.mark.asyncio
async def test_seam_between_backfill_and_both_incremental_passes(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff + timedelta(microseconds=250_000)))
    config = make_config(["ATVPDKIKX0DER"], start_date="2026-01-01T00:00:00Z")
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[
            transaction("before", cutoff - MS),
            transaction("at", cutoff),
            transaction("after", cutoff + MS),
        ],
        page_size=10,
    )
    client = client_for(fake)
    resource = resources.financial_transactions(LOG, client, config)
    state = resource.initial_state
    assert resource.key == ["/transactionId"]
    assert state.backfill.cutoff == cutoff
    assert state.inc[resources.REALTIME].cursor == cutoff
    assert state.inc[resources.LATE].cursor == cutoff - api.TRANSACTIONS_LATE_LAG
    # The default lookback reaches back past start_date.
    assert state.inc[resources.LOOKBACK].cursor == datetime(2026, 1, 1, tzinfo=UTC)

    backfilled = await collect(
        backfill_financial_transactions(client, cutoff - timedelta(minutes=1), DAY, LOG, None, state.backfill.cutoff)
    )
    realtime = await collect(
        fetch_financial_transactions(client, api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, state.inc[resources.REALTIME].cursor)
    )

    assert ids(backfilled) == ["before"]
    assert sorted(ids(realtime)) == ["after", "at"]


@pytest.mark.parametrize(
    "start_offset,late_offset,lookback_offset",
    [
        # start_date between the two lags: the late pass starts its lag back,
        # the lookback pass at start_date.
        (-timedelta(days=5), -api.TRANSACTIONS_LATE_LAG, -timedelta(days=5)),
        # start_date after the cutoff: both start at the cutoff, with realtime.
        (timedelta(days=2), timedelta(), timedelta()),
    ],
)
def test_rereading_passes_never_start_before_start_date(monkeypatch: pytest.MonkeyPatch, start_offset: timedelta, late_offset: timedelta, lookback_offset: timedelta):
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff))
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(cutoff + start_offset))
    state = resources.financial_transactions(LOG, client_for(FakeAmazon(now=NOW, orders=[])), config).initial_state
    assert state.inc[resources.REALTIME].cursor == cutoff
    assert state.inc[resources.LATE].cursor == cutoff + late_offset
    assert state.inc[resources.LOOKBACK].cursor == cutoff + lookback_offset


@pytest.mark.asyncio
async def test_open_wires_each_pass_to_its_lag(monkeypatch: pytest.MonkeyPatch):
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    config = make_config(["ATVPDKIKX0DER"], advanced={"financial_transactions_lookback": "P45D"})
    fake = FakeAmazon(now=NOW, orders=[])
    resource = resources.financial_transactions(LOG, client_for(fake), config)
    await resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]

    fetch_changes = recorded["fetch_changes"]
    assert set(fetch_changes) == set(resource.initial_state.inc)
    for name, lag in [
        (resources.REALTIME, api.TRANSACTIONS_REALTIME_LAG),
        (resources.LATE, api.TRANSACTIONS_LATE_LAG),
        (resources.LOOKBACK, timedelta(days=45)),
    ]:
        fake.requests.clear()
        out = await collect(fetch_changes[name](LOG, NOW - lag - timedelta(hours=1)))
        assert out == [NOW - lag], name
        assert fake.requests[0]["postedBefore"] == api.to_iso(NOW - lag), name


@pytest.mark.asyncio
async def test_open_wires_the_backfill_to_start_date_and_window_size(monkeypatch: pytest.MonkeyPatch):
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(start), advanced={"window_size": "PT6H"})
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction("at-start", start), transaction("before-start", start - MS)],
        page_size=10,
    )
    resource = resources.financial_transactions(LOG, client_for(fake), config)
    await resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]

    out = await collect(recorded["fetch_page"](LOG, None, NOW - timedelta(hours=1)))
    window_end = api.to_iso(start + timedelta(hours=6))
    assert ids(out) == ["at-start"]
    assert out[-1] == window_end
    assert fake.requests == [{"postedAfter": api.to_iso(start), "postedBefore": window_end}]


class _Binding:
    stateKey = "financial_transactions"


class _CheckpointingTask:
    """
    Stands in for the CDK task, merge-patching each checkpoint into the
    persisted state the way the runtime does, so a later open can resume
    from what was saved.
    """

    def __init__(self, persisted: ResourceState) -> None:
        self.persisted = persisted
        self.checkpoints = 0

    async def checkpoint(self, state: ConnectorState, merge_patch: bool = True) -> None:
        assert merge_patch
        binding_state = state.bindingStateV1[_Binding.stateKey]
        assert binding_state is not None
        patch = binding_state.model_dump(mode="json", exclude_unset=True)
        self.persisted = ResourceState.model_validate(
            _merge_patch(self.persisted.model_dump(mode="json", exclude_unset=True), patch)
        )
        self.checkpoints += 1


def inc_cursor(state: ResourceState, subtask: str) -> datetime:
    """The cursor of one of the binding's incremental subtasks."""
    assert isinstance(state.inc, dict)
    inc = state.inc[subtask]
    assert inc is not None and isinstance(inc.cursor, datetime)
    return inc.cursor


def _merge_patch(target: Any, patch: Any):
    """RFC 7396 JSON merge patch."""
    if not isinstance(patch, dict):
        return patch
    merged = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = _merge_patch(merged.get(key), value)
    return merged


async def _open_lookback(
    monkeypatch: pytest.MonkeyPatch,
    fake: FakeAmazon,
    lookback: str,
    start_date: str,
    stored: datetime | ResourceState,
    at: datetime = NOW,
) -> tuple[datetime, dict, _CheckpointingTask]:
    """
    Opens the stream at `at` with a stored lookback cursor, or with a whole
    persisted state. Returns the cursor the lookback pass runs from, the
    arguments given to open_binding, and the task holding what was saved.
    """
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(at))
    config = make_config(
        ["ATVPDKIKX0DER"], start_date=start_date, advanced={"financial_transactions_lookback": lookback}
    )
    resource = resources.financial_transactions(LOG, client_for(fake), config)
    if isinstance(stored, ResourceState):
        state = stored.model_copy(deep=True)
    else:
        state = resource.initial_state.model_copy(deep=True)
        state.inc[resources.LOOKBACK].cursor = stored
    task = _CheckpointingTask(state.model_copy(deep=True))
    await resource.open(_Binding(), 0, state, task, [])  # type: ignore[arg-type,misc]
    return inc_cursor(state, resources.LOOKBACK), recorded, task


@pytest.mark.asyncio
async def test_raising_the_lookback_rereads_the_newly_covered_time(monkeypatch: pytest.MonkeyPatch):
    # The capture ran with a 30-day lookback, so its cursor sits 30 days
    # back. A transaction posted 100 days back was last read at 30 days old,
    # before Amazon released it at 60 days.
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction("released-at-60-days", NOW - timedelta(days=100), status="DEFERRED_RELEASED")],
        page_size=10,
    )
    cursor, recorded, _ = await _open_lookback(
        monkeypatch, fake, "P180D", "2025-01-01T00:00:00Z", NOW - timedelta(days=30)
    )
    assert cursor == NOW - timedelta(days=180)

    # Raised to 180 days, the pass reads it again once it is 180 days old.
    later = NOW + timedelta(days=81)
    monkeypatch.setattr(api, "now", lambda: later)
    fake.now = later
    lookback_pass = recorded["fetch_changes"][resources.LOOKBACK]
    emitted: list[str] = []
    while out := await collect(lookback_pass(LOG, cursor)):
        emitted += ids(out)
        cursor = cursors(out)[-1]
    assert emitted == ["released-at-60-days"]
    assert cursor == later - timedelta(days=180)


@pytest.mark.parametrize(
    "stored,start_date,expected",
    [
        # Steady state: the cursor is at or behind the horizon and stays put.
        (NOW - timedelta(days=95), "2025-01-01T00:00:00Z", NOW - timedelta(days=95)),
        (NOW - timedelta(days=90), "2025-01-01T00:00:00Z", NOW - timedelta(days=90)),
        # Raised past start_date: rewound only as far as start_date.
        (NOW - timedelta(days=30), api.to_iso(NOW - timedelta(days=50)), NOW - timedelta(days=50)),
        # A cursor held at start_date is already where it should be.
        (NOW - timedelta(days=50), api.to_iso(NOW - timedelta(days=50)), NOW - timedelta(days=50)),
    ],
)
@pytest.mark.asyncio
async def test_lookback_rewind_only_moves_back_to_the_horizon_or_start_date(monkeypatch: pytest.MonkeyPatch, stored: datetime, start_date: str, expected: datetime):
    cursor, _, task = await _open_lookback(monkeypatch, FakeAmazon(now=NOW, orders=[]), "P90D", start_date, stored)
    assert cursor == expected
    # The persisted cursor matches the one the pass runs from.
    assert inc_cursor(task.persisted, resources.LOOKBACK) == expected
    assert task.checkpoints == (1 if expected < stored else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("downtime", [timedelta(minutes=10), timedelta(days=2)])
async def test_lookback_rewind_survives_a_restart_before_the_first_window(monkeypatch: pytest.MonkeyPatch, downtime: timedelta):
    # Raised from 30 to 180 days, then restarted before the lookback pass
    # completed a window. The first poll after the rewind finds no full
    # window, so it saves nothing.
    fake = FakeAmazon(now=NOW, orders=[])
    first, recorded, task = await _open_lookback(
        monkeypatch, fake, "P180D", "2025-01-01T00:00:00Z", NOW - timedelta(days=30)
    )
    assert first == NOW - timedelta(days=180)
    assert await collect(recorded["fetch_changes"][resources.LOOKBACK](LOG, first)) == []

    # The restart resumes from what was persisted, so it keeps the first
    # rewind point instead of rewinding again from the later now.
    second, _, _ = await _open_lookback(
        monkeypatch, fake, "P180D", "2025-01-01T00:00:00Z", task.persisted, at=NOW + downtime
    )
    assert second == NOW - timedelta(days=180)
    # The rewind's checkpoint leaves the other passes' saved state alone.
    assert inc_cursor(task.persisted, resources.REALTIME) == NOW
    assert inc_cursor(task.persisted, resources.LATE) == NOW - api.TRANSACTIONS_LATE_LAG
    assert task.persisted.backfill == ResourceState.Backfill(
        next_page="2025-01-01T00:00:00+00:00", cutoff=NOW
    )


@pytest.mark.asyncio
async def test_response_without_payload_fails_instead_of_advancing():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[], payload_key="errors")
    with pytest.raises(PydanticValidationError):
        await collect(
            fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
        )


@pytest.mark.asyncio
async def test_response_with_moved_transactions_fails_instead_of_advancing():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[],
        transactions=[transaction("x", cursor)],
        transactions_key="financialTransactions",
    )
    out: list = []
    with pytest.raises(PydanticValidationError):
        async for item in fetch_financial_transactions(
            client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor
        ):
            out.append(item)
    assert out == []


@pytest.mark.asyncio
async def test_items_prefix_and_bound_params_come_from_the_model(monkeypatch: pytest.MonkeyPatch):
    cursor = NOW - timedelta(hours=1)
    monkeypatch.setattr(FinancialTransaction, "ITEMS_PREFIX", "payload.moved.item")
    fake = FakeAmazon(
        now=NOW, orders=[], transactions=[transaction("x", cursor)], transactions_key="moved"
    )
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert ids(out) == ["x"]
    assert set(fake.requests[0]) == {FinancialTransaction.SINCE_PARAM, FinancialTransaction.BEFORE_PARAM}


@pytest.mark.asyncio
async def test_transaction_without_posted_date_is_captured():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[], transactions=[transaction("x", cursor)], omit_posted_date=True)
    out = await collect(
        fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
    )
    assert ids(out) == ["x"]
    assert "postedDate" not in out[0].model_extra


@pytest.mark.asyncio
async def test_transaction_without_id_fails():
    cursor = NOW - timedelta(hours=1)
    missing_id = transaction("x", cursor)
    del missing_id["transactionId"]
    fake = FakeAmazon(now=NOW, orders=[], transactions=[missing_id])
    with pytest.raises(PydanticValidationError):
        await collect(
            fetch_financial_transactions(client_for(fake), api.TRANSACTIONS_REALTIME_LAG, DAY, LOG, cursor)
        )


def test_list_transactions_uses_its_own_default_limit():
    assert DEFAULT_LIMITS[Operation.LIST_TRANSACTIONS] == OperationLimit(rate=0.5, burst=10)


def test_lookback_defaults_past_invoiced_order_deferrals():
    assert make_config(["ATVPDKIKX0DER"]).advanced.financial_transactions_lookback == timedelta(days=90)


def test_late_pass_waits_out_amazons_48_hour_posting_delay():
    assert api.TRANSACTIONS_LATE_LAG > timedelta(hours=48)


def test_lookback_reads_after_the_late_pass():
    # The lookback read is the final one only if it comes after the late pass.
    with pytest.raises(PydanticValidationError):
        make_config(["ATVPDKIKX0DER"], advanced={"financial_transactions_lookback": api.TRANSACTIONS_LATE_LAG})


@pytest.mark.parametrize(
    "lookback,ok", [("P30D", True), ("P4D", True), ("P3D", False), ("P2D", False), ("P181D", False)]
)
def test_lookback_config_bounds(lookback: str, ok: bool):
    def build():
        return make_config(["ATVPDKIKX0DER"], advanced={"financial_transactions_lookback": lookback})

    if ok:
        assert build().advanced.financial_transactions_lookback == timedelta(days=int(lookback[1:-1]))
    else:
        with pytest.raises(PydanticValidationError):
            build()
