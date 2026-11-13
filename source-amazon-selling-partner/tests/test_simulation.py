"""Seeded, offline runtime simulations. All wall time and HTTP are fake.

The inherited transport has no HTTP-session retry layer, so injecting 429s
would bypass the CDK retry behavior rather than exercise it. We omit them.
Keeping postedDate when a DEFERRED transaction is released is an explicit
scenario assumption (Amazon's behavior here is not yet verified).

Mutation audit: from this connector directory run
    PATH=$PWD/../.venv/bin:$PATH python tests/run_simulation_mutations.py
The helper snapshots bytes, applies each mutation alone, runs only this file,
and restores in finally; API edits are temporary. Verified M1--M6 all KILLED:
M1 orders checkpoint + configured overlap (10m) + 1h: completed-window oracle.
M2 finance backfill shrink assignment removed: repeated slow window budget.
M3 orders backfill checkpoint uses original end: completed-window oracle.
M4 finance checkpoint + 1h: completed-window oracle.
M5 runner skips late subtask: independent late-pass coverage.
M6 runner commits pending docs on crash: rollback oracle.
A skip inside the orders overlap may legitimately be absorbed on the next
poll. Final keyed-state equality permits duplicates but rejects missing keys
and stale versions. M6 is a harness negative control: keeping rolled-back docs
can conceal a loss bug, and cannot itself be expected to create missing keys.
The rollback check validates the simulated crash; it does not reject replay
or overlap duplicates that are committed after recovery.
"""

import logging
import random
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

import pytest
from estuary_cdk.http import HTTPError

from source_amazon_selling_partner import api, resources
from source_amazon_selling_partner.client import SPAPIClient
from source_amazon_selling_partner.models import FinancialTransaction, Order

from .fake_amazon import FakeAmazon, order, parse, transaction
from .test_orders import FakeTime, FrozenDatetime, MARKETPLACES, make_config

LOG = logging.getLogger("test.simulation")
START = datetime(2026, 1, 1, tzinfo=UTC)
DAY = timedelta(days=1)
LOOKBACK = 7 * DAY
BUSY = START + 10 * DAY
SEED = 270027


class SimulationTime(FakeTime):
    """One clock for API horizons, HTTP latency, sleeps and rate limiting."""

    def __init__(self, at: datetime):
        super().__init__()
        self.origin = at

    def now(self) -> datetime:
        return self.origin + timedelta(seconds=self.t)

    def advance(self, delta: timedelta) -> None:
        self.t += delta.total_seconds()


class SimulationAmazon(FakeAmazon):
    def __init__(self, clock: SimulationTime, kind: str):
        super().__init__(now=clock.now(), orders=[])
        self.time = clock
        self.kind = kind
        self.history: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
        self.token_windows: dict[str, tuple[datetime, datetime, datetime]] = {}
        self.window_attempts: dict[tuple[datetime, datetime], int] = {}
        self.stalled = False
        self.expired_windows: list[tuple[datetime, datetime]] = []
        self.expirations = 0
        self.page_lengths: list[int] = []
        rng = random.Random(SEED)
        quiet = {2, 17, 36, 53} if kind == "orders" else {2, 7, 17, 23, 36, 41, 47, 53, 57}
        for day in range(60):
            count = (300 if kind == "orders" else 250) if day == 10 else (
                0 if day in quiet else (30 if kind == "orders" else 25)
            )
            # Include exact day boundaries, and jitter all other records.
            seconds = [0] + sorted(rng.sample(range(1, 86400), count - 1)) if count else []
            for second in seconds:
                at = START + day * DAY + timedelta(seconds=second)
                key = f"{kind}-{len(self.history):04d}"
                index = len(self.history)
                if kind == "orders":
                    versions = [(at, order(key, at, at))]
                    if index % 7 == 0:
                        updated = at + 3 * DAY
                        versions.append((updated, order(key, at, updated)))
                else:
                    visible = at + (2 * DAY if index % 9 == 0 else timedelta())
                    deferred = index % 11 == 0
                    versions = [(visible, transaction(key, at, "DEFERRED" if deferred else "RELEASED"))]
                    if deferred:
                        versions.append((at + 4 * DAY, transaction(key, at, "RELEASED")))
                self.history[key] = versions

    def visible(self) -> list[dict[str, Any]]:
        result = []
        for versions in self.history.values():
            available = [doc for at, doc in versions if at <= self.now]
            if available:
                result.append(available[-1])
        return result

    def _respond(self, body):
        docs = body["orders"] if self.kind == "orders" else body["payload"]["transactions"]
        self.page_lengths.append(len(docs))
        assert len(docs) <= self.page_size
        return super()._respond(body)

    async def request_stream(self, log, url, method="GET", params=None, **kwargs):
        params = dict(params or {})
        token = params.get("paginationToken", params.get("nextToken"))
        if token:
            issued, lo, hi = self.token_windows[token]
            # A full busy-day window stalls; a smaller retry can complete.
            if lo < BUSY + DAY and hi > BUSY and hi - lo >= DAY - api.TICK:
                self.time.advance(DAY + timedelta(seconds=1))
                self.stalled = True
            if self.time.now() - issued >= DAY:
                self.expirations += 1
                self.expired_windows.append((lo, hi))
                self.time.advance(timedelta(seconds=2))
                raise HTTPError("400 Bad Request: InvalidInput", 400)
        else:
            lower = "createdAfter" if "createdAfter" in params else (
                "lastUpdatedAfter" if "lastUpdatedAfter" in params else "postedAfter"
            )
            upper = {"createdAfter": "createdBefore", "lastUpdatedAfter": "lastUpdatedBefore", "postedAfter": "postedBefore"}[lower]
            issued, lo, hi = self.time.now(), parse(params[lower]), parse(params[upper])
            # Bound repeated attempts at exactly the same slow window. A
            # broken shrink loop must fail promptly instead of spinning.
            window = (lo, hi)
            self.window_attempts[window] = self.window_attempts.get(window, 0) + 1
            assert self.window_attempts[window] <= 4, "pagination failed to converge after token expiry"
        self.now = self.time.now()
        self.page_size = int(params["maxResultsPerPage"]) if self.kind == "orders" else 100
        if self.kind == "orders":
            self.orders = self.visible()
        else:
            self.transactions = self.visible()
        old_tokens = set(self._pages) | set(self._transaction_pages)
        response = await super().request_stream(log, url, method, params, **kwargs)
        new_tokens = (set(self._pages) | set(self._transaction_pages)) - old_tokens
        for new_token in new_tokens:
            # Every token in a chain expires relative to its FIRST page.
            self.token_windows[new_token] = (issued, lo, hi)
        self.time.advance(timedelta(seconds=2))
        return response


class Runtime:
    """Commit at yielded cursors or successful end-of-backfill only."""

    def __init__(self, amazon: SimulationAmazon):
        self.amazon = amazon
        self.client = self.new_client()
        self.committed: list[Any] = []
        self.invocations = 0
        self.attempted_docs = 0
        rng = random.Random(SEED)
        self.restart_points = [rng.randrange(20, 80), rng.randrange(600, 750), rng.randrange(2000, 2100)]
        self.restarts = 0
        self.discarded = 0

    def new_client(self):
        return SPAPIClient(http=self.amazon, base_url="https://fake", clock=self.amazon.time.clock, sleep=self.amazon.time.sleep)  # type: ignore[arg-type]

    async def invoke(self, factory, cursor):
        self.invocations += 1
        assert self.invocations < 600, "simulation failed to converge"
        pending = []
        gen = factory(cursor)
        request_start = len(self.amazon.requests)
        try:
            async for item in gen:
                if isinstance(item, (Order, FinancialTransaction)):
                    pending.append(item)
                    self.attempted_docs += 1
                    if self.attempted_docs in self.restart_points:
                        self.discarded += len(pending)
                        self.restarts += 1
                        self.client = self.new_client()
                        return cursor, False
                else:
                    # A checkpoint commits all preceding documents atomically.
                    previous = parse(cursor) if isinstance(cursor, str) else cursor
                    next_cursor = parse(item) if isinstance(item, str) else item
                    assert next_cursor > previous
                    # Check against the actual completed window, including retries.
                    # Later overlap/lookback passes must not hide a skipped seam.
                    request = next(r for r in reversed(self.amazon.requests[request_start:])
                                   if "createdBefore" in r or "lastUpdatedBefore" in r or "postedBefore" in r)
                    upper = next(k for k in ("createdBefore", "lastUpdatedBefore", "postedBefore") if k in request)
                    expected_end = parse(request[upper])
                    if upper == "createdBefore":
                        expected_end += api.TICK
                    assert next_cursor == expected_end, "checkpoint skipped beyond the completed window"
                    self.committed.extend(pending)
                    pending.clear()
                    cursor = item
                    return cursor, False
            self.committed.extend(pending)
            return cursor, True
        finally:
            await gen.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,expected_count", [("orders", 1950), ("finance", 1500)])
async def test_fake_clock_backfill_then_incremental(kind, expected_count, monkeypatch, caplog, record_property):
    began = perf_counter()
    cutoff = START + 30 * DAY
    clock = SimulationTime(cutoff)
    monkeypatch.setattr(api, "now", clock.now)
    monkeypatch.setattr(api.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff))
    amazon = SimulationAmazon(clock, kind)
    runtime = Runtime(amazon)
    config = make_config(MARKETPLACES, start_date=api.to_iso(START), advanced={"financial_transactions_lookback": "P7D"})
    resource = resources.orders if kind == "orders" else resources.financial_transactions
    state = resource(LOG, runtime.client, config).initial_state
    assert state.backfill.cutoff == cutoff
    page = state.backfill.next_page

    async def invoke(factory, saved):
        # Independent harness check: a crash before a checkpoint commits nothing.
        # This allows replay duplicates after recovery, but prevents the oracle
        # from using rolled-back documents to conceal data loss.
        committed_before = list(runtime.committed)
        restarts_before = runtime.restarts
        result = await runtime.invoke(factory, saved)
        if runtime.restarts != restarts_before:
            assert runtime.committed == committed_before, "restart retained uncheckpointed documents"
            assert result == (saved, False)
        return result
    backfill_starts = []
    with caplog.at_level(logging.WARNING, logger=LOG.name):
        while True:
            before = len(amazon.requests)
            old_page = page
            def backfill(saved):
                if kind == "orders":
                    return api.backfill_orders(runtime.client, MARKETPLACES, START, DAY, LOG, saved, cutoff)
                return api.backfill_financial_transactions(runtime.client, START, DAY, LOG, saved, cutoff)
            page, done = await invoke(backfill, page)
            first = amazon.requests[before]
            lower, upper = ("createdAfter", "createdBefore") if kind == "orders" else ("postedAfter", "postedBefore")
            assert parse(first[lower]) == parse(old_page)
            if page != old_page:
                backfill_starts.append(parse(page))
            if done:
                # Last backfill window meets the initial realtime cursor.
                last_end = parse(amazon.requests[-1][upper])
                assert last_end == (cutoff - api.TICK if kind == "orders" else cutoff)
                break
        assert backfill_starts == sorted(set(backfill_starts))
        assert amazon.expirations >= 1, "backfill must exercise token expiration"
        # Every expired window must have been retried at a smaller upper bound.
        for lo, hi in amazon.expired_windows:
            assert any(parse(r[lower]) == lo and parse(r[upper]) < hi
                       for r in amazon.requests if lower in r), "expired backfill window never shrank"
        if kind == "orders":
            cursors = {"orders": state.inc.cursor}
            lags = {"orders": api.SEARCH_LAG}
            assert cursors["orders"] == cutoff - api.TICK
        else:
            cursors = {name: inc.cursor for name, inc in state.inc.items()}
            lags = {resources.REALTIME: api.TRANSACTIONS_REALTIME_LAG,
                    resources.LATE: api.TRANSACTIONS_LATE_LAG,
                    resources.LOOKBACK: LOOKBACK}
            assert cursors[resources.REALTIME] == cutoff
            assert cursors[resources.LOOKBACK] == cutoff - LOOKBACK
        by_pass = {name: [] for name in cursors}
        # Poll daily until the final lookback can see even the latest releases.
        finish = START + 70 * DAY
        while True:
            for name, lag in lags.items():
                while True:
                    old = cursors[name]
                    before = len(amazon.requests)
                    def fetch(saved, lag=lag):
                        if kind == "orders":
                            return api.fetch_orders(runtime.client, MARKETPLACES, DAY, config.advanced.overlap, LOG, saved)
                        return api.fetch_financial_transactions(runtime.client, lag, DAY, LOG, saved)
                    committed_before = len(runtime.committed)
                    cursors[name], idle = await invoke(fetch, old)
                    by_pass[name].extend(runtime.committed[committed_before:])
                    if len(amazon.requests) > before:
                        request = amazon.requests[before]
                        lower = "lastUpdatedAfter" if kind == "orders" else "postedAfter"
                        expected = old + api.TICK - config.advanced.overlap if kind == "orders" else old
                        assert parse(request[lower]) == expected
                    if idle:
                        assert api.floor_to_tick(clock.now() - lag) - cursors[name] < api.MIN_INCREMENTAL_WINDOW
                        break
            if clock.now() >= finish:
                break
            clock.advance(min(DAY, finish - clock.now()))

    assert len(amazon.history) == expected_count
    key_field = "orderId" if kind == "orders" else "transactionId"
    model = Order if kind == "orders" else FinancialTransaction
    last = {getattr(doc, key_field): doc for doc in runtime.committed}
    expected = {key: model.model_validate(versions[-1][1]) for key, versions in amazon.history.items()}
    assert last == expected, {
        "missing": expected.keys() - last.keys(),
        "unexpected": last.keys() - expected.keys(),
        "stale": {key for key in expected.keys() & last.keys() if last[key] != expected[key]},
    }
    assert len(runtime.committed) > len(last), "scenario must allow duplicate keys"
    # Verify the simulation actually observed changing records, not just finals.
    if kind == "orders":
        assert any(doc.lastUpdatedTime != expected[doc.orderId].lastUpdatedTime for doc in runtime.committed)
    else:
        assert any(doc.transactionStatus == "DEFERRED" for doc in runtime.committed)
        # A genuinely late record was absent on realtime and caught later.
        late_key = next(key for key, versions in amazon.history.items()
                        if cutoff < parse(versions[0][1]["postedDate"]) < cutoff + DAY
                        and versions[0][0] - parse(versions[0][1]["postedDate"]) == 2 * DAY)
        assert late_key not in {doc.transactionId for doc in by_pass[resources.REALTIME]}
        assert late_key in {doc.transactionId for doc in by_pass[resources.LATE]}
        # Check late-pass coverage independently of the final collection: the
        # lookback can otherwise mask an entirely missing late subtask.
        late_expected = {key for key, versions in amazon.history.items()
                         if cutoff <= parse(versions[-1][1]["postedDate"]) < finish - api.TRANSACTIONS_LATE_LAG}
        assert late_expected <= {doc.transactionId for doc in by_pass[resources.LATE]}
        lookback = {doc.transactionId: doc for doc in by_pass[resources.LOOKBACK]}
        deferred_keys = {doc.transactionId for doc in by_pass[resources.REALTIME]
                         if doc.transactionStatus == "DEFERRED"}
        assert deferred_keys
        assert all(lookback[key].transactionStatus == "RELEASED" for key in deferred_keys)
    assert runtime.restarts == 3
    assert runtime.discarded > 0
    assert amazon.stalled and amazon.expirations >= 1
    assert max(amazon.page_lengths) == (Order.PAGE_SIZE if kind == "orders" else 100)
    warnings = [r for r in caplog.records if "shrinking" in r.getMessage()]
    assert any(r.args["window_start"] <= BUSY < r.args["window_end"] for r in warnings)
    assert all(r.args["new_window_end"] < r.args["window_end"] for r in warnings)
    elapsed = perf_counter() - began
    record_property("simulation", {"keys": len(last), "committed_docs": len(runtime.committed),
                                   "invocations": runtime.invocations, "restarts": runtime.restarts,
                                   "expired_tokens": amazon.expirations, "seconds": round(elapsed, 3)})
    assert elapsed < 5
