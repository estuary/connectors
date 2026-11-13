import logging
from datetime import timedelta
from typing import cast

import pytest
from estuary_cdk.http import HTTPSession

from source_amazon_selling_partner import api
from source_amazon_selling_partner.client import SPAPIClient

from .fake_amazon import FakeAmazon, order, transaction
from .test_orders import LOG, MARKETPLACES, NOW, FakeTime, collect


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["orders", "finance", "sweep"])
@pytest.mark.parametrize("count", [0, 3])
async def test_finished_windows_log_progress_at_info(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, stream: str, count: int):
    start = NOW - timedelta(days=4)
    size = timedelta(days=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), start + timedelta(minutes=i + 1), start + timedelta(minutes=i + 1))
                for i in range(count)],
        transactions=[transaction(str(i), start + timedelta(minutes=i + 1)) for i in range(count)],
    )
    time = FakeTime()
    monkeypatch.setattr(api, "now", lambda: fake.now)
    monkeypatch.setattr(api, "monotonic", time.clock)
    client = SPAPIClient(http=cast(HTTPSession, fake), base_url="https://fake", clock=time.clock, sleep=time.sleep)
    with caplog.at_level(logging.INFO, logger=LOG.name):
        if stream == "orders":
            await collect(api.backfill_orders(client, MARKETPLACES, start, size, LOG, None, start + size))
            message = "backfilled orders window"
            bounds = [(start, start + size - api.TICK)]
        elif stream == "finance":
            await collect(api.backfill_financial_transactions(client, start, size, LOG, None, start + size))
            message = "backfilled financial transactions window"
            bounds = [(start, start + size)]
        else:
            await collect(api.fetch_orders_sweep(client, MARKETPLACES, timedelta(days=2), size, LOG, start))
            message = "swept orders window"
            bounds = [(start + api.TICK, start + size), (start + size + api.TICK, start + size * 2)]
    records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert [r.msg for r in records] == [message] * len(bounds)
    elapsed = [cast(dict[str, float], record.args)["elapsed_seconds"] for record in records]
    assert all(seconds >= 0 for seconds in elapsed)
    assert sum(elapsed) == pytest.approx(time.t)
    for i, (record, (window_start, window_end)) in enumerate(zip(records, bounds)):
        assert record.args == {
            "window_start": window_start,
            "window_end": window_end,
            "count": count if i == 0 else 0,
            "window_seconds": size.total_seconds(),
            "elapsed_seconds": elapsed[i],
        }
    if count:
        assert time.t > 0  # Paging waits for rate-limit tokens.


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["orders", "finance"])
async def test_incremental_window_progress_stays_at_debug(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, stream: str):
    monkeypatch.setattr(api, "now", lambda: NOW)
    fake = FakeAmazon(now=NOW, orders=[])
    time = FakeTime()
    client = SPAPIClient(http=cast(HTTPSession, fake), base_url="https://fake", clock=time.clock, sleep=time.sleep)
    with caplog.at_level(logging.DEBUG, logger=LOG.name):
        if stream == "orders":
            await collect(api.fetch_orders(
                client, MARKETPLACES, timedelta(days=1), timedelta(), LOG, NOW - timedelta(days=2)
            ))
            message = "fetched orders window"
        else:
            await collect(api.fetch_financial_transactions(
                client, timedelta(hours=1), timedelta(days=1), LOG, NOW - timedelta(days=2)
            ))
            message = "fetched financial transactions window"
    records = [r for r in caplog.records if r.msg == message]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert not [r for r in caplog.records if r.levelno == logging.INFO]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["orders", "finance"])
async def test_backfill_progress_excludes_freshness_wait_and_has_actual_window_size(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, stream: str):
    start = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[])
    time = FakeTime()
    waited = []

    async def sleep(seconds: float):
        await time.sleep(seconds)
        fake.now += timedelta(seconds=seconds)

    async def freshness_wait(seconds: float):
        waited.append(seconds)
        await sleep(seconds)

    monkeypatch.setattr(api, "now", lambda: fake.now)
    monkeypatch.setattr(api, "monotonic", time.clock)
    monkeypatch.setattr(api.asyncio, "sleep", freshness_wait)
    client = SPAPIClient(http=cast(HTTPSession, fake), base_url="https://fake", clock=time.clock, sleep=sleep)
    with caplog.at_level(logging.INFO, logger=LOG.name):
        if stream == "orders":
            await collect(api.backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, None, NOW))
            wait = (api.SEARCH_LAG - api.TICK).total_seconds()
        else:
            await collect(api.backfill_financial_transactions(client, start, timedelta(days=1), LOG, None, NOW))
            wait = api.TRANSACTIONS_REALTIME_LAG.total_seconds()
    records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(records) == 1
    assert cast(dict[str, float], records[0].args)["window_seconds"] == 3600
    assert sum(waited) >= wait
    assert cast(dict[str, float], records[0].args)["elapsed_seconds"] == pytest.approx(time.t - sum(waited))


@pytest.mark.asyncio
async def test_backfill_progress_times_only_the_attempt_that_finished(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), start + timedelta(minutes=i), start + timedelta(minutes=i)) for i in range(1, 4)],
        expire_token_once=True,
    )
    time = FakeTime()
    monkeypatch.setattr(api, "now", lambda: fake.now)
    monkeypatch.setattr(api, "monotonic", time.clock)
    client = SPAPIClient(http=cast(HTTPSession, fake), base_url="https://fake", clock=time.clock, sleep=time.sleep)
    with caplog.at_level(logging.INFO, logger=LOG.name):
        await collect(api.backfill_orders(client, MARKETPLACES, start, timedelta(days=2), LOG, None, NOW - timedelta(hours=1)))
    records = [r for r in caplog.records if r.msg == "backfilled orders window"]
    assert len(records) == 1
    assert cast(dict[str, float], records[0].args)["window_seconds"] == timedelta(days=1).total_seconds()
    assert 0 < cast(dict[str, float], records[0].args)["elapsed_seconds"] < time.t
