from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from source_amazon_selling_partner import api, resources
from source_amazon_selling_partner.client import Priority
from source_amazon_selling_partner.models import EndpointConfig, Order, ResourceState

from .fake_amazon import FakeAmazon, order
from .test_orders import LOG, MARKETPLACES, NOW, FrozenDatetime, client_for, collect, make_config

LOOKBACK = timedelta(hours=48)


@pytest.mark.parametrize("value", ["PT1H", "PT48H", "P7D"])
def test_orders_lookback_accepts_iso_duration(value):
    assert EndpointConfig.Advanced.model_validate({"orders_lookback": value}).orders_lookback >= timedelta(hours=1)
    assert EndpointConfig.Advanced().orders_lookback == LOOKBACK


@pytest.mark.parametrize("value", ["PT59M", "PT0S", "-P1D", "invalid"])
def test_orders_lookback_rejects_invalid_duration(value):
    with pytest.raises(ValidationError):
        EndpointConfig.Advanced.model_validate({"orders_lookback": value})


@pytest.mark.asyncio
async def test_sweep_ranges_chain_when_runs_are_late_and_slow(monkeypatch):
    fake = FakeAmazon(now=NOW, orders=[])
    monkeypatch.setattr(api, "now", lambda: fake.now)
    client = client_for(fake)
    covered = NOW - LOOKBACK - timedelta(days=1)
    first = covered
    ranges = []
    original_get = client.get

    async def get(log, operation, path, params, priority):
        result = await original_get(log, operation, path, params, priority)
        fake.now += timedelta(hours=8)
        return result

    monkeypatch.setattr(client, "get", get)
    for delay in [timedelta(), timedelta(days=3), timedelta(days=4)]:
        fake.now += delay
        horizon = api.floor_to_tick(fake.now - LOOKBACK)
        before = len(fake.requests)
        out = await collect(api.fetch_orders_sweep(
            client, MARKETPLACES, LOOKBACK, timedelta(hours=12), LOG, covered
        ))
        assert out[-1] == horizon
        assert all(a < b for a, b in zip([covered] + out, out))
        for request in fake.requests[before:]:
            ranges.append((datetime.fromisoformat(request["lastUpdatedAfter"]),
                           datetime.fromisoformat(request["lastUpdatedBefore"])))
        covered = out[-1]
    assert ranges[0][0] == first + api.TICK
    assert all(lo == previous_hi + api.TICK for (_, previous_hi), (lo, _) in zip(ranges, ranges[1:]))
    assert ranges[-1][1] == covered


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [timedelta(hours=2), LOOKBACK])
async def test_sweep_emits_orders_indexed_up_to_lookback_late(monkeypatch, delay):
    updated = NOW - LOOKBACK
    fake = FakeAmazon(now=NOW, orders=[order("late", updated, updated)],
                      order_index_delays={"late": delay})
    monkeypatch.setattr(api, "now", lambda: fake.now)
    out = await collect(api.fetch_orders_sweep(
        client_for(fake), MARKETPLACES, LOOKBACK, timedelta(days=1), LOG,
        updated - timedelta(days=1),
    ))
    assert [o.orderId for o in out if isinstance(o, Order)] == ["late"]
    assert out[-1] == updated


@pytest.mark.asyncio
async def test_sweep_includes_window_seams_and_range_end(monkeypatch):
    end = NOW - LOOKBACK
    covered = end - timedelta(days=2)
    seam = covered + timedelta(days=1)
    fake = FakeAmazon(now=NOW, orders=[
        order("seam", covered, seam),
        order("after-seam", covered, seam + api.TICK),
        order("end", covered, end),
    ])
    monkeypatch.setattr(api, "now", lambda: fake.now)
    out = await collect(api.fetch_orders_sweep(
        client_for(fake), MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, covered
    ))
    assert [o.orderId for o in out if isinstance(o, Order)] == ["seam", "after-seam", "end"]
    assert [item for item in out if isinstance(item, datetime)] == [seam, end]
    assert fake.requests[1]["lastUpdatedAfter"] == api.to_iso(seam + api.TICK)
    assert fake.requests[-1]["lastUpdatedBefore"] == api.to_iso(end)


@pytest.mark.asyncio
async def test_restart_resumes_after_last_complete_window(monkeypatch):
    start = NOW - LOOKBACK - timedelta(days=3)
    fake = FakeAmazon(now=NOW, orders=[
        order(str(i), start, start + timedelta(days=i, hours=1)) for i in range(3)
    ])
    monkeypatch.setattr(api, "now", lambda: fake.now)
    client = client_for(fake)
    gen = api.fetch_orders_sweep(client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, start)
    assert (await anext(gen)).orderId == "0"
    checkpoint = await anext(gen)
    saved = ResourceState.Incremental.model_validate_json(
        ResourceState.Incremental(cursor=checkpoint).model_dump_json()
    ).cursor
    assert saved == start + timedelta(days=1)
    assert (await anext(gen)).orderId == "1"
    await gen.aclose()

    fake.now += timedelta(days=2)
    before = len(fake.requests)
    resumed = await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, saved
    ))
    assert [o.orderId for o in resumed if isinstance(o, Order)] == ["1", "2"]
    assert fake.requests[before]["lastUpdatedAfter"] == api.to_iso(saved + api.TICK)
    assert resumed[-1] == fake.now - LOOKBACK


@pytest.mark.asyncio
async def test_no_request_before_a_full_day_is_due(monkeypatch):
    fake = FakeAmazon(now=NOW, orders=[])
    monkeypatch.setattr(api, "now", lambda: fake.now)
    covered = NOW - LOOKBACK
    client = client_for(fake)
    for delay in [timedelta(), timedelta(days=1) - api.TICK]:
        fake.now = NOW + delay
        assert await collect(api.fetch_orders_sweep(
            client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, covered
        )) == []
    assert fake.requests == []
    fake.now = NOW + timedelta(days=1)
    assert await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, covered
    )) == [covered + timedelta(days=1)]


@pytest.mark.asyncio
async def test_sweep_reuses_token_halving_and_backfill_priority(monkeypatch):
    monkeypatch.setattr(api, "now", lambda: NOW)
    covered = NOW - LOOKBACK - timedelta(days=1)
    fake = FakeAmazon(now=NOW, orders=[
        order(str(i), covered, covered + timedelta(minutes=i + 1)) for i in range(4)
    ], expire_token_once=True)
    client = client_for(fake)
    priorities = []
    original_get = client.get

    async def get(log, operation, path, params, priority):
        priorities.append(priority)
        return await original_get(log, operation, path, params, priority)

    monkeypatch.setattr(client, "get", get)
    out = await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, covered
    ))
    assert {o.orderId for o in out if isinstance(o, Order)} == {"0", "1", "2", "3"}
    assert set(priorities) == {Priority.BACKFILL}
    assert fake.requests[2]["lastUpdatedBefore"] < fake.requests[0]["lastUpdatedBefore"]
    assert out[-1] == NOW - LOOKBACK


def test_initial_sweep_cursor_starts_at_cutoff(monkeypatch):
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(NOW))
    state = resources.orders(LOG, client_for(FakeAmazon(now=NOW, orders=[])), make_config(MARKETPLACES)).initial_state
    assert state.inc[resources.SWEEP].cursor == state.backfill.cutoff - api.TICK
