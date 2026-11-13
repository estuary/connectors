import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from source_amazon_selling_partner import api, resources
from source_amazon_selling_partner.client import Operation, Priority
from source_amazon_selling_partner.models import EndpointConfig, Order, ResourceState

from .fake_amazon import FakeAmazon, order
from .test_orders import LOG, MARKETPLACES, NOW, FrozenDatetime, client_for, collect, make_config

LOOKBACK = timedelta(hours=48)
COMPLETED = (f"completed|{api.to_iso(NOW - timedelta(days=1))}",)


@pytest.mark.parametrize("value", ["PT1H", "PT48H", "P7D"])
def test_orders_lookback_accepts_iso_duration(value):
    assert EndpointConfig.Advanced.model_validate({"orders_lookback": value}).orders_lookback >= timedelta(hours=1)
    assert EndpointConfig.Advanced().orders_lookback == LOOKBACK


@pytest.mark.parametrize("value", ["PT59M", "PT0S", "-P1D", "invalid"])
def test_orders_lookback_rejects_invalid_duration(value):
    with pytest.raises(ValidationError):
        EndpointConfig.Advanced.model_validate({"orders_lookback": value})


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_after", [1, 2, 3])
async def test_restart_replays_frozen_sweep_without_advancing_main_cursor(monkeypatch, crash_after):
    monkeypatch.setattr(api, "now", lambda: NOW)
    fake = FakeAmazon(now=NOW, orders=[
        order(str(i), NOW - timedelta(days=3), NOW - LOOKBACK + timedelta(seconds=i))
        for i in range(4)
    ])
    client = client_for(fake)
    main_cursor = NOW - api.SEARCH_LAG
    gen = api.fetch_orders_sweep(client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, COMPLETED)
    pending = await anext(gen)
    # Round-trip through the same JSON/model boundary as persisted CDK state.
    saved = ResourceState.Incremental.model_validate_json(
        ResourceState.Incremental(cursor=pending).model_dump_json()
    ).cursor
    assert saved[0].startswith("pending|")
    for _ in range(crash_after):
        assert isinstance(await anext(gen), Order)
    await gen.aclose()
    first_request = dict(fake.requests[0])

    # Even a restart after the oldest order ages out must retry its saved range.
    fake.now += timedelta(days=3)
    monkeypatch.setattr(api, "now", lambda: fake.now)
    client = client_for(fake)
    resumed = await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, timedelta(hours=1), timedelta(days=1), LOG, saved
    ))
    assert {o.orderId for o in resumed if isinstance(o, Order)} == {"0", "1", "2", "3"}
    assert resumed[-1] == (f"completed|{api.to_iso(fake.now)}",)
    assert first_request == fake.requests[-3]
    assert main_cursor == NOW - api.SEARCH_LAG
    assert client.bucket(Operation.SEARCH_ORDERS).reserve_for(Priority.BACKFILL) == 2
    before = len(fake.requests)
    assert await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, resumed[-1]
    )) == []
    fake.now += timedelta(days=1) - timedelta(seconds=1)
    assert await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, resumed[-1]
    )) == []
    assert len(fake.requests) == before


@pytest.mark.asyncio
async def test_sweep_reuses_token_halving_and_backfill_priority(monkeypatch):
    monkeypatch.setattr(api, "now", lambda: NOW)
    fake = FakeAmazon(now=NOW, orders=[
        order(str(i), NOW - timedelta(days=3), NOW - LOOKBACK + timedelta(minutes=i))
        for i in range(4)
    ], expire_token_once=True)
    client = client_for(fake)
    priorities = []
    original_get = client.get

    async def get(log, operation, path, params, priority):
        priorities.append(priority)
        return await original_get(log, operation, path, params, priority)

    monkeypatch.setattr(client, "get", get)
    out = await collect(api.fetch_orders_sweep(
        client, MARKETPLACES, LOOKBACK, timedelta(days=1), LOG, COMPLETED
    ))
    assert {o.orderId for o in out if isinstance(o, Order)} == {"0", "1", "2", "3"}
    assert set(priorities) == {Priority.BACKFILL}
    assert fake.requests[2]["lastUpdatedBefore"] < fake.requests[0]["lastUpdatedBefore"]
    assert out[-1][0].startswith("completed|")
