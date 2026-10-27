"""
Tests for price_list_prices: price lines from the bulk JSONL have no id and are attributed to
their price list through `__parentId`, and the stream is a full-refresh snapshot that sweeps
less often than the snapshot default.
"""

import json
from datetime import UTC, datetime, timedelta
from logging import Logger
from unittest.mock import MagicMock, patch

import pytest

import source_shopify_native.graphql as gql
from source_shopify_native.graphql.price_lists import PriceListPrices
from source_shopify_native.models import EndpointConfig, StoreCapabilities
from source_shopify_native.resources import StoreContext, discovered_resources

NOW = datetime(2024, 1, 1, tzinfo=UTC)
PRICE_LIST_ID = "gid://shopify/PriceList/1"


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


async def _lines(*records: dict):
    for record in records:
        yield json.dumps(record).encode()


PRICE_LIST = {
    "id": PRICE_LIST_ID,
    "name": "Mexico Buyers",
    "currency": "MXN",
    "parent": {
        "adjustment": {"type": "PERCENTAGE_DECREASE", "value": 5.0},
        "settings": {"compareAtMode": "ADJUSTED"},
    },
    "catalog": {
        "__typename": "MarketCatalog",
        "id": "gid://shopify/MarketCatalog/1",
        "title": "Mexico",
        "status": "ACTIVE",
    },
}


def _price(origin_type: str, variant_id: str, parent_id: str = PRICE_LIST_ID) -> dict:
    return {
        "originType": origin_type,
        "price": {"amount": "11000.0", "currencyCode": "MXN"},
        "compareAtPrice": None,
        "variant": {
            "id": variant_id,
            "product": {"id": "gid://shopify/Product/1"},
        },
        "__parentId": parent_id,
    }


def test_query_sweeps_every_price_list_without_a_date_filter():
    query = PriceListPrices.build_query(NOW, NOW)

    assert "priceLists {" in query
    assert "updated_at" not in query
    assert "first:" not in query
    assert "originType" in query


@pytest.mark.asyncio
async def test_prices_carry_their_price_list(log):
    fixed = _price("FIXED", "gid://shopify/ProductVariant/1")
    relative = _price("RELATIVE", "gid://shopify/ProductVariant/2")
    lines = _lines(PRICE_LIST, fixed, relative)

    docs = [doc async for doc in PriceListPrices.process_result(log, lines)]

    assert [d["originType"] for d in docs] == ["FIXED", "RELATIVE"]
    assert all(d["priceList"] == PRICE_LIST for d in docs)
    assert all("__parentId" not in d for d in docs)
    assert docs[1]["variant"]["id"] == "gid://shopify/ProductVariant/2"

    validated = PriceListPrices.model_validate(docs[0])
    assert validated.id is None
    assert "id" not in validated.model_dump(by_alias=True, exclude_unset=True)


@pytest.mark.asyncio
async def test_price_before_its_price_list_raises(log):
    lines = _lines(_price("FIXED", "gid://shopify/ProductVariant/1"), PRICE_LIST)

    with pytest.raises(RuntimeError):
        _ = [doc async for doc in PriceListPrices.process_result(log, lines)]


@pytest.mark.asyncio
async def test_unidentified_line_raises(log):
    lines = _lines(PRICE_LIST, {"id": "gid://shopify/Product/1", "__parentId": PRICE_LIST_ID})

    with pytest.raises(RuntimeError):
        _ = [doc async for doc in PriceListPrices.process_result(log, lines)]


@pytest.mark.asyncio
async def test_discovered_as_a_snapshot_with_its_own_interval(log):
    context = StoreContext(
        http=MagicMock(),
        client=MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=StoreCapabilities(scopes=frozenset({"read_products"})),
        available_resources={gql.Markets, gql.PriceListPrices},
    )

    async def fake_create_store_context(log, http, store_config, should_cancel_ongoing_job, max_concurrent_bulk_ops):
        return context

    config = EndpointConfig.model_validate(
        {
            "stores": [
                {
                    "store": "store",
                    "credentials": {
                        "credentials_title": "Private App Credentials",
                        "access_token": "tok",
                    },
                }
            ],
        }
    )
    with patch(
        "source_shopify_native.resources._create_store_context",
        side_effect=fake_create_store_context,
    ):
        resources = {r.name: r for r in await discovered_resources(log, MagicMock(), config)}

    price_list_prices = resources["price_list_prices"]
    assert price_list_prices.key == ["/_meta/store", "/_meta/row_id"]
    assert price_list_prices.initial_config.interval == timedelta(hours=6)
    assert resources["markets"].initial_config.interval == timedelta(minutes=15)
