"""
Tests for `price_lists`, a snapshot of every price list read page by page in id order, and
`price_list_prices`, a snapshot of every price on every price list.
"""

from datetime import UTC, datetime
from logging import Logger
from unittest.mock import MagicMock

import pytest

import source_shopify_native.graphql as gql
from source_shopify_native.api import fetch_snapshot, snapshot_price_list_prices
from source_shopify_native.models import StoreCapabilities, create_response_data_model

NOW = datetime(2026, 11, 13, tzinfo=UTC)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


class _MockClient:
    """Returns queued pages in order and records each query."""

    def __init__(self, pages: list[dict]):
        self._pages = list(pages)
        self.queries: list[str] = []

    async def request(self, query, data_model, log, context=None):
        self.queries.append(query)
        return data_model.model_validate(self._pages.pop(0), context=context)


def _price_list(list_id: int, catalog: dict | None) -> dict:
    return {
        "id": f"gid://shopify/PriceList/{list_id}",
        "name": f"List {list_id}",
        "currency": "MXN",
        "fixedPricesCount": 1,
        "parent": {
            "adjustment": {"type": "PERCENTAGE_DECREASE", "value": 5},
            "settings": {"compareAtMode": "ADJUSTED"},
        },
        "catalog": catalog,
    }


def _page(nodes: list[dict], end_cursor: str | None = None) -> dict:
    return {
        "priceLists": {
            "edges": [{"node": n} for n in nodes],
            "pageInfo": {"hasNextPage": end_cursor is not None, "endCursor": end_cursor},
        }
    }


def test_price_lists_query_is_unfiltered_in_id_order():
    query = gql.PriceLists.build_query(NOW, NOW, first=250, after="c1")

    assert "priceLists(" in query
    assert "sortKey: ID" in query
    assert "first: 250" in query
    assert 'after: "c1"' in query
    assert "query:" not in query
    assert "updated_at" not in query


@pytest.mark.asyncio
async def test_price_lists_snapshot_reads_every_page(log):
    market_catalog = {
        "__typename": "MarketCatalog",
        "id": "gid://shopify/MarketCatalog/1",
        "title": "Mexico",
        "status": "ACTIVE",
    }
    client = _MockClient(
        [
            _page([_price_list(1, market_catalog)], end_cursor="c1"),
            # A price list can have no catalog (Bruno `Pricing / price lists`).
            _page([_price_list(2, None)]),
        ]
    )

    docs = [
        doc
        async for doc in fetch_snapshot(
            client,
            gql.PriceLists,
            create_response_data_model(gql.PriceLists),
            "store-a",
            StoreCapabilities(scopes=frozenset()),
            log,
        )
    ]

    assert [d.id for d in docs] == [
        "gid://shopify/PriceList/1",
        "gid://shopify/PriceList/2",
    ]
    dumped = [d.model_dump(by_alias=True, exclude_unset=True) for d in docs]
    assert dumped[0]["catalog"] == market_catalog
    assert dumped[1]["catalog"] is None
    assert all(d["_meta"]["store"] == "store-a" for d in dumped)
    assert 'after: "c1"' in client.queries[1]


def _money(amount: str, currency: str) -> dict:
    return {"amount": amount, "currencyCode": currency}


def _price(variant_id: int, origin_type: str, price: dict, compare_at: dict | None) -> dict:
    return {
        "originType": origin_type,
        "price": price,
        "compareAtPrice": compare_at,
        "variant": {"id": f"gid://shopify/ProductVariant/{variant_id}"},
    }


def _prices_page(nodes: list[dict], end_cursor: str | None = None) -> dict:
    return {
        "priceList": {
            "prices": {
                "edges": [{"node": n} for n in nodes],
                "pageInfo": {"hasNextPage": end_cursor is not None, "endCursor": end_cursor},
            }
        }
    }


def test_price_list_prices_query_pages_one_price_list():
    query = gql.PriceListPrices.build_query_for_price_list(
        "gid://shopify/PriceList/1", 250, after="p1"
    )

    assert 'priceList(id: "gid://shopify/PriceList/1")' in query
    assert "first: 250" in query
    assert 'after: "p1"' in query
    assert "originType" in query


@pytest.mark.asyncio
async def test_price_list_prices_reads_every_price_of_every_list(log):
    fixed = _price(7, "FIXED", _money("11000.0", "MXN"), _money("15000.0", "MXN"))
    # RELATIVE prices stay in the store's currency (Bruno `Pricing / price list prices`).
    relative = _price(9, "RELATIVE", _money("9.5", "USD"), None)
    client = _MockClient(
        [
            _page([_price_list(1, None), _price_list(2, None), _price_list(3, None)]),
            _prices_page([fixed], end_cursor="p1"),
            _prices_page([relative]),
            # Price list 2 was deleted after it was listed.
            {"priceList": None},
            _prices_page([relative]),
        ]
    )

    docs = [
        doc
        async for doc in snapshot_price_list_prices(
            client,
            create_response_data_model(gql.PriceListPrices),
            "store-a",
            StoreCapabilities(scopes=frozenset()),
            log,
        )
    ]
    dumped = [d.model_dump(by_alias=True, exclude_unset=True) for d in docs]

    assert [(d["priceList"]["id"], d["variant"]["id"]) for d in dumped] == [
        ("gid://shopify/PriceList/1", "gid://shopify/ProductVariant/7"),
        ("gid://shopify/PriceList/1", "gid://shopify/ProductVariant/9"),
        ("gid://shopify/PriceList/3", "gid://shopify/ProductVariant/9"),
    ]
    assert dumped[0] == {
        "_meta": {"store": "store-a"},
        "priceList": {"id": "gid://shopify/PriceList/1"},
        **fixed,
    }
    assert 'priceList(id: "gid://shopify/PriceList/1")' in client.queries[1]
    assert 'after: "p1"' in client.queries[2]
    assert 'priceList(id: "gid://shopify/PriceList/3")' in client.queries[4]


@pytest.mark.asyncio
async def test_price_list_prices_without_price_lists_is_empty(log):
    client = _MockClient([_page([])])

    docs = [
        doc
        async for doc in snapshot_price_list_prices(
            client,
            create_response_data_model(gql.PriceListPrices),
            "store-a",
            StoreCapabilities(scopes=frozenset()),
            log,
        )
    ]

    assert docs == []
    assert len(client.queries) == 1
