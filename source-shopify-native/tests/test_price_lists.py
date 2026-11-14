"""
Tests for `price_lists`: a snapshot of every price list, read page by page in id order.
"""

from datetime import UTC, datetime
from logging import Logger
from unittest.mock import MagicMock

import pytest

import source_shopify_native.graphql as gql
from source_shopify_native.api import fetch_snapshot
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
            # A price list can have no catalog.
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
