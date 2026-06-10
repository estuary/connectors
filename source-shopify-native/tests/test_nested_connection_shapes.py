"""End-to-end tests for every connection shape `NestedConnection` supports.

These drive the `ShopifyGraphQLClient` and the `_paginate_through_resources` flow
(query build -> HTTP POST -> response validation -> inline stitch -> overflow drain). Only the HTTP
transport is mocked, so each test shows the actual `{"data": ...}` envelope Shopify would return for
a given connection shape and verifies the document the connector emits after stitching.

Supported shapes exercised here (declared via `NestedConnection.parent_path`):
  - `["refunds"]`            connection on each element of a top-level list   (OrderRefunds, real)
  - `[]`                     connection directly on the document node
  - `["wrapper", "things"]`  connection on each element of a list nested under an object
  - `["agreements"]`         connection nested inside another connection (resolved in order)
"""

import json
import re
from datetime import UTC, datetime
from logging import Logger
from unittest.mock import MagicMock

import pytest

from source_shopify_native.api import _paginate_through_resources
from source_shopify_native.graphql.client import ShopifyGraphQLClient
from source_shopify_native.graphql.orders.refunds import OrderRefunds
from source_shopify_native.models import (
    NestedConnection,
    ShopifyGraphQLResource,
    SortKey,
    StoreCapabilities,
    create_response_data_model,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 2, 1, tzinfo=UTC)
STORE = "teststore"
CAPS = StoreCapabilities(scopes=frozenset())


class MockHTTP:
    """Stands in for estuary_cdk's HTTPSession, returning canned GraphQL JSON bodies.

    Routes by query shape: a `node(id: "<id>")` overflow re-query returns the next queued page for
    that parent id; any other query (an outer connection page) returns the next queued outer page.
    Bodies are the full `{"data": ...}` envelope Shopify returns, so the real ShopifyGraphQLClient
    parses, validates, and stitches them exactly as it would in production.
    """

    def __init__(
        self,
        outer_pages: list[dict],
        node_pages: dict[str, list[dict]] | None = None,
    ):
        self._outer = list(outer_pages)
        self._node = {pid: list(pages) for pid, pages in (node_pages or {}).items()}
        self.queries: list[str] = []

    async def request(self, log, url, method="GET", **kwargs) -> bytes:
        query = (kwargs.get("json") or {}).get("query", "")
        self.queries.append(query)
        match = re.search(r'node\(id: "([^"]+)"', query)
        page = self._node[match.group(1)].pop(0) if match else self._outer.pop(0)
        return json.dumps({"data": page}).encode()


def _conn(nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> dict:
    """The `{edges, pageInfo}` shape a connection takes in a response (inline or in a node re-query)."""
    return {
        "edges": [{"node": n} for n in nodes],
        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
    }


def _outer_page(query_root: str, nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> dict:
    """An outer-query response body: `{ <query_root>: { edges, pageInfo } }`."""
    return {
        query_root: {
            "edges": [{"node": n} for n in nodes],
            "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
        }
    }


def _node_page(field: str, nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> dict:
    """A `node(id:){ ... }` overflow response body: `{ node: { <field>: { edges, pageInfo } } }`."""
    return {"node": {field: _conn(nodes, has_next, end_cursor)}}


async def _capture(model: type[ShopifyGraphQLResource], http: MockHTTP, log: Logger) -> list:
    """Run a single non-bulk fetch through the real client + paginate flow, collecting documents."""
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]
    data_model = create_response_data_model(model)
    return [
        doc
        async for doc in _paginate_through_resources(
            client, model, data_model, START, END, STORE, CAPS, log
        )
    ]


# Test-only models for the shapes that no shipping stream uses yet. Each is a real
# ShopifyGraphQLResource subclass, so it goes through class-definition validation and the actual
# query builder.


class _NodeConnectionModel(ShopifyGraphQLResource):
    """A connection directly on the node (`parent_path=[]`)."""

    NAME = "node_conn"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUERY = "# {{ agreements }}"
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=[],
            parent_typename="Order",
            field_name="agreements",
            node_selection="id",
            page_size=2,
        ),
    ]

    @staticmethod
    def build_query(start, end, first=None, after=None, capabilities=None):
        return _NodeConnectionModel.build_query_with_fragment(
            start, end, first=first, after=after, capabilities=capabilities
        )


class _NestedListModel(ShopifyGraphQLResource):
    """A connection on each element of a list nested under an object (`parent_path=["wrapper", "things"]`)."""

    NAME = "nested_list"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUERY = "wrapper { things { id # {{ widgets }} } }"
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=["wrapper", "things"],
            parent_typename="Thing",
            field_name="widgets",
            node_selection="id",
            page_size=2,
        ),
    ]

    @staticmethod
    def build_query(start, end, first=None, after=None, capabilities=None):
        return _NestedListModel.build_query_with_fragment(
            start, end, first=first, after=after, capabilities=capabilities
        )


class _ConnectionInConnectionModel(ShopifyGraphQLResource):
    """A connection nested inside another connection; the outer must be declared first."""

    NAME = "conn_in_conn"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUERY = "# {{ agreements }}"
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=[],
            parent_typename="Order",
            field_name="agreements",
            node_selection="id\n# {{ sales }}",
            page_size=2,
        ),
        NestedConnection(
            parent_path=["agreements"],
            parent_typename="SalesAgreement",
            field_name="sales",
            node_selection="id",
            page_size=2,
        ),
    ]

    @staticmethod
    def build_query(start, end, first=None, after=None, capabilities=None):
        return _ConnectionInConnectionModel.build_query_with_fragment(
            start, end, first=first, after=after, capabilities=capabilities
        )


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


# --- ["refunds"]: connection on each element of a top-level list (real stream) --------------------


@pytest.mark.asyncio
async def test_top_level_list_inline(log):
    """A refund whose inline page is complete is flattened with no extra request."""
    order = {
        "id": "gid://shopify/Order/1",
        "refunds": [
            {"id": "gid://shopify/Refund/1", "refundLineItems": _conn([{"id": "a"}, {"id": "b"}])},
        ],
    }
    http = MockHTTP(outer_pages=[_outer_page("orders", [order])])

    docs = await _capture(OrderRefunds, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["refunds"][0]["refundLineItems"] == [{"id": "a"}, {"id": "b"}]
    assert len(http.queries) == 1  # outer query only


@pytest.mark.asyncio
async def test_top_level_list_overflow_drains(log):
    """A refund whose inline page reports hasNextPage drains the rest via node(id: <refund>)."""
    order = {
        "id": "gid://shopify/Order/1",
        "refunds": [
            {
                "id": "gid://shopify/Refund/1",
                "refundLineItems": _conn([{"id": "a"}], has_next=True, end_cursor="C1"),
            },
        ],
    }
    http = MockHTTP(
        outer_pages=[_outer_page("orders", [order])],
        node_pages={"gid://shopify/Refund/1": [_node_page("refundLineItems", [{"id": "b"}])]},
    )

    docs = await _capture(OrderRefunds, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["refunds"][0]["refundLineItems"] == [{"id": "a"}, {"id": "b"}]
    assert any('node(id: "gid://shopify/Refund/1"' in q for q in http.queries)


# --- []: connection directly on the node ----------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_on_node_inline(log):
    """A connection on the node itself is flattened in place (the node is the parent)."""
    order = {"id": "gid://shopify/Order/1", "agreements": _conn([{"id": "ag1"}, {"id": "ag2"}])}
    http = MockHTTP(outer_pages=[_outer_page("orders", [order])])

    docs = await _capture(_NodeConnectionModel, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["agreements"] == [{"id": "ag1"}, {"id": "ag2"}]
    assert len(http.queries) == 1


@pytest.mark.asyncio
async def test_connection_on_node_overflow_drains(log):
    """An overflowing connection on the node drains via node(id: <the node's own id>)."""
    order = {
        "id": "gid://shopify/Order/1",
        "agreements": _conn([{"id": "ag1"}], has_next=True, end_cursor="C1"),
    }
    http = MockHTTP(
        outer_pages=[_outer_page("orders", [order])],
        node_pages={"gid://shopify/Order/1": [_node_page("agreements", [{"id": "ag2"}])]},
    )

    docs = await _capture(_NodeConnectionModel, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["agreements"] == [{"id": "ag1"}, {"id": "ag2"}]
    assert any('node(id: "gid://shopify/Order/1"' in q for q in http.queries)


# --- ["wrapper", "things"]: connection on a list nested under an object ----------------------------


@pytest.mark.asyncio
async def test_nested_list_path_fans_out(log):
    """A multi-step path descends an object, then resolves the connection on each list element."""
    order = {
        "id": "gid://shopify/Order/1",
        "wrapper": {
            "things": [
                {"id": "t1", "widgets": _conn([{"id": "w1"}])},
                {"id": "t2", "widgets": _conn([{"id": "w2"}])},
            ]
        },
    }
    http = MockHTTP(outer_pages=[_outer_page("orders", [order])])

    docs = await _capture(_NestedListModel, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["wrapper"]["things"][0]["widgets"] == [{"id": "w1"}]
    assert dumped["wrapper"]["things"][1]["widgets"] == [{"id": "w2"}]
    assert len(http.queries) == 1


# --- ["agreements"]: connection nested inside another connection ----------------------------------


@pytest.mark.asyncio
async def test_connection_in_connection_resolves_both(log):
    """The outer connection is flattened first, so the inner one can be resolved on each parent."""
    order = {
        "id": "gid://shopify/Order/1",
        "agreements": _conn(
            [{"id": "gid://shopify/SalesAgreement/1", "sales": _conn([{"id": "s1"}, {"id": "s2"}])}]
        ),
    }
    http = MockHTTP(outer_pages=[_outer_page("orders", [order])])

    docs = await _capture(_ConnectionInConnectionModel, http, log)

    dumped = docs[0].model_dump(by_alias=True)
    assert dumped["agreements"][0]["id"] == "gid://shopify/SalesAgreement/1"
    assert dumped["agreements"][0]["sales"] == [{"id": "s1"}, {"id": "s2"}]
    assert len(http.queries) == 1  # everything inline, no overflow
