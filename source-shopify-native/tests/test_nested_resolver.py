"""
Tests for the inline-first nested-connection resolver. Each parent carries its connection's first
page inline from the outer query. The resolver flattens that to a clean list and only issues
`node(id:)` re-queries to drain overflow when the inline page reports `hasNextPage`.
"""

import asyncio
import re
from logging import Logger
from unittest.mock import MagicMock

import pytest

from source_shopify_native.graphql.nested_resolver import (
    _NodeResponse,
    drain_connection,
    resolve_document,
)
from source_shopify_native.models import NestedConnection

CONNECTION = NestedConnection(
    parent_path=["refunds"],
    parent_typename="Refund",
    field_name="refundLineItems",
    node_selection="id quantity",
    page_size=2,
)


def _inline(nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> dict:
    """The inline `{edges, pageInfo}` shape a parent carries from the outer query."""
    return {
        "edges": [{"node": n} for n in nodes],
        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
    }


def _node_page(nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> _NodeResponse:
    """A `node(id:){ ...connection... }` overflow re-query response."""
    return _NodeResponse(
        node={CONNECTION.field_name: _inline(nodes, has_next, end_cursor)}
    )


class _MockClient:
    """Returns queued overflow pages keyed by the parent id parsed out of each `node(id:)` query.

    Keying by parent id (not call order) keeps results correct under the resolver's concurrent
    gather across parents.
    """

    def __init__(self, pages_by_parent: dict[str, list[_NodeResponse]]):
        self._pages = {pid: list(pages) for pid, pages in pages_by_parent.items()}
        self.queries: list[str] = []

    async def request(self, query, data_model, log, context=None):
        self.queries.append(query)
        match = re.search(r'node\(id: "([^"]+)"', query)
        assert match, query
        return self._pages[match.group(1)].pop(0)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


# --- drain_connection: the overflow pager ---------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_seeds_after_cursor(log):
    client = _MockClient({"gid://shopify/Refund/1": [_node_page([{"id": "c"}])]})

    nodes = await drain_connection(client, "gid://shopify/Refund/1", CONNECTION, log, after="CUR0")

    assert nodes == [{"id": "c"}]
    assert 'after: "CUR0"' in client.queries[0]


@pytest.mark.asyncio
async def test_drain_paginates_until_exhausted(log):
    client = _MockClient(
        {
            "gid://shopify/Refund/1": [
                _node_page([{"id": "c"}], has_next=True, end_cursor="CUR1"),
                _node_page([{"id": "d"}], has_next=False),
            ]
        }
    )

    nodes = await drain_connection(client, "gid://shopify/Refund/1", CONNECTION, log, after="CUR0")

    assert nodes == [{"id": "c"}, {"id": "d"}]
    assert len(client.queries) == 2
    assert 'after: "CUR1"' in client.queries[1]


@pytest.mark.asyncio
async def test_drain_stops_when_has_next_but_no_cursor(log):
    """A truthy hasNextPage with a null endCursor must terminate, not loop forever."""
    client = _MockClient(
        {"gid://shopify/Refund/1": [_node_page([{"id": "c"}], has_next=True, end_cursor=None)]}
    )

    nodes = await drain_connection(client, "gid://shopify/Refund/1", CONNECTION, log, after="CUR0")

    assert nodes == [{"id": "c"}]
    assert len(client.queries) == 1


@pytest.mark.asyncio
async def test_drain_missing_node_returns_empty(log):
    client = _MockClient({"gid://shopify/Refund/1": [_NodeResponse(node=None)]})

    nodes = await drain_connection(client, "gid://shopify/Refund/1", CONNECTION, log, after="CUR0")

    assert nodes == []
    log.warning.assert_called_once()


@pytest.mark.asyncio
async def test_drain_missing_connection_field_raises(log):
    """A node present but lacking the queried connection field is a wrong-shape breach, not 'no data'."""
    client = _MockClient({"gid://shopify/Refund/1": [_NodeResponse(node={})]})

    with pytest.raises(TypeError):
        await drain_connection(client, "gid://shopify/Refund/1", CONNECTION, log, after="CUR0")


@pytest.mark.asyncio
async def test_overflow_page_size_sets_drain_first(log):
    """drain_connection uses overflow_page_size for its `first:`, independent of the inline page_size."""
    connection = NestedConnection(
        parent_path=["refunds"],
        parent_typename="Refund",
        field_name="refundLineItems",
        node_selection="id",
        page_size=2,
        overflow_page_size=50,
    )
    client = _MockClient({"gid://shopify/Refund/1": [_node_page([{"id": "a"}])]})

    await drain_connection(client, "gid://shopify/Refund/1", connection, log, after="CUR0")

    assert "first: 50" in client.queries[0]
    assert "first: 2" not in client.queries[0]


# --- resolve_document: inline-first stitching -----------------------------------------------------


@pytest.mark.asyncio
async def test_inline_only_makes_no_request(log):
    """A parent whose inline page is complete is flattened with zero extra requests."""
    document = {
        "id": "gid://shopify/Order/9",
        "refunds": [{"id": "gid://shopify/Refund/1", "refundLineItems": _inline([{"id": "a"}, {"id": "b"}])}],
    }
    client = _MockClient({})

    await resolve_document(client, document, [CONNECTION], log, asyncio.Semaphore(5))

    assert document["refunds"][0]["refundLineItems"] == [{"id": "a"}, {"id": "b"}]
    assert client.queries == []


@pytest.mark.asyncio
async def test_overflow_drains_remainder(log):
    """When the inline page reports hasNextPage, the rest is drained and appended."""
    document = {
        "id": "gid://shopify/Order/9",
        "refunds": [
            {"id": "gid://shopify/Refund/1", "refundLineItems": _inline([{"id": "a"}], has_next=True, end_cursor="CUR1")},
        ],
    }
    client = _MockClient({"gid://shopify/Refund/1": [_node_page([{"id": "b"}, {"id": "c"}])]})

    await resolve_document(client, document, [CONNECTION], log, asyncio.Semaphore(5))

    assert document["refunds"][0]["refundLineItems"] == [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    assert 'after: "CUR1"' in client.queries[0]


@pytest.mark.asyncio
async def test_mixed_parents_only_overflow_requeries(log):
    document = {
        "id": "gid://shopify/Order/9",
        "refunds": [
            {"id": "gid://shopify/Refund/1", "refundLineItems": _inline([{"id": "a"}])},
            {"id": "gid://shopify/Refund/2", "refundLineItems": _inline([{"id": "b"}], has_next=True, end_cursor="C")},
        ],
    }
    client = _MockClient({"gid://shopify/Refund/2": [_node_page([{"id": "c"}])]})

    await resolve_document(client, document, [CONNECTION], log, asyncio.Semaphore(5))

    assert document["refunds"][0]["refundLineItems"] == [{"id": "a"}]
    assert document["refunds"][1]["refundLineItems"] == [{"id": "b"}, {"id": "c"}]
    # Only the overflowing parent triggered a request.
    assert len(client.queries) == 1


@pytest.mark.asyncio
async def test_overflow_missing_id_raises(log):
    """hasNextPage but no parent id can't happen with valid data (id is always selected), so it raises."""
    document = {
        "id": "gid://shopify/Order/9",
        "refunds": [{"refundLineItems": _inline([{"id": "a"}], has_next=True, end_cursor="C")}],
    }

    with pytest.raises(ValueError):
        await resolve_document(_MockClient({}), document, [CONNECTION], log, asyncio.Semaphore(5))


@pytest.mark.asyncio
async def test_empty_inline_yields_empty_list(log):
    document = {"id": "gid://shopify/Order/9", "refunds": [{"id": "gid://shopify/Refund/1", "refundLineItems": _inline([])}]}
    client = _MockClient({})

    await resolve_document(client, document, [CONNECTION], log, asyncio.Semaphore(5))

    assert document["refunds"][0]["refundLineItems"] == []
    assert client.queries == []


@pytest.mark.asyncio
async def test_no_parents_makes_no_requests(log):
    document = {"id": "gid://shopify/Order/9", "refunds": []}
    client = _MockClient({})

    await resolve_document(client, document, [CONNECTION], log, asyncio.Semaphore(5))

    assert document["refunds"] == []
    assert client.queries == []


# --- shape enforcement: empty list is a no-op, absent/wrong shape raises -------------------------


@pytest.mark.asyncio
async def test_absent_parent_field_raises(log):
    """The parent field is always selected and non-null, so an absent field signals a bug."""
    document = {"id": "gid://shopify/Order/9"}

    with pytest.raises(TypeError):
        await resolve_document(_MockClient({}), document, [CONNECTION], log, asyncio.Semaphore(5))


@pytest.mark.asyncio
async def test_scalar_parent_raises(log):
    """A path landing on a scalar (not an object) can't be a parent, so the navigator raises."""
    document = {"id": "gid://shopify/Order/9", "refunds": "not-an-object"}

    with pytest.raises(TypeError):
        await resolve_document(_MockClient({}), document, [CONNECTION], log, asyncio.Semaphore(5))


@pytest.mark.asyncio
async def test_parent_missing_connection_raises(log):
    """A parent lacking the inlined connection we always query signals a bug, so it raises."""
    document = {"id": "gid://shopify/Order/9", "refunds": [{"id": "gid://shopify/Refund/1"}]}

    with pytest.raises(TypeError):
        await resolve_document(_MockClient({}), document, [CONNECTION], log, asyncio.Semaphore(5))


# --- parent_path: empty / multi-step / connection-in-connection ----------------------------------


@pytest.mark.asyncio
async def test_empty_path_resolves_connection_on_node(log):
    """An empty path treats the document node itself as the parent (a connection directly on it)."""
    connection = NestedConnection(
        parent_path=[],
        parent_typename="Order",
        field_name="agreements",
        node_selection="id",
        page_size=2,
    )
    document = {"id": "gid://shopify/Order/9", "agreements": _inline([{"id": "a"}, {"id": "b"}])}

    await resolve_document(_MockClient({}), document, [connection], log, asyncio.Semaphore(5))

    assert document["agreements"] == [{"id": "a"}, {"id": "b"}]


@pytest.mark.asyncio
async def test_empty_path_drains_via_node_id(log):
    """An overflowing connection on the node drains via node(id: <the node's own id>)."""
    connection = NestedConnection(
        parent_path=[],
        parent_typename="Order",
        field_name="agreements",
        node_selection="id",
        page_size=2,
    )
    document = {
        "id": "gid://shopify/Order/9",
        "agreements": _inline([{"id": "a"}], has_next=True, end_cursor="C"),
    }
    client = _MockClient(
        {"gid://shopify/Order/9": [_NodeResponse(node={"agreements": _inline([{"id": "b"}])})]}
    )

    await resolve_document(client, document, [connection], log, asyncio.Semaphore(5))

    assert document["agreements"] == [{"id": "a"}, {"id": "b"}]
    assert 'node(id: "gid://shopify/Order/9"' in client.queries[0]


@pytest.mark.asyncio
async def test_multi_step_path_fans_out(log):
    """A multi-step path descends an object, then fans out over a nested list of parents."""
    connection = NestedConnection(
        parent_path=["wrapper", "refunds"],
        parent_typename="Refund",
        field_name="refundLineItems",
        node_selection="id",
        page_size=2,
    )
    document = {
        "id": "gid://shopify/Order/9",
        "wrapper": {
            "refunds": [
                {"id": "gid://shopify/Refund/1", "refundLineItems": _inline([{"id": "a"}])},
                {"id": "gid://shopify/Refund/2", "refundLineItems": _inline([{"id": "b"}])},
            ]
        },
    }

    await resolve_document(_MockClient({}), document, [connection], log, asyncio.Semaphore(5))

    assert document["wrapper"]["refunds"][0]["refundLineItems"] == [{"id": "a"}]
    assert document["wrapper"]["refunds"][1]["refundLineItems"] == [{"id": "b"}]


@pytest.mark.asyncio
async def test_connection_in_connection_resolves_in_order(log):
    """Resolving the outer connection flattens it, so the inner connection's path can navigate it."""
    agreements = NestedConnection(
        parent_path=[],
        parent_typename="Order",
        field_name="agreements",
        node_selection="id",
        page_size=2,
    )
    sales = NestedConnection(
        parent_path=["agreements"],
        parent_typename="SalesAgreement",
        field_name="sales",
        node_selection="id",
        page_size=2,
    )
    document = {
        "id": "gid://shopify/Order/9",
        "agreements": _inline(
            [{"id": "gid://shopify/SalesAgreement/1", "sales": _inline([{"id": "s1"}])}]
        ),
    }

    await resolve_document(_MockClient({}), document, [agreements, sales], log, asyncio.Semaphore(5))

    # Outer flattened to a list, and the inner connection on each agreement flattened too.
    assert document["agreements"][0]["id"] == "gid://shopify/SalesAgreement/1"
    assert document["agreements"][0]["sales"] == [{"id": "s1"}]


@pytest.mark.asyncio
async def test_descend_into_non_object_raises(log):
    """A path that tries to descend through a non-object raises rather than resolve nothing."""
    connection = NestedConnection(
        parent_path=["wrapper", "refunds"],
        parent_typename="Refund",
        field_name="refundLineItems",
        node_selection="id",
        page_size=2,
    )
    document = {"id": "gid://shopify/Order/9", "wrapper": "not-an-object"}

    with pytest.raises(TypeError):
        await resolve_document(_MockClient({}), document, [connection], log, asyncio.Semaphore(5))
