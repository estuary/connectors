"""Resolver for GraphQL connections nested inside a parent document.

Shopify exposes some connections only beneath a list field (e.g. a Refund's `refundLineItems`
lives under `Order.refunds`, a list). Bulk operations reject a connection nested in a list, so
these must be captured with the standard, non-bulk API. Ideally, most parents have few children
and are fully captured inline with zero extra requests.

Only when a parent's inline page reports `hasNextPage` do we fall back to `drain_connection`,
which pages the remainder with focused `node(id: ...)` re-queries seeded from the inline cursor.
This keeps the common case at one request per outer page while still capturing parents with large
connections without truncation.

Resolved nodes are stitched into the parent document as a plain list under the connection's field
name, with all pagination metadata (`edges`/`pageInfo`/cursors) stripped.
"""

import asyncio
from logging import Logger
from typing import Any

from pydantic import BaseModel

from ..models import NestedConnection, PageInfo
from .client import ShopifyGraphQLClient

# Bound on concurrent nested-connection re-queries in flight at once. Keeps the connector from
# flooding the API and its cost budget while still overlapping requests across a fetched page.
NESTED_CONNECTION_CONCURRENCY = 5


class _NodeResponse(BaseModel, extra="allow"):
    """Data model for a `{ node(id: ...) { ... } }` response.

    `node` is kept as a raw dict because the connection field name is dynamic. The resolved node
    dicts are the payloads we stitch into the document.
    """

    node: dict[str, Any] | None = None


def _build_node_query(parent_id: str, connection: NestedConnection, after: str | None) -> str:
    after_arg = f', after: "{after}"' if after else ""
    first = connection.overflow_page_size or connection.page_size
    query = f"""
    {{
        node(id: "{parent_id}") {{
            ... on {connection.parent_typename} {{
                {connection.field_name}(first: {first}{after_arg}) {{
                    edges {{
                        node {{
                            {connection.node_selection}
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                }}
            }}
        }}
    }}
    """

    for fragment in connection.fragments:
        query += fragment

    return query


async def drain_connection(
    client: ShopifyGraphQLClient,
    parent_id: str,
    connection: NestedConnection,
    log: Logger,
    after: str | None = None,
) -> list[dict[str, Any]]:
    """Page the remainder of a parent's connection from `after`, returning the node dicts.

    Called only for overflow when the inline first page reported `hasNextPage`, seeded with the inline
    page's end cursor.
    """
    nodes: list[dict[str, Any]] = []

    while True:
        query = _build_node_query(parent_id, connection, after)
        data = await client.request(query, _NodeResponse, log)

        if data.node is None:
            # The parent disappeared between the outer query and this re-query (e.g. deleted).
            # Treat as no nested data rather than failing the whole fetch.
            log.warning(
                "Parent node not found while resolving nested connection.",
                {"parent_id": parent_id, "field": connection.field_name},
            )
            break

        # `node` is present, so the connection we queried under it must be too.
        page = data.node.get(connection.field_name)
        if not isinstance(page, dict):
            raise TypeError(
                f"Parent {parent_id!r} is missing its {connection.field_name!r} connection "
                f"(expected an object, got {type(page).__name__})."
            )
        nodes.extend(edge["node"] for edge in page.get("edges", []))

        page_info = PageInfo.model_validate(page.get("pageInfo") or {})
        if not page_info.hasNextPage or not page_info.endCursor:
            break

        after = page_info.endCursor

    return nodes


def _navigate_to_parents(document: dict[str, Any], parent_path: list[str]) -> list[dict[str, Any]]:
    """Resolve `parent_path` from `document` to the parent object(s) containing the connection.

    An empty path yields the document node itself. Otherwise each step descends one field, fanning
    out over any list it lands on. The fields/lists along the path should always be selected in the outer
    query, so an absent/scalar value where the path keeps going, or a non-object parent,
    can't arise from valid data - it's a query-building bug or an API contract breach, and we raise
    rather than silently resolve nothing.
    """
    current: list[Any] = [document]
    for step in parent_path:
        next: list[Any] = []
        for obj in current:
            if not isinstance(obj, dict):
                raise TypeError(
                    f"Cannot descend {step!r} while navigating {parent_path}: expected an object, "
                    f"got {type(obj).__name__}."
                )
            value = obj.get(step)
            next.extend(value) if isinstance(value, list) else next.append(value)
        current = next

    for parent in current:
        if not isinstance(parent, dict):
            raise TypeError(
                f"Nested-connection parent at path {parent_path} must be an object, "
                f"got {type(parent).__name__}."
            )

    return current


async def _resolve_parent(
    client: ShopifyGraphQLClient,
    parent: dict[str, Any],
    connection: NestedConnection,
    log: Logger,
    sem: asyncio.Semaphore,
) -> None:
    """Replace one parent's inline connection with a flat list of its nodes, draining any overflow.

    The parent already carries the connection's first page inline (`{edges, pageInfo}`). We flatten
    those edges and, only if the page reports more, append the remainder via `drain_connection`.
    The result is written back under the connection's field name, so the parent
    ends up with a plain list in place of the `{edges, pageInfo}` wrapper.
    """
    # The connection is always selected inline in the outer query, so a parent that lacks it
    # or carries a non-object is a query bug or API contract violation.
    inline = parent.get(connection.field_name)
    if not isinstance(inline, dict):
        raise TypeError(
            f"Parent {parent.get('id')!r} is missing its inlined {connection.field_name!r} "
            f"connection (expected an object, got {type(inline).__name__})."
        )

    nodes = [edge["node"] for edge in inline.get("edges", [])]
    page_info = PageInfo.model_validate(inline.get("pageInfo") or {})

    if page_info.hasNextPage and page_info.endCursor:
        # Draining needs the parent's id for the `node(id:)` re-query. `id` is always selected
        # and non-null, so a parent that overflows without one is a query bug or API contract violation.
        parent_id = parent.get("id")
        if not parent_id:
            raise ValueError(
                f"Parent for {connection.field_name!r} ({connection.parent_typename}) reports "
                f"more pages but has no id to fetch them."
            )

        async with sem:
            nodes.extend(
                await drain_connection(
                    client, parent_id, connection, log, after=page_info.endCursor
                )
            )

    parent[connection.field_name] = nodes


async def resolve_document(
    client: ShopifyGraphQLClient,
    document: dict[str, Any],
    connections: list[NestedConnection],
    log: Logger,
    sem: asyncio.Semaphore,
) -> None:
    """Resolve every connection in `document` in place, draining overflow under `sem`.

    Connections resolve in declaration order - resolving one flattens it to a plain list, so a
    connection nested inside another can navigate to it. Within a connection, its parents resolve
    concurrently. The ordering only serializes across connections, not within one.
    """
    for connection in connections:
        parents = _navigate_to_parents(document, connection.parent_path)
        await asyncio.gather(
            *[_resolve_parent(client, parent, connection, log, sem) for parent in parents]
        )
