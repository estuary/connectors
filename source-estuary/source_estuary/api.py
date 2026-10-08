import json
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPSession

from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    AuthorizedPrefix,
    CapabilityBit,
    CatalogStats,
    EndpointConfig,
    EstuarySnapshot,
    Grain,
    LiveSpecRefDocument,
    Pager,
    PublicationHistoryItem,
    Scope,
)

API_BASE_URL = "https://api.estuary.dev"

GRAPHQL_PATH = "/api/graphql"
GRAPHQL_URL = f"{API_BASE_URL}{GRAPHQL_PATH}"

# The API enforces no maximum page size; this bounds response sizes.
PAGE_SIZE = 100


class GraphQLError(RuntimeError):
    pass


async def graphql_request(
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """POST a query and return its `data`. GraphQL failures arrive as HTTP 200 with an `errors` array.

    A provisional authorization failure is an HTTP 307 back to /api/graphql;
    aiohttp follows it with the same method, body and Authorization header.
    """
    body = json.loads(
        await http.request(
            log,
            GRAPHQL_URL,
            method="POST",
            json={"query": query, "variables": variables or {}},
        )
    )
    if errors := body.get("errors"):
        raise GraphQLError(f"Estuary API returned errors: {[e.get('message') for e in errors]}")
    return body["data"]


AUTHORIZED_PREFIXES_QUERY = """
query AuthorizedPrefixes {
  prefixes(by: { minCapability: read }) {
    edges { node { prefix capabilities } }
  }
}
"""


async def fetch_authorized_prefixes(http: HTTPSession, log: Logger) -> list[AuthorizedPrefix]:
    # `prefixes` returns every row when `first` is omitted.
    data = await graphql_request(http, log, AUTHORIZED_PREFIXES_QUERY)
    return [AuthorizedPrefix.model_validate(edge["node"]) for edge in data["prefixes"]["edges"]]


def _prune_nested(prefixes: set[str]) -> list[str]:
    pruned: list[str] = []
    for p in sorted(prefixes):
        if not pruned or not p.startswith(pruned[-1]):
            pruned.append(p)
    return pruned


async def resolve_scopes(
    http: HTTPSession,
    log: Logger,
    config: EndpointConfig,
    stream: str,
    required: frozenset[CapabilityBit],
) -> list[str]:
    """Return the disjoint prefixes to query: the configured prefixes (or every
    authorized prefix) where the API key holds every `required` capability.

    Scopes are planned rather than probed because an unauthorized query fails
    as a whole, and skipping a scope mid-sweep would tombstone its rows.
    """
    authorized = await fetch_authorized_prefixes(http, log)
    holders = [a.prefix for a in authorized if required <= set(a.capabilities)]

    if not config.prefixes:
        return _prune_nested(set(holders))

    candidates: set[str] = set()
    for c in config.prefixes:
        matched = False
        for h in holders:
            if c.startswith(h):
                candidates.add(c)
                matched = True
            elif h.startswith(c):
                candidates.add(h)
                matched = True
        if not matched:
            log.info(f"{stream}: skipping {c}, the API key lacks the required capabilities there")

    return _prune_nested(candidates)


async def _walk(
    cls: type[EstuarySnapshot],
    http: HTTPSession,
    log: Logger,
    variables: dict[str, Any],
) -> AsyncGenerator[dict[str, Any], None]:
    cursor: str | None = None
    while True:
        page_vars = dict(variables)
        match cls.pager:
            case Pager.FORWARD:
                page_vars |= {"first": PAGE_SIZE, "after": cursor}
            case Pager.BACKWARD:
                # `first` is ignored by `alerts` unless `after` is set, so page
                # backward. Edges are newest-first: `endCursor` is the oldest.
                page_vars |= {"last": PAGE_SIZE, "before": cursor}

        connection: Any = await graphql_request(http, log, cls.query, page_vars)
        for key in cls.connection_path:
            # `tenant(name)` is null for an unknown tenant.
            if connection is None:
                return
            connection = connection[key]

        if cls.pager == Pager.LIST:
            for node in connection:
                yield node
            return

        for edge in connection["edges"]:
            yield edge["node"]

        page_info = connection["pageInfo"]
        has_more = (
            page_info["hasNextPage"]
            if cls.pager == Pager.FORWARD
            else page_info["hasPreviousPage"]
        )
        cursor = page_info["endCursor"]
        if not has_more or cursor is None:
            return


async def snapshot_stream(
    cls: type[EstuarySnapshot],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[EstuarySnapshot, None]:
    scopes: list[str] = []
    if cls.scope != Scope.GLOBAL:
        scopes = await resolve_scopes(http, log, config, cls.name, cls.required)
        if not scopes:
            log.info(f"{cls.name}: the API key lacks the required capabilities on every configured prefix")
            return

    match cls.scope:
        case Scope.PREFIX:
            queries = [({"scope": s}, {}) for s in scopes]
        case Scope.TENANT:
            # Billing is checked on the exact tenant prefix.
            tenants = sorted({s.split("/")[0] + "/" for s in scopes} & set(scopes))
            queries = [({"tenant": t}, {"tenant": t}) for t in tenants]
        case _:
            queries = [({}, {})]

    # Snapshot row ids follow emission order, so emit in a deterministic order
    # to avoid re-committing unchanged snapshots.
    rows: dict[tuple[Any, ...], EstuarySnapshot] = {}
    for variables, stamp in queries:
        async for node in _walk(cls, http, log, variables):
            doc = cls.model_validate({**node, **stamp})
            if cls.scope == Scope.GLOBAL_FILTERED and config.prefixes and not doc.in_scope(scopes):
                continue
            rows[doc.identity()] = doc

    for key in sorted(rows):
        yield rows[key]


LIVE_SPECS_REQUIRED = frozenset({CapabilityBit.CatalogRead})
LIVE_SPECS_PAGE_SIZE = 200
HISTORY_REFS_PAGE_SIZE = 100
HISTORY_NESTED_PAGE_SIZE = 25
HISTORY_DRAIN_PAGE_SIZE = 500
# `publishedAt` is the commit transaction's start time, so a publication only
# becomes visible after it. Sweeps stop this far behind now so every
# publication at or before the horizon has committed.
COMMIT_SETTLE = timedelta(minutes=5)


def _live_spec_refs_query(selection: str, fragments: str = "", params: str = "") -> str:
    return f"""
query LiveSpecRefs($prefix: Prefix!, $first: Int!, $after: String{params}) {{
  liveSpecs(by: {{ prefix: $prefix }}, first: $first, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{ node {{ catalogName {selection} }} }}
  }}
}}
{fragments}"""


async def walk_live_spec_refs(
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: dict[str, Any],
    first: int,
    after: str | None = None,
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """Yield successive pages of ref nodes under `variables["prefix"]`,
    ascending by catalog name, starting after the catalog name `after`."""
    while True:
        data = await graphql_request(
            http, log, query, {**variables, "first": first, "after": after}
        )
        connection = data["liveSpecs"]
        nodes = [edge["node"] for edge in connection["edges"]]
        if nodes:
            yield nodes
        # `hasNextPage` is `rows == first`, so an exact multiple ends with an empty page.
        if not connection["pageInfo"]["hasNextPage"] or not nodes:
            return
        after = connection["pageInfo"]["endCursor"]


async def snapshot_live_spec_refs(
    cls: type[LiveSpecRefDocument],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[LiveSpecRefDocument, None]:
    query = _live_spec_refs_query(cls.node_selection, cls.fragments)
    for prefix in await resolve_scopes(http, log, config, cls.name, LIVE_SPECS_REQUIRED):
        async for nodes in walk_live_spec_refs(
            http, log, query, {"prefix": prefix}, LIVE_SPECS_PAGE_SIZE
        ):
            for node in nodes:
                if (doc := cls.from_ref(node)) is not None:
                    yield doc


_HISTORY_CONNECTION = f"""
    pageInfo {{ hasNextPage endCursor }}
    edges {{ node {{ {PublicationHistoryItem.node_selection} }} }}
"""

PUBLICATION_HISTORY_PAGE_QUERY = _live_spec_refs_query(
    f"publicationHistory(after: $since, first: {HISTORY_NESTED_PAGE_SIZE}) {{ {_HISTORY_CONNECTION} }}",
    params=", $since: String!",
)

PUBLICATION_HISTORY_DRAIN_QUERY = f"""
query PublicationHistoryDrain($name: Name!, $after: String!, $first: Int!) {{
  liveSpecs(by: {{ names: [$name] }}) {{
    edges {{ node {{ catalogName publicationHistory(after: $after, first: $first) {{ {_HISTORY_CONNECTION} }} }} }}
  }}
}}
"""


def _rfc3339(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


async def _drain_spec_history(
    http: HTTPSession,
    log: Logger,
    name: str,
    history: dict[str, Any] | None,
    until: datetime,
) -> list[PublicationHistoryItem]:
    """Collect one spec's publications up to and including `until`, following
    its history past the nested page when needed. History is oldest-first, and
    forward paging has no upper-bound filter, so stop at the first item past `until`."""
    items: list[PublicationHistoryItem] = []
    while history is not None:
        for edge in history["edges"]:
            item = PublicationHistoryItem.model_validate({**edge["node"], "catalogName": name})
            if item.publishedAt > until:
                return items
            items.append(item)

        if not history["pageInfo"]["hasNextPage"]:
            break

        data = await graphql_request(
            http,
            log,
            PUBLICATION_HISTORY_DRAIN_QUERY,
            {"name": name, "after": history["pageInfo"]["endCursor"], "first": HISTORY_DRAIN_PAGE_SIZE},
        )
        edges = data["liveSpecs"]["edges"]
        # A spec hard-deleted since the page was read takes its history with it.
        history = edges[0]["node"]["publicationHistory"] if edges else None

    return items


async def _publication_history_pages(
    http: HTTPSession,
    log: Logger,
    prefixes: list[str],
    since: datetime,
    until: datetime,
    resume_after: str | None,
) -> AsyncGenerator[tuple[str, list[PublicationHistoryItem]], None]:
    """Yield (last catalog name of the page, items with since < publishedAt <= until)
    for each liveSpecs page, walking `prefixes` in order and starting after the
    catalog name `resume_after` when given. Every spec on a yielded page is fully drained."""
    start = 0
    if resume_after is not None:
        # If the configured prefixes changed and none match, restart (re-emits only).
        start = next((i for i, p in enumerate(prefixes) if resume_after.startswith(p)), 0)
        if not resume_after.startswith(prefixes[start]):
            resume_after = None

    for i, prefix in enumerate(prefixes[start:], start):
        async for nodes in walk_live_spec_refs(
            http,
            log,
            PUBLICATION_HISTORY_PAGE_QUERY,
            {"prefix": prefix, "since": _rfc3339(since)},
            HISTORY_REFS_PAGE_SIZE,
            after=resume_after if i == start else None,
        ):
            items: list[PublicationHistoryItem] = []
            for node in nodes:
                items.extend(
                    await _drain_spec_history(
                        http, log, node["catalogName"], node["publicationHistory"], until
                    )
                )
            yield nodes[-1]["catalogName"], items


async def fetch_publication_history(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[PublicationHistoryItem | LogCursor, None]:
    """Emit publications in (log_cursor, horizon], where horizon = now - COMMIT_SETTLE.

    ```
          log_cursor                       horizon
    ──────────┼───────────────────────────────┼─────────────▶ publishedAt
              (═══════════════════════════════]
              └ exclusive: the server's        └ later publications may be
                `after` filter                   uncommitted; next sweep
    ```

    The cursor is always the sweep horizon, never an item's timestamp, so
    publications sharing a microsecond can't straddle it. An interrupted sweep
    yields no cursor and is replayed.
    """
    assert isinstance(log_cursor, datetime)
    horizon = datetime.now(tz=UTC) - COMMIT_SETTLE
    if horizon <= log_cursor:
        return

    prefixes = await resolve_scopes(http, log, config, PublicationHistoryItem.name, LIVE_SPECS_REQUIRED)
    async for _, items in _publication_history_pages(http, log, prefixes, log_cursor, horizon, None):
        for item in items:
            yield item

    yield horizon


async def backfill_publication_history(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[PublicationHistoryItem | PageCursor, None]:
    """Emit publications in (start_date, cutoff], one liveSpecs page per call.

    The page cursor is the last fully drained catalog name: names are immutable
    and ascending, so deleted or new specs don't shift it. Backfill is complete
    when a call finds no further page.
    """
    assert isinstance(cutoff, datetime)
    assert page_cursor is None or isinstance(page_cursor, str)

    prefixes = await resolve_scopes(http, log, config, PublicationHistoryItem.name, LIVE_SPECS_REQUIRED)
    if not prefixes:
        return

    async for last_name, items in _publication_history_pages(
        http, log, prefixes, config.start_date, cutoff, page_cursor
    ):
        for item in items:
            yield item
        yield last_name
        return


CATALOG_STATS_NAMES_PAGE_SIZE = 500
CATALOG_STATS_NAMES_QUERY = _live_spec_refs_query("liveSpec { catalogType }")


async def _catalog_stats_names(
    http: HTTPSession, log: Logger, config: EndpointConfig, grain: Grain
) -> list[str]:
    """Each scope's own rollup name plus every live, non-test spec under it.

    The rollup is the only source of stats for deleted specs. Intermediate
    sub-prefix rollups are left out: they are sums of rows already captured.
    """
    names: set[str] = set()
    for prefix in await resolve_scopes(http, log, config, grain.stream_name, LIVE_SPECS_REQUIRED):
        names.add(prefix)
        async for nodes in walk_live_spec_refs(
            http, log, CATALOG_STATS_NAMES_QUERY, {"prefix": prefix}, CATALOG_STATS_NAMES_PAGE_SIZE
        ):
            for node in nodes:
                spec = node["liveSpec"]
                if spec is not None and spec["catalogType"] != "test":
                    names.add(node["catalogName"])
    return sorted(names)


def _window_buckets(names: list[str]) -> int:
    """Buckets per window, so the largest name chunk stays within the bucket budget."""
    largest_chunk = max(1, min(len(names), CatalogStats.names_per_query))
    return CatalogStats.bucket_budget // largest_chunk


async def _fetch_catalog_stats_window(
    http: HTTPSession,
    log: Logger,
    grain: Grain,
    names: list[str],
    start: datetime,
    end: datetime,
) -> AsyncGenerator[CatalogStats, None]:
    """Fetch buckets in [start, end) for every name. Both bounds are grain-aligned."""
    for i in range(0, len(names), CatalogStats.names_per_query):
        data = await graphql_request(
            http,
            log,
            CatalogStats.query,
            {
                "names": names[i : i + CatalogStats.names_per_query],
                "grain": grain.api_value,
                "start": _rfc3339(start),
                "end": _rfc3339(end),
            },
        )
        for edge in data["catalogStats"]["edges"]:
            yield CatalogStats.model_validate(edge["node"])


async def fetch_catalog_stats(
    http: HTTPSession,
    config: EndpointConfig,
    grain: Grain,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[CatalogStats | LogCursor, None]:
    """Re-read recent buckets, ending with the bucket that is still open.

    ```
          cursor - lookback      cursor        floor(now)    now
    ─────────────┼──────────────────┼──────────────┼──────────┼──▶ grain ticks
                 │                  │              │          │
    start ───────[══════════════════╪══════════════╪══════════╪══▶
    end ═════════╪══════════════════╪══════════════╪══════════╪═══)
    emitted ─────[══════════════════╪══════════════]          │
                 │                  │              └─ open bucket; re-read by
                 │                  │                 every later sweep
                 │                  └─ captured last sweep, re-read for late stats
                 └─ floored to the grain; clamped up to floor(start_date)
    ```

    `start` is inclusive and `end` exclusive, at `floor(now) + 1 bucket`. A
    bucket keeps accruing after it closes, so none is ever final: the cursor is
    the wall-clock instant the sweep began, and the lookback re-reads the rest.
    Re-read buckets collapse on the collection key.
    """
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)
    if now <= log_cursor:
        return

    lookback = timedelta(hours=config.advanced.catalog_stats_lookback_hours)
    window_start = max(grain.floor(config.start_date), grain.floor(log_cursor - lookback))
    stop = grain.add(grain.floor(now), 1)
    names = await _catalog_stats_names(http, log, config, grain)
    window_buckets = _window_buckets(names)

    cursor = log_cursor
    while window_start < stop:
        window_end = min(grain.add(window_start, window_buckets), stop)

        async for row in _fetch_catalog_stats_window(http, log, grain, names, window_start, window_end):
            yield row

        window_start = window_end

        # Checkpoint between windows so a long sweep doesn't restart from the
        # beginning. Cursors stay below `now` so the final one is always greater.
        if cursor < window_end < now:
            cursor = window_end
            yield cursor

    yield now


async def backfill_catalog_stats(
    http: HTTPSession,
    config: EndpointConfig,
    grain: Grain,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[CatalogStats | PageCursor, None]:
    """Fetch one window of buckets in [floor(start_date), floor(cutoff)) per call.

    The page cursor is the start of the next window. The incremental task's
    lookback re-reads the buckets at and after floor(cutoff).
    """
    assert isinstance(cutoff, datetime)
    assert page_cursor is None or isinstance(page_cursor, str)

    start = (
        grain.floor(config.start_date)
        if page_cursor is None
        else datetime.fromisoformat(page_cursor)
    )
    end = grain.floor(cutoff)
    if start >= end:
        return

    names = await _catalog_stats_names(http, log, config, grain)
    window_end = min(grain.add(start, _window_buckets(names)), end)

    async for row in _fetch_catalog_stats_window(http, log, grain, names, start, window_end):
        yield row

    if window_end < end:
        yield window_end.isoformat()
