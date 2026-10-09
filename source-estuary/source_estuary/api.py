from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPSession
from pydantic import BaseModel

from .models import (
    AuthorizedPrefix,
    AuthorizedPrefixesData,
    CapabilityBit,
    CatalogStats,
    CatalogStatsData,
    Connection,
    EstuarySnapshot,
    GraphQLResponse,
    Grain,
    LiveSpecRefDocument,
    LiveSpecsByNameData,
    LiveSpecsData,
    Pager,
    PublicationHistoryItem,
    RawData,
    Scope,
)

DataT = TypeVar("DataT", bound=BaseModel)

API_BASE_URL = "https://api.estuary.dev"

GRAPHQL_PATH = "/api/graphql"
GRAPHQL_URL = f"{API_BASE_URL}{GRAPHQL_PATH}"

# The API enforces no maximum page size; this bounds response sizes.
PAGE_SIZE = 100


class GraphQLError(RuntimeError):
    pass


async def graphql_request(
    log: Logger,
    http: HTTPSession,
    query: str,
    variables: dict[str, Any],
    data_model: type[DataT],
) -> DataT:
    """POST a query and return its `data` as `data_model`, raising GraphQLError
    on any GraphQL error.

    A provisional authorization failure is an HTTP 307 back to /api/graphql;
    aiohttp follows it with the same method, body and Authorization header.
    """
    response = GraphQLResponse.model_validate_json(
        await http.request(
            log,
            GRAPHQL_URL,
            method="POST",
            json={"query": query, "variables": variables},
        )
    )
    if response.errors:
        raise GraphQLError(f"Estuary API returned errors: {[e.message for e in response.errors]}")
    if response.data is None:
        raise GraphQLError("Estuary API returned no data")
    return data_model.model_validate(response.data)


AUTHORIZED_PREFIXES_QUERY = """
query AuthorizedPrefixes {
  prefixes(by: { minCapability: read }) {
    edges { node { prefix capabilities } }
  }
}
"""


async def fetch_authorized_prefixes(log: Logger, http: HTTPSession) -> list[AuthorizedPrefix]:
    # `prefixes` returns every row when `first` is omitted.
    data = await graphql_request(log, http, AUTHORIZED_PREFIXES_QUERY, {}, AuthorizedPrefixesData)
    return [edge.node for edge in data.prefixes.edges]


def _prune_nested(prefixes: set[str]) -> list[str]:
    pruned: list[str] = []
    for p in sorted(prefixes):
        if not pruned or not p.startswith(pruned[-1]):
            pruned.append(p)
    return pruned


async def resolve_scopes(
    log: Logger,
    http: HTTPSession,
    prefixes: list[str],
    stream: str,
    required: frozenset[CapabilityBit],
) -> list[str]:
    """Return the disjoint prefixes to query: the configured `prefixes` (or every
    authorized prefix when none are configured) where the API key holds every
    `required` capability.

    A configured prefix the key can't read at all raises, rather than being
    skipped: skipping would tombstone its snapshot rows and let incremental
    cursors move past data that can't be re-read once access returns. A
    configured prefix the key can read, but without a stream's extra
    capabilities, is skipped for that stream.
    """
    authorized = await fetch_authorized_prefixes(log, http)
    holders = [a.prefix for a in authorized if required <= set(a.capabilities)]

    if not prefixes:
        return _prune_nested(set(holders))

    readable = [a.prefix for a in authorized]
    lost = [
        c for c in prefixes
        if not any(c.startswith(r) or r.startswith(c) for r in readable)
    ]
    if lost:
        raise ValidationError([
            f"The API key can no longer read the configured prefixes {lost}. Restore the key's access, or remove these prefixes from the capture."
        ])

    candidates: set[str] = set()
    for c in prefixes:
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
    log: Logger,
    http: HTTPSession,
    cls: type[EstuarySnapshot],
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

        result: Any = (await graphql_request(log, http, cls.query, page_vars, RawData)).root
        for key in cls.connection_path:
            # `tenant(name)` is null for an unknown tenant.
            if result is None:
                return
            result = result[key]

        if cls.pager == Pager.LIST:
            for node in result:
                yield node
            return

        connection = Connection.model_validate(result)
        for edge in connection.edges:
            yield edge.node

        has_more = (
            connection.pageInfo.hasNextPage
            if cls.pager == Pager.FORWARD
            else connection.pageInfo.hasPreviousPage
        )
        if has_more is None:
            raise GraphQLError(f"{cls.name}: the query's pageInfo is missing the field its pager reads")
        cursor = connection.pageInfo.endCursor
        if not has_more or cursor is None:
            return


async def snapshot_stream(
    cls: type[EstuarySnapshot],
    http: HTTPSession,
    prefixes: list[str],
    log: Logger,
) -> AsyncGenerator[EstuarySnapshot, None]:
    scopes: list[str] = []
    if cls.scope != Scope.GLOBAL:
        scopes = await resolve_scopes(log, http, prefixes, cls.name, cls.required)
        if not scopes:
            log.info(f"{cls.name}: the API key lacks the required capabilities on every configured prefix")
            return

    match cls.scope:
        case Scope.PREFIX:
            queries = [({"scope": s}, {}) for s in scopes]
        case Scope.TENANT:
            # Billing is checked on the exact tenant prefix.
            tenants = sorted({s.split("/")[0] + "/" for s in scopes} & set(scopes))
            if not tenants:
                log.info(f"{cls.name}: no tenant prefix (e.g. acmeCo/) is configured; {cls.name} are only captured for a configured tenant prefix")
            queries = [({"tenant": t}, {"tenant": t}) for t in tenants]
        case _:
            queries = [({}, {})]

    # Snapshot row ids follow emission order, so emit in a deterministic order
    # to avoid re-committing unchanged snapshots.
    rows: dict[tuple[Any, ...], EstuarySnapshot] = {}
    for variables, stamp in queries:
        async for node in _walk(log, http, cls, variables):
            doc = cls.model_validate({**node, **stamp})
            if cls.scope == Scope.GLOBAL_FILTERED and prefixes and not doc.in_scope(scopes):
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
    log: Logger,
    http: HTTPSession,
    query: str,
    variables: dict[str, Any],
    first: int,
    after: str | None = None,
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """Yield successive pages of ref nodes under `variables["prefix"]`,
    ascending by catalog name, starting after the catalog name `after`."""
    while True:
        data = await graphql_request(
            log, http, query, {**variables, "first": first, "after": after}, LiveSpecsData
        )
        connection = data.liveSpecs
        nodes = [edge.node for edge in connection.edges]
        if nodes:
            yield nodes
        # `hasNextPage` is `rows == first`, so an exact multiple ends with an empty page.
        if not connection.pageInfo.hasNextPage or not nodes:
            return
        after = connection.pageInfo.endCursor


async def snapshot_live_spec_refs(
    cls: type[LiveSpecRefDocument],
    http: HTTPSession,
    prefixes: list[str],
    log: Logger,
) -> AsyncGenerator[LiveSpecRefDocument, None]:
    query = _live_spec_refs_query(cls.node_selection, cls.fragments)
    for prefix in await resolve_scopes(log, http, prefixes, cls.name, LIVE_SPECS_REQUIRED):
        async for nodes in walk_live_spec_refs(
            log, http, query, {"prefix": prefix}, LIVE_SPECS_PAGE_SIZE
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
    log: Logger,
    http: HTTPSession,
    name: str,
    history: Connection | None,
    until: datetime,
) -> list[PublicationHistoryItem]:
    """Collect one spec's publications up to and including `until`, following
    its history past the nested page when needed. History is oldest-first, and
    forward paging has no upper-bound filter, so stop at the first item past `until`."""
    items: list[PublicationHistoryItem] = []
    while history is not None:
        for edge in history.edges:
            item = PublicationHistoryItem.model_validate({**edge.node, "catalogName": name})
            if item.publishedAt > until:
                return items
            items.append(item)

        if not history.pageInfo.hasNextPage:
            break

        data = await graphql_request(
            log,
            http,
            PUBLICATION_HISTORY_DRAIN_QUERY,
            {"name": name, "after": history.pageInfo.endCursor, "first": HISTORY_DRAIN_PAGE_SIZE},
            LiveSpecsByNameData,
        )
        edges = data.liveSpecs.edges
        # A spec hard-deleted since the page was read takes its history with it.
        history = _history_connection(edges[0].node) if edges else None

    return items


def _history_connection(node: dict[str, Any]) -> Connection | None:
    history = node["publicationHistory"]
    return None if history is None else Connection.model_validate(history)


async def _publication_history_pages(
    log: Logger,
    http: HTTPSession,
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
            log,
            http,
            PUBLICATION_HISTORY_PAGE_QUERY,
            {"prefix": prefix, "since": _rfc3339(since)},
            HISTORY_REFS_PAGE_SIZE,
            after=resume_after if i == start else None,
        ):
            items: list[PublicationHistoryItem] = []
            for node in nodes:
                items.extend(
                    await _drain_spec_history(
                        log, http, node["catalogName"], _history_connection(node), until
                    )
                )
            yield nodes[-1]["catalogName"], items


async def fetch_publication_history(
    http: HTTPSession,
    prefixes: list[str],
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

    scopes = await resolve_scopes(log, http, prefixes, PublicationHistoryItem.name, LIVE_SPECS_REQUIRED)
    async for _, items in _publication_history_pages(log, http, scopes, log_cursor, horizon, None):
        for item in items:
            yield item

    yield horizon


async def backfill_publication_history(
    http: HTTPSession,
    prefixes: list[str],
    start_date: datetime,
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

    scopes = await resolve_scopes(log, http, prefixes, PublicationHistoryItem.name, LIVE_SPECS_REQUIRED)
    if not scopes:
        return

    async for last_name, items in _publication_history_pages(
        log, http, scopes, start_date, cutoff, page_cursor
    ):
        for item in items:
            yield item
        yield last_name
        return


CATALOG_STATS_NAMES_PAGE_SIZE = 500
CATALOG_STATS_NAMES_QUERY = _live_spec_refs_query("liveSpec { catalogType }")


async def _catalog_stats_names(
    log: Logger, http: HTTPSession, prefixes: list[str], grain: Grain
) -> list[str]:
    """Each scope's own rollup name plus every live, non-test spec under it.

    The rollup is the only source of stats for deleted specs. Intermediate
    sub-prefix rollups are left out: they are sums of rows already captured.
    """
    names: set[str] = set()
    for prefix in await resolve_scopes(log, http, prefixes, grain.stream_name, LIVE_SPECS_REQUIRED):
        names.add(prefix)
        async for nodes in walk_live_spec_refs(
            log, http, CATALOG_STATS_NAMES_QUERY, {"prefix": prefix}, CATALOG_STATS_NAMES_PAGE_SIZE
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
    log: Logger,
    http: HTTPSession,
    grain: Grain,
    names: list[str],
    start: datetime,
    end: datetime,
) -> AsyncGenerator[CatalogStats, None]:
    """Fetch buckets in [start, end) for every name. Both bounds are grain-aligned."""
    for i in range(0, len(names), CatalogStats.names_per_query):
        data = await graphql_request(
            log,
            http,
            CatalogStats.query,
            {
                "names": names[i : i + CatalogStats.names_per_query],
                "grain": grain.api_value,
                "start": _rfc3339(start),
                "end": _rfc3339(end),
            },
            CatalogStatsData,
        )
        for edge in data.catalogStats.edges:
            yield edge.node


async def fetch_catalog_stats(
    http: HTTPSession,
    prefixes: list[str],
    start_date: datetime,
    lookback: timedelta,
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

    window_start = max(grain.floor(start_date), grain.floor(log_cursor - lookback))
    stop = grain.add(grain.floor(now), 1)
    names = await _catalog_stats_names(log, http, prefixes, grain)
    window_buckets = _window_buckets(names)

    cursor = log_cursor
    while window_start < stop:
        window_end = min(grain.add(window_start, window_buckets), stop)

        async for row in _fetch_catalog_stats_window(log, http, grain, names, window_start, window_end):
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
    prefixes: list[str],
    start_date: datetime,
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
        grain.floor(start_date)
        if page_cursor is None
        else datetime.fromisoformat(page_cursor)
    )
    end = grain.floor(cutoff)
    if start >= end:
        return

    names = await _catalog_stats_names(log, http, prefixes, grain)
    window_end = min(grain.add(start, _window_buckets(names)), end)

    async for row in _fetch_catalog_stats_window(log, http, grain, names, start, window_end):
        yield row

    if window_end < end:
        yield window_end.isoformat()
