import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    Initiative,
    Issue,
    IssueLabel,
    LinearGraphQLRemainder,
    LinearResource,
    PageInfo,
    Project,
)

# Linear exposes a single GraphQL endpoint. Every stream POSTs a different query document to
# this URL rather than hitting per-resource REST paths, so there are no path segments to
# append — only differing GraphQL query bodies.
API = "https://api.linear.app/graphql"

# Argument-validated ceiling. `first: 251` is rejected rather than clamped, and the
# rejection arrives as HTTP 200 with an `errors` array — see `06 - Page Size Over Limit`.
MAX_PAGE_SIZE = 250

# Linear stamps timestamps at millisecond resolution, so one tick is one millisecond.
TICK = timedelta(milliseconds=1)

# Requests, not complexity, is the binding budget: 2,500 requests/hour against 3,000,000
# complexity points/hour, and a full page of the widest stream costs well under 5,000
# points. Pause before the request budget is spent, because a rate-limited response cannot
# be retried by the framework (see `_execute`).
_REQUESTS_REMAINING_FLOOR = 50
# Bound a single pre-emptive sleep so a malformed or stale reset header cannot park the
# connector indefinitely.
_MAX_SLEEP_SECONDS = 60 * 60

_AUTH_ERROR_CODES = frozenset({"AUTHENTICATION_ERROR", "FORBIDDEN"})


def _format_timestamp(dt: datetime) -> str:
    """RFC3339 with milliseconds and a literal `Z`, which Linear accepts verbatim."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def floor_to_tick(dt: datetime) -> datetime:
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _horizon() -> datetime:
    """The last fully-elapsed tick.

    A tick is final the moment it has elapsed, because every Linear write stamps its own
    "now"; nothing can later appear with a timestamp inside it.
    """
    return floor_to_tick(datetime.now(tz=UTC)) - TICK


def _connection_query(
    entity: type[LinearResource],
    *,
    paginated: bool,
    ascending: bool,
    archival: bool = False,
) -> str:
    """Build the document for one page of `entity`'s Relay connection."""
    cursor_field = entity.ARCHIVAL_CURSOR_FIELD if archival else entity.CURSOR_FIELD

    args = [
        "first: $first",
        # Mandatory on every request, not only the archival pass: without it archived rows
        # are unreachable by any means, and their absence is indistinguishable from
        # "nothing was archived".
        "includeArchived: true",
        f"filter: {{ {cursor_field}: {{ gt: $after_ts, lte: $through }} }}",
    ]
    if paginated:
        args.append("after: $after")

    # The archival pass cannot be sorted — `IssueSortInput` exposes no `archivedAt` member —
    # so it always walks in the connection's default order.
    if entity.supports_sort and not archival:
        order = "Ascending" if ascending else "Descending"
        args.append(f"sort: [{{{cursor_field}: {{order: {order}}}}}]")
    elif not entity.supports_sort:
        args.append(f"orderBy: {cursor_field}")

    declarations = "$first: Int!, $after_ts: DateTimeOrDuration!, $through: DateTimeOrDuration!"
    if paginated:
        declarations += ", $after: String"

    return f"""
query Fetch({declarations}) {{
  {entity.root_field}({", ".join(args)}) {{
    nodes {{ {entity.selection} }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""


async def _throttle(log: Logger, headers: Mapping[str, str]) -> None:
    """Sleep when the hourly request budget is nearly spent.

    Guarantees the connector stays under the request limit. This is the only reliable
    defence: Linear signals exhaustion as HTTP 400 with a `RATELIMITED` code, and the CDK
    raises 4xx immediately without consulting `should_retry`, so the framework cannot retry
    one. `_execute` keeps a backstop for the race.
    """
    raw_remaining = headers.get("x-ratelimit-requests-remaining")
    raw_reset = headers.get("x-ratelimit-requests-reset")
    if raw_remaining is None or raw_reset is None:
        return

    try:
        remaining = int(raw_remaining)
        # Reset headers are UTC epoch MILLISECONDS — not seconds, and not a delta.
        reset_at = datetime.fromtimestamp(int(raw_reset) / 1000, tz=UTC)
    except (TypeError, ValueError):
        log.warning(
            "could not parse Linear rate limit headers; continuing without throttling",
            {"remaining": raw_remaining, "reset": raw_reset},
        )
        return

    if remaining > _REQUESTS_REMAINING_FLOOR:
        return

    delay = min((reset_at - datetime.now(tz=UTC)).total_seconds(), _MAX_SLEEP_SECONDS)
    if delay <= 0:
        return

    log.info(
        "Linear request budget nearly exhausted; sleeping until the window resets",
        {"remaining": remaining, "reset_at": reset_at.isoformat(), "sleep_seconds": delay},
    )
    await asyncio.sleep(delay)


def _check_errors(
    log: Logger, entity: type[LinearResource], remainder: LinearGraphQLRemainder
) -> None:
    """Raise unless the response carries usable rows.

    A populated `data` block alongside a non-empty `errors` array is a partial success and
    its rows are emitted: a field gated behind a paid add-on errors on every page, and
    treating that as fatal would break the sync outright on workspaces lacking the feature.
    An empty `data` with errors is a total failure and raises.
    """
    errors = remainder.errors or []
    if not errors:
        return

    codes = {err.extensions.code for err in errors if err.extensions}
    messages = [err.message for err in errors]

    if remainder.data is not None:
        log.warning(
            "Linear returned errors alongside data; emitting the rows that resolved",
            {"stream": entity.name, "codes": sorted(codes), "errors": messages},
        )
        return

    if codes & _AUTH_ERROR_CODES:
        raise RuntimeError(
            f"Linear rejected the configured API key while fetching {entity.name}. "
            f"Confirm the key is valid and has read access. Errors: {messages}"
        )

    raise RuntimeError(f"Linear returned errors for {entity.name}: {messages}")


async def _execute(
    entity: type[LinearResource],
    http: HTTPSession,
    log: Logger,
    query: str,
    variables: dict[str, Any],
) -> tuple[list[LinearResource], PageInfo]:
    """POST one query document and return its rows plus pagination state."""
    try:
        headers, body = await http.request_stream(
            log, API, method="POST", json={"query": query, "variables": variables}
        )
    except HTTPError as err:
        # Backstop to `_throttle`. A rate-limit rejection is only identifiable from the
        # body, which the CDK embeds in the message; its status is 400, which the framework
        # treats as terminal.
        if err.code == 400 and "RATELIMITED" in err.message:
            log.warning(
                "Linear rate limit hit despite pre-emptive throttling; backing off",
                {"stream": entity.name},
            )
            await asyncio.sleep(60)
            headers, body = await http.request_stream(
                log, API, method="POST", json={"query": query, "variables": variables}
            )
        else:
            raise

    processor = IncrementalJsonProcessor(
        body(),
        f"data.{entity.root_field}.nodes.item",
        entity,
        remainder_cls=LinearGraphQLRemainder,
    )

    nodes = [node async for node in processor]

    remainder = processor.get_remainder()
    _check_errors(log, entity, remainder)
    await _throttle(log, headers)

    return nodes, remainder.page_info(entity.root_field)


async def _walk(
    entity: type[LinearResource],
    http: HTTPSession,
    log: Logger,
    after_ts: datetime,
    through: datetime,
    *,
    ascending: bool = True,
    archival: bool = False,
) -> AsyncGenerator[LinearResource, None]:
    """Yield every row in `(after_ts, through]`, following `pageInfo.endCursor`.

    The Relay cursor is used only within one walk and is never checkpointed; durable
    position is always a timestamp.
    """
    after: str | None = None

    while True:
        variables: dict[str, Any] = {
            "first": MAX_PAGE_SIZE,
            "after_ts": _format_timestamp(after_ts),
            "through": _format_timestamp(through),
        }
        if after is not None:
            variables["after"] = after

        query = _connection_query(
            entity, paginated=after is not None, ascending=ascending, archival=archival
        )
        nodes, page_info = await _execute(entity, http, log, query, variables)

        for node in nodes:
            yield node

        if not page_info.hasNextPage or not page_info.endCursor:
            return

        after = page_info.endCursor


async def _fetch_changes(
    entity: type[LinearResource],
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    """Emit rows whose cursor field falls in `(log_cursor, horizon]`, then checkpoint.

    Guarantees every row is emitted at least once and the checkpoint never advances past an
    unswept tick, for either sort direction.

                  log_cursor  log_cursor+1ms   horizon = last elapsed ms
    ─────────────────┼──────────┼──────────────┼─────▶ time (1ms ticks)
                     │          │              │
    gt ──────────────(══════════╪══════════════╪═════▶
    lte ═════════════╪══════════╪══════════════]
    emitted ─────────┼──────────[══════════════]
                     │          │              └─ the current ms may still be
                     │          │                 written to; it waits for the
                     │          │                 next poll's window
                     │          └─ first emitted tick
                     └─ already captured by the previous poll

    `gt` is exclusive and `lte` inclusive on both filter clocks. Bounding the window at a
    fully-elapsed tick is what makes an exclusive lower bound safe here: every row sharing
    the boundary tick was drained before the checkpoint moved, so ties cannot be split.
    """
    assert isinstance(log_cursor, datetime)

    horizon = _horizon()
    if horizon <= log_cursor:
        return

    emitted = False
    async for node in _walk(
        entity, http, log, log_cursor, horizon, ascending=entity.supports_sort
    ):
        yield node
        emitted = True

    if emitted:
        yield horizon


async def _fetch_issue_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    """Emit Issues changed or archived in `(log_cursor, horizon]`, then checkpoint.

    Guarantees archival is observable for Issues, which a cursor over `updatedAt` alone
    cannot provide: archiving does not advance `updatedAt`, so an archived row would
    otherwise stay invisible forever. `IssueFilter` is the only filter type exposing an
    `archivedAt` comparator, making Issues the only stream where this is recoverable.

                  log_cursor  log_cursor+1ms   horizon = last elapsed ms
    ─────────────────┼──────────┼──────────────┼─────▶ time (1ms ticks)
                     │          │              │
    updatedAt ───────(══════════╪══════════════]
    archivedAt ──────(══════════╪══════════════]
    emitted ─────────┼──────────[══════════════]
                     │          │              └─ both clocks share one horizon,
                     │          │                 so one checkpoint covers both
                     │          └─ first emitted tick
                     └─ already captured by the previous poll

    Both passes span the same window and the single checkpoint means "both clocks are swept
    through `horizon`", which is why two unrelated clocks can share one cursor scalar —
    necessary because `LogCursor` admits no two-element tuple. A row that is both updated
    and archived in the window is emitted twice and collapsed by the collection key.
    """
    assert isinstance(log_cursor, datetime)

    horizon = _horizon()
    if horizon <= log_cursor:
        return

    emitted = False

    async for node in _walk(Issue, http, log, log_cursor, horizon, ascending=True):
        yield node
        emitted = True

    async for node in _walk(Issue, http, log, log_cursor, horizon, archival=True):
        yield node
        emitted = True

    if emitted:
        yield horizon


async def _backfill(
    entity: type[LinearResource],
    http: HTTPSession,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    """Emit historical rows below the cutoff, checkpointing a timestamp watermark.

    Guarantees resumption is deletion-proof: the checkpoint is the cursor value last
    drained, not a positional offset, so a row removed mid-backfill renumbers nothing and
    no untouched row can be skipped.

                  start_date                   cutoff-1ms      cutoff
    ─────────────────┼──────────────────────────────┼────────────┼──▶ time
                     │                              │            │
    gt ──────────────(══════════════════════════════╪════════════╪──▶
    lte ═════════════╪══════════════════════════════]            │
    emitted ─────────[══════════════════════════════]            │
                     │                              │            └─ owned by
                     │                              │               incremental
                     │                              └─ last backfilled tick
                     └─ start of history; boundary instant is not load-bearing

    The watermark advances only when a page's cursor value strictly moves, so a tie group
    wider than one page is drained within a single invocation rather than re-read forever.
    Archived rows need no special handling: `includeArchived` is always set and an archived
    row keeps the `updatedAt` of its last real edit, so the ordinary filter reaches it.
    """
    assert isinstance(cutoff, datetime)

    # Backfill owns everything strictly below the cutoff; incremental owns the cutoff tick
    # onward, so the two meet with no gap and no overlap.
    through = floor_to_tick(cutoff) - TICK

    # Ascending streams resume forwards from the last drained value. Labels cannot sort, so
    # its walk is descending and it simply restarts from `start_date`; label populations are
    # small enough (tens to low hundreds) to drain in one page.
    ascending = entity.supports_sort
    after_ts = start_date
    if ascending and isinstance(page, str):
        after_ts = datetime.fromisoformat(page)

    if after_ts >= through:
        return

    watermark = after_ts
    advanced = False

    async for node in _walk(entity, http, log, after_ts, through, ascending=ascending):
        yield node

        if ascending and node.get_cursor() > watermark:
            watermark = node.get_cursor()
            advanced = True

    # Yielding a watermark asserts everything at or below it is captured, so it is only
    # emitted once the walk has drained. Returning without one ends the backfill.
    if advanced:
        yield watermark.isoformat()


# The CDK's fetch contracts bind these by name, one public pair per stream over the shared
# engine above.


async def fetch_issues(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_issue_changes(http, log, log_cursor):
        yield item


async def fetch_projects(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_changes(Project, http, log, log_cursor):
        yield item


async def fetch_initiatives(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_changes(Initiative, http, log, log_cursor):
        yield item


async def fetch_labels(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_changes(IssueLabel, http, log, log_cursor):
        yield item


async def backfill_issues(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Issue, http, start_date, log, page, cutoff):
        yield item


async def backfill_projects(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Project, http, start_date, log, page, cutoff):
        yield item


async def backfill_initiatives(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Initiative, http, start_date, log, page, cutoff):
        yield item


async def backfill_labels(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(IssueLabel, http, start_date, log, page, cutoff):
        yield item
