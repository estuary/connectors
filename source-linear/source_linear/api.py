import asyncio
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator, Callable, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    Initiative,
    Issue,
    IssueLabel,
    LinearEntity,
    LinearGraphQLRemainder,
    LinearResource,
    PageInfo,
    Project,
)

# PEP 695 syntax needs Python 3.12; this connector still targets ^3.11.
_Entity = TypeVar("_Entity", bound=LinearEntity)

# Linear exposes a single GraphQL endpoint. Every stream POSTs a different query document to
# this URL rather than hitting per-resource REST paths, so there are no path segments to
# append — only differing GraphQL query bodies.
API = "https://api.linear.app/graphql"

# Argument-validated ceiling. `first: 251` is rejected rather than clamped, and the
# rejection arrives as HTTP 200 with an `errors` array — see `06 - Page Size Over Limit`.
MAX_PAGE_SIZE = 250

# Linear stamps timestamps at millisecond resolution, so one tick is one millisecond.
TICK = timedelta(milliseconds=1)

# Linear meters two independent hourly budgets — 2,500 requests and 3,000,000 complexity
# points — and either can bind first, because complexity is charged per row returned rather
# than per request. Pause before either is spent, since a rate-limited response cannot be
# retried by the framework (see `_execute`).
_REQUESTS_REMAINING_FLOOR = 50
# Measured full-page costs run from 10 points (Issues) to 2,225 (Projects), so hold back
# several of the costliest pages rather than a fixed small count.
_COMPLEXITY_REMAINING_FLOOR = 20_000
# Bound a single pre-emptive sleep so a malformed or stale reset header cannot park the
# connector indefinitely.
_MAX_SLEEP_SECONDS = 60 * 60
# The `RATELIMITED` backstop's retry budget, and its starting backoff when no reset time
# has been seen yet.
_RATE_LIMITED_ATTEMPTS = 5
_RATE_LIMITED_BACKOFF_SECONDS = 60

# UNVERIFIED: Linear documents no distinct error for exceeding its 10,000-point per-query
# cap, and the test workspace is too small to provoke one (see `05 - Complexity Envelope`).
# This assumes the error message mentions complexity.
_COMPLEXITY_ERROR_RE = re.compile(r"complex", re.IGNORECASE)

# The latest budget reset any response has announced. A `RATELIMITED` rejection carries no
# usable headers, so the backstop waits on this instead.
_latest_reset_at: datetime | None = None


class _QueryTooComplex(Exception):
    """One page's query exceeded Linear's per-query complexity cap."""

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


def _query_document(
    entity: type[LinearEntity], args: list[str], declarations: list[str], *, paginated: bool
) -> str:
    """Build the document for one page of `entity`'s Relay connection.

    `includeArchived` is set on every request: without it archived rows are unreachable by
    any means, and their absence is indistinguishable from "nothing was archived".
    """
    args = ["first: $first", "includeArchived: true", *args]
    declarations = ["$first: Int!", *declarations]
    if paginated:
        args.append("after: $after")
        declarations.append("$after: String")

    return f"""
query Fetch({", ".join(declarations)}) {{
  {entity.root_field}({", ".join(args)}) {{
    nodes {{ {entity.selection} }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""


def _connection_query(
    entity: type[LinearResource],
    *,
    paginated: bool,
    archival: bool = False,
) -> str:
    """One page of `entity`'s rows whose cursor field falls in `(after_ts, through]`."""
    cursor_field = entity.ARCHIVAL_CURSOR_FIELD if archival else entity.CURSOR_FIELD
    assert cursor_field is not None, f"{entity.name} has no archival clock"

    args = [f"filter: {{ {cursor_field}: {{ gt: $after_ts, lte: $through }} }}"]
    # The archival pass cannot be sorted — `IssueSortInput` exposes no `archivedAt` member —
    # so it walks in the connection's default order.
    if not archival:
        args.append(f"sort: [{{{cursor_field}: {{order: Ascending}}}}]")

    return _query_document(
        entity,
        args,
        ["$after_ts: DateTimeOrDuration!", "$through: DateTimeOrDuration!"],
        paginated=paginated,
    )


def _snapshot_query(entity: type[LinearEntity], *, paginated: bool) -> str:
    """One page of all of `entity`'s rows, archived ones included."""
    return _query_document(entity, [], [], paginated=paginated)


def _budget_delay(
    log: Logger, headers: Mapping[str, str], budget: str, floor: int
) -> float:
    """Seconds to wait for `budget` to reset, or 0 while it still has headroom."""
    raw_remaining = headers.get(f"x-ratelimit-{budget}-remaining")
    raw_reset = headers.get(f"x-ratelimit-{budget}-reset")
    if raw_remaining is None or raw_reset is None:
        return 0.0

    try:
        remaining = int(raw_remaining)
        # Reset headers are UTC epoch MILLISECONDS — not seconds, and not a delta.
        reset_at = datetime.fromtimestamp(int(raw_reset) / 1000, tz=UTC)
    except (TypeError, ValueError):
        log.warning(
            "could not parse Linear rate limit headers; continuing without throttling",
            {"budget": budget, "remaining": raw_remaining, "reset": raw_reset},
        )
        return 0.0

    _note_reset(reset_at)

    if remaining > floor:
        return 0.0

    delay = min((reset_at - datetime.now(tz=UTC)).total_seconds(), _MAX_SLEEP_SECONDS)
    if delay <= 0:
        return 0.0

    log.info(
        "Linear budget nearly exhausted; sleeping until the window resets",
        {
            "budget": budget,
            "remaining": remaining,
            "reset_at": reset_at.isoformat(),
            "sleep_seconds": delay,
        },
    )
    return delay


def _note_reset(reset_at: datetime) -> None:
    global _latest_reset_at
    if _latest_reset_at is None or reset_at > _latest_reset_at:
        _latest_reset_at = reset_at


def _rate_limited_delay(attempt: int) -> float:
    """Seconds to wait before retrying a `RATELIMITED` rejection.

    Waits for the latest budget reset Linear has announced when one is still ahead, since
    both budgets refill hourly and an earlier retry would only be rejected again. Without
    one, backs off exponentially from `_RATE_LIMITED_BACKOFF_SECONDS`.
    """
    now = datetime.now(tz=UTC)
    if _latest_reset_at is not None and _latest_reset_at > now:
        delay = (_latest_reset_at - now).total_seconds() + 1
    else:
        delay = _RATE_LIMITED_BACKOFF_SECONDS * 2 ** (attempt - 1)
    return min(delay, _MAX_SLEEP_SECONDS)


async def _throttle(log: Logger, headers: Mapping[str, str]) -> None:
    """Sleep until whichever hourly budget is nearly spent has reset.

    Guarantees the connector stays under both limits. This is the primary defence:
    Linear signals exhaustion as HTTP 400 with a `RATELIMITED` code, and the CDK raises 4xx
    immediately without consulting `should_retry`, so the framework cannot retry one.
    `_request` keeps a backstop for the race.

    Both budgets are checked because which one binds depends on the workspace, not on the
    connector. Requests bind when rows are sparse, but complexity is charged per row
    returned, so a workspace whose records have many populated relations can exhaust the
    complexity budget while thousands of requests remain.
    """
    delay = max(
        _budget_delay(log, headers, "requests", _REQUESTS_REMAINING_FLOOR),
        _budget_delay(log, headers, "complexity", _COMPLEXITY_REMAINING_FLOOR),
    )
    if delay > 0:
        await asyncio.sleep(delay)


def _check_errors(
    log: Logger, entity: type[LinearEntity], remainder: LinearGraphQLRemainder
) -> None:
    """Raise if the response carries any GraphQL error.

    A partial success is treated as a failure too. The selections exclude every field
    gated behind a paid add-on, so no partial error is expected, and emitting the rows that
    did resolve would hide a resolver failure that nulls a field across a whole page.
    """
    errors = remainder.errors or []
    if not errors:
        return

    codes = {err.extensions.code for err in errors if err.extensions}
    messages = [err.message for err in errors]

    # An exhausted hourly complexity budget is `RATELIMITED`, not an oversized page.
    if "RATELIMITED" not in codes and any(_COMPLEXITY_ERROR_RE.search(m) for m in messages):
        raise _QueryTooComplex(f"{entity.name}: {messages}")

    if codes & _AUTH_ERROR_CODES:
        raise RuntimeError(
            f"Linear rejected the configured API key while fetching {entity.name}. "
            f"Confirm the key is valid and has read access. Errors: {messages}"
        )

    raise RuntimeError(f"Linear returned errors for {entity.name}: {messages}")


async def _request(
    entity: type[LinearEntity],
    log: Logger,
    http: HTTPSession,
    query: str,
    variables: dict[str, Any],
):
    """POST one query document, retrying `RATELIMITED` rejections until a budget resets.

    A rate-limit rejection is only identifiable from the body, which the CDK embeds in the
    message; its status is 400, which the framework treats as terminal.
    """
    payload = {"query": query, "variables": variables}
    for attempt in range(1, _RATE_LIMITED_ATTEMPTS + 1):
        try:
            return await http.request_stream(log, API, method="POST", json=payload)
        except HTTPError as err:
            if err.code != 400:
                raise
            if _COMPLEXITY_ERROR_RE.search(err.message) and "RATELIMITED" not in err.message:
                raise _QueryTooComplex(f"{entity.name}: {err.message}") from err
            if "RATELIMITED" not in err.message or attempt == _RATE_LIMITED_ATTEMPTS:
                raise

            delay = _rate_limited_delay(attempt)
            log.warning(
                "Linear rate limit hit despite pre-emptive throttling; backing off",
                {"stream": entity.name, "attempt": attempt, "sleep_seconds": delay},
            )
            await asyncio.sleep(delay)

    raise AssertionError("unreachable: the last attempt either returns or raises")


async def _execute(
    entity: type[_Entity],
    log: Logger,
    http: HTTPSession,
    query: str,
    variables: dict[str, Any],
) -> tuple[list[_Entity], PageInfo]:
    """POST one query document and return its rows plus pagination state."""
    headers, body = await _request(entity, log, http, query, variables)

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


async def _pages(
    entity: type[_Entity],
    log: Logger,
    http: HTTPSession,
    query_for: Callable[[bool], str],
    variables: dict[str, Any],
) -> AsyncGenerator[tuple[list[_Entity], bool], None]:
    """Yield one Relay connection a page at a time, with whether another page follows.

    A page rejected as too complex is retried at half the size, and the walk keeps the
    smaller size from then on. The Relay cursor stays valid across a size change, so no row
    is skipped or repeated. `query_for(paginated)` builds the document for one page.
    """
    after: str | None = None
    first = MAX_PAGE_SIZE

    while True:
        page_variables = {**variables, "first": first}
        if after is not None:
            page_variables["after"] = after

        try:
            nodes, page_info = await _execute(
                entity, log, http, query_for(after is not None), page_variables
            )
        except _QueryTooComplex:
            if first == 1:
                raise
            first = max(1, first // 2)
            log.warning(
                "Linear rejected a page as too complex; retrying with a smaller page",
                {"stream": entity.name, "page_size": first},
            )
            continue

        more = page_info.hasNextPage and page_info.endCursor is not None
        yield nodes, more

        if not more:
            return

        after = page_info.endCursor


async def _walk_pages(
    entity: type[LinearResource],
    log: Logger,
    http: HTTPSession,
    after_ts: datetime,
    through: datetime,
    *,
    archival: bool = False,
) -> AsyncGenerator[tuple[list[LinearResource], bool], None]:
    """Yield `(after_ts, through]` one page at a time, with whether another page follows.

    Lets a caller checkpoint between pages and know when the window has drained. The Relay
    cursor is used only within one walk and is never checkpointed; durable position is a
    timestamp.
    """
    async for page in _pages(
        entity,
        log,
        http,
        lambda paginated: _connection_query(entity, paginated=paginated, archival=archival),
        {"after_ts": _format_timestamp(after_ts), "through": _format_timestamp(through)},
    ):
        yield page


async def _walk(
    entity: type[LinearResource],
    log: Logger,
    http: HTTPSession,
    after_ts: datetime,
    through: datetime,
    *,
    archival: bool = False,
) -> AsyncGenerator[LinearResource, None]:
    """Yield every row in `(after_ts, through]`."""
    async for nodes, _ in _walk_pages(
        entity, log, http, after_ts, through, archival=archival
    ):
        for node in nodes:
            yield node


async def _fetch_changes(
    entity: type[LinearResource],
    log: Logger,
    http: HTTPSession,
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
    async for node in _walk(entity, log, http, log_cursor, horizon):
        yield node
        emitted = True

    if emitted:
        yield horizon


async def _fetch_issue_changes(
    log: Logger,
    http: HTTPSession,
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

    async for node in _walk(Issue, log, http, log_cursor, horizon):
        yield node
        emitted = True

    async for node in _walk(Issue, log, http, log_cursor, horizon, archival=True):
        yield node
        emitted = True

    if emitted:
        yield horizon


async def _backfill(
    entity: type[LinearResource],
    log: Logger,
    http: HTTPSession,
    start_date: datetime,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    """Emit historical rows below the cutoff, checkpointing a timestamp watermark per page.

    Guarantees the checkpoint never lands inside a tie group, and that resumption is
    deletion-proof: it resumes by cursor value rather than by position, so a row deleted
    mid-backfill renumbers nothing and cannot displace an untouched row past the resume
    point.

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

    The walk ascends, so a resume value raises `gt`. Archived rows need no special
    handling — `includeArchived` is always set and an archived row keeps the `updatedAt`
    of its last real edit.
    """
    assert isinstance(cutoff, datetime)

    # Backfill owns everything strictly below the cutoff; incremental owns the cutoff tick
    # onward, so the two meet with no gap and no overlap.
    through = floor_to_tick(cutoff) - TICK
    resume = datetime.fromisoformat(page) if isinstance(page, str) else None
    after_ts = resume if resume is not None else start_date

    if after_ts >= through:
        return

    # A cursor value is only safe to resume from once a DIFFERENT value has been seen:
    # ordering then proves every row sharing it was already emitted. Checkpointing the
    # last-seen value instead would split a tie group and permanently drop its remainder,
    # and Linear ties are common — a bulk edit stamps one timestamp across every row it
    # touches. A page that holds a single value therefore checkpoints nothing and the walk
    # continues, so a tie group wider than one page cannot stall it. The last page never
    # checkpoints: resuming from its boundary would re-emit its final tie group only to
    # observe an empty window.
    boundary: datetime | None = None
    previous: datetime | None = None

    async for nodes, more in _walk_pages(
        entity, log, http, after_ts, through
    ):
        for node in nodes:
            yield node

            current = node.get_cursor()
            if previous is not None and current != previous:
                boundary = previous
            previous = current

        if more and boundary is not None:
            yield boundary.isoformat()
            return

    # Falling out of the loop means the window drained, so the backfill is complete.
    # Returning without a cursor ends it; checkpointing here would only buy one more
    # invocation that observes an empty window.


# The CDK's fetch contracts bind these by name, one public pair per stream over the shared
# engine above.


async def fetch_issues(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_issue_changes(log, http, log_cursor):
        yield item


async def fetch_projects(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_changes(Project, log, http, log_cursor):
        yield item


async def fetch_initiatives(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[LinearResource | LogCursor, None]:
    async for item in _fetch_changes(Initiative, log, http, log_cursor):
        yield item


async def backfill_issues(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Issue, log, http, start_date, page, cutoff):
        yield item


async def backfill_projects(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Project, log, http, start_date, page, cutoff):
        yield item


async def backfill_initiatives(
    http: HTTPSession, start_date: datetime, log: Logger, page: PageCursor, cutoff: LogCursor
) -> AsyncGenerator[LinearResource | PageCursor, None]:
    async for item in _backfill(Initiative, log, http, start_date, page, cutoff):
        yield item


async def _snapshot(
    entity: type[_Entity], log: Logger, http: HTTPSession
) -> AsyncGenerator[_Entity, None]:
    """Yield every row of `entity`, archived ones included, regardless of age."""
    async for nodes, _ in _pages(
        entity,
        log,
        http,
        lambda paginated: _snapshot_query(entity, paginated=paginated),
        {},
    ):
        for node in nodes:
            yield node


async def snapshot_labels(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[IssueLabel, None]:
    async for item in _snapshot(IssueLabel, log, http):
        yield item
