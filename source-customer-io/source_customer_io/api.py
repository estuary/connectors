from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import (
    ConfigObject,
    DeliveriesResponse,
    Delivery,
    PaginatedObject,
    SnapshotObject,
    SnapshotPage,
)

# Customer.io App API base URLs. Every endpoint carries a `/v1/` path prefix that is
# *not* part of the base URL, so per-endpoint paths look like
# f"{base_url(config.region)}/v1/<path>".
API = "https://api.customer.io"
API_EU = "https://api-eu.customer.io"

# Keys mirror EndpointConfig.region's Literal values. Customer.io does not route
# App API requests across regions -- the wrong host returns 401, not a redirect --
# and the region is not derivable from an App API Key, so it must come from config.
REGION_BASE_URLS = {
    "us": API,
    "eu": API_EU,
}


def base_url(region: str) -> str:
    return REGION_BASE_URLS[region]


# `limit` above this is accepted rather than rejected, so the ceiling is ours to
# enforce. See the `limit over max` request in the Bruno collection.
MAX_PAGE_SIZE = 1000

# `/v1/messages` serves at most six months of history. Requests for a wider range
# return 200 with a narrowed result set and no indication they were narrowed, so
# the connector clamps its own floor and says so.
MAX_HISTORY = timedelta(days=183)

# An out-of-range `start` token makes the API restart the walk at page one and
# hand back page one's `next`, which is a live cycle. The connector never injects
# a stale token, so normal operation cannot enter it; this bounds a provider-side
# bug rather than an expected case.
MAX_PAGES_PER_WINDOW = 10_000


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def backfill_floor(start_date: datetime, log: Logger) -> datetime:
    """The earliest instant `/v1/messages` will serve, given a configured start date."""
    clamp = datetime.now(tz=UTC) - MAX_HISTORY
    if start_date < clamp:
        log.warning(
            "start_date predates Customer.io's six-month delivery history limit; "
            "backfilling from the limit instead.",
            {"start_date": start_date, "earliest_available": clamp},
        )
        return clamp
    return start_date


async def _drain_window(
    http: HTTPSession,
    base: str,
    log: Logger,
    start: datetime,
    end: datetime,
) -> AsyncGenerator[Delivery, None]:
    """Yield every delivery created within [start, end], following pagination to the end.

    Both bounds are inclusive. The continuation token is local to this call: it is
    a positional offset the provider does not guarantee across a gap, so it is
    never checkpointed.
    """
    url = f"{base}{Delivery.PATH}"
    params: dict[str, str | int] = {
        "limit": MAX_PAGE_SIZE,
        Delivery.SINCE_PARAM: _dt_to_ts(start),
        Delivery.BEFORE_PARAM: _dt_to_ts(end),
        # Free to request: the response is otherwise identical, and in-app survey
        # responses are picked up where they exist.
        "get_tracked_responses": "true",
    }

    for page in range(MAX_PAGES_PER_WINDOW):
        response = DeliveriesResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        for delivery in response.messages:
            yield delivery

        if not response.next:
            return

        params = {**params, "start": response.next}
    else:
        raise RuntimeError(
            f"Pagination did not terminate within {MAX_PAGES_PER_WINDOW} pages "
            f"for window [{start}, {end}]."
        )


async def fetch_deliveries(
    http: HTTPSession,
    base: str,
    lag: timedelta,
    window_size: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Delivery | LogCursor, None]:
    """Incrementally fetch deliveries by creation time, trailing the present by `lag`.

                 cursor   cursor + 1s  horizon = last elapsed second - lag
    ────────────────┼──────────┼──────────────┼─────▶ time (1s ticks)
                    │          │              │
    start_ts ───────┼──────────[══════════════╪═════▶
    end_ts ═════════╪══════════╪══════════════]
    emitted ────────┼──────────[══════════════]
                    │          │              └─ the present second is still
                    │          │                 in progress; its deliveries
                    │          │                 wait for the next poll
                    │          └─ first emitted second
                    └─ already emitted by the previous poll
    Both bounds inclusive, so start_ts is cursor + 1s.

    Bound twice per binding: once at lag=0 to tail new deliveries, and once
    trailing by the re-scan window to re-read them after their metrics settle.
    """
    assert isinstance(log_cursor, datetime)

    # The last fully-elapsed second, less this subtask's lag.
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1) - lag
    if horizon <= log_cursor:
        return

    start = log_cursor + timedelta(seconds=1)
    end = min(start + window_size, horizon)

    async for delivery in _drain_window(http, base, log, start, end):
        yield delivery

    # Advance even when the window was empty. Every tick in it had fully elapsed
    # and was filtered server-side, so nothing can still arrive in it -- and
    # because the window is capped at `window_size`, holding the cursor back
    # would leave it stranded behind a quiet stretch longer than one window.
    yield end


async def backfill_deliveries(
    http: HTTPSession,
    base: str,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: datetime,
) -> AsyncGenerator[Delivery | PageCursor, None]:
    """Walk historical deliveries in fixed forward windows up to the cutoff.

       window_start  window_start + W    cutoff - 1s    cutoff
    ────────┼──────────────┼──────────────────┼────────────┼───▶ time (1s ticks)
            │              │                  │            │
    start_ts[══════════════╪══════════════════╪════════════╪──▶
    end_ts ═╪══════════════]                  │            │
    emitted [══════════════]                  │            │
            │              │                  │            └─ incremental's
            │              │                  │               first tick
            │              │                  └─ backfill's last tick
            │              └─ end of this window, inclusive
            └─ resume point, carried as an RFC3339 PageCursor
    Both bounds inclusive. One window drains per invocation.

    The resume key is a timestamp rather than a position: `/v1/messages` exposes
    no sort parameter, and an elapsed window is a frozen set because `created`
    is immutable, so the walk is correct without depending on result order.
    """
    assert isinstance(page, str)

    window_start = datetime.fromisoformat(page)
    if window_start >= cutoff:
        return

    window_end = min(window_start + window_size, cutoff) - timedelta(seconds=1)

    async for delivery in _drain_window(http, base, log, window_start, window_end):
        yield delivery

    yield (window_end + timedelta(seconds=1)).isoformat()


def _ordered(
    model: type[SnapshotObject], items: list[dict[str, Any]], log: Logger
) -> list[dict[str, Any]]:
    """Sort rows by the model's ordering key, or warn and leave them alone.

    Snapshot rows are addressed positionally, so a stable order is what keeps an
    unchanged row on the same key between passes. The provider documents no
    ordering of its own.
    """
    if not items:
        return items

    if not all(model.ORDER_KEY in item for item in items):
        log.warning(
            "Not every row carries the ordering key; emitting them in the "
            "provider's order instead.",
            {"resource": model.NAME, "order_key": model.ORDER_KEY},
        )
        return items

    try:
        return sorted(items, key=lambda item: item[model.ORDER_KEY])
    except TypeError:
        log.warning(
            "Rows carry mixed types in their ordering key; emitting them in "
            "the provider's order instead.",
            {"resource": model.NAME, "order_key": model.ORDER_KEY},
        )
        return items


async def snapshot_config_objects(
    http: HTTPSession,
    base: str,
    model: type[ConfigObject],
    log: Logger,
) -> AsyncGenerator[ConfigObject, None]:
    """Yield every row of one configuration endpoint.

    These endpoints take no parameters and return the whole collection in a
    single response, so each pass is a complete snapshot.

    Rows are emitted in `ORDER_KEY` order where every row carries it. The
    provider documents no ordering, and snapshot bindings address rows
    positionally, so sorting is what stops an unchanged row from moving to a
    different key when the provider reorders its response.
    """
    url = f"{base}{model.PATH}"
    page = SnapshotPage.model_validate_json(await http.request(log, url))

    for item in _ordered(model, page.items(model.ITEMS_KEY), log):
        yield model.model_validate(item)


async def snapshot_paginated_objects(
    http: HTTPSession,
    base: str,
    model: type[PaginatedObject],
    log: Logger,
) -> AsyncGenerator[PaginatedObject, None]:
    """Yield every row of one paginated endpoint, walking `start` to the end.

    The whole result set is collected before anything is emitted, because
    ordering has to be stable across the entire snapshot rather than within a
    page -- rows are addressed positionally, so a row that moves between pages
    would otherwise land on a different key.

    The continuation token stays inside this call. It is a positional offset the
    provider makes no promises about across a gap, so it is never checkpointed.
    """
    url = f"{base}{model.PATH}"
    request_params: dict[str, str | int] = {
        "limit": MAX_PAGE_SIZE,
        **model.EXTRA_PARAMS,
    }

    items: list[dict[str, Any]] = []
    for _ in range(MAX_PAGES_PER_WINDOW):
        page = SnapshotPage.model_validate_json(
            await http.request(log, url, params=request_params)
        )
        items.extend(page.items(model.ITEMS_KEY))

        # Three terminal conventions exist across this API: an empty string, an
        # absent key, and a populated token. Only a falsiness test covers all.
        if not page.next:
            break

        request_params = {**request_params, "start": page.next}
    else:
        raise RuntimeError(
            f"Pagination did not terminate within {MAX_PAGES_PER_WINDOW} pages "
            f"for {model.NAME}."
        )

    for item in _ordered(model, items, log):
        yield model.model_validate(item)
