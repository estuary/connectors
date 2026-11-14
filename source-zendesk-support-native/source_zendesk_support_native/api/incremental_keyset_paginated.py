import base64
from datetime import datetime, UTC
import json
from logging import Logger
import struct
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    IncrementalKeysetPaginatedResponse,
    IncrementalResource,
    TimestampedResource,
)

from .shared import (
    CHECKPOINT_INTERVAL,
    CURSOR_PAGINATION_PAGE_SIZE,
    INCREMENTAL_LAG,
    MIN_INCREMENTAL_WINDOW_SIZE,
    url_base,
    dt_to_s,
    s_to_dt,
)


def _build_seek_cursor(lower_bound: datetime) -> str:
    # Zendesk's list cursors are undocumented keysets, and Zendesk accepts ones we
    # build. Each is base64 JSON whose "v" holds every sort field as a type byte and
    # a little-endian int64, with "d" for epoch-seconds updated_at and "i" for id.
    # An id of 0 starts at the first record updated in the second after `lower_bound`.
    keyset = b"d" + struct.pack("<q", dt_to_s(lower_bound) + 1) + b"i" + struct.pack("<q", 0)
    cursor = {"o": "updated_at,id", "v": base64.b64encode(keyset).decode()}
    return base64.b64encode(json.dumps(cursor, separators=(",", ":")).encode()).decode()


async def fetch_incremental_keyset_paginated_window[ResourceT: IncrementalResource](
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalKeysetPaginatedResponse],
    resource_model: type[ResourceT],
    lower_bound: datetime,
    upper_bound: datetime,
    log: Logger,
) -> AsyncGenerator[ResourceT | datetime, None]:
    """
    Yields the records whose cursor is after `lower_bound` and at or before
    `upper_bound`, in ascending cursor order. A yielded datetime means every
    record with a cursor at or before it has been yielded.

              lower_bound    lower_bound + 1s      upper_bound
    ───────────────┼─────────────────┼──────────────────┼──────▶ time (1s ticks)
                   │                 │                  │
    page[after] ───┼─────────────────[══════════════════╪══════▶
    emitted ───────┼─────────────────[══════════════════]
                   │                 │                  └─ the first record past
                   │                 │                     this ends the sweep
                   │                 └─ seek (lower_bound + 1s, 0)
                   └─ every record with a cursor at or before
                      this second was already emitted

    Checkpoints only fall between seconds, so a resumed sweep never needs a
    cursor partway through a second.
    """
    url = f"{url_base(subdomain)}/{path}"

    params: dict[str, str | int] = {
        "sort": "updated_at",
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
        "page[after]": _build_seek_cursor(lower_bound),
        **response_model.REQUEST_PARAMS,
    }

    last_seen_dt = lower_bound
    count = 0

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            f"{name}.item",
            resource_model,
            response_model,
        )

        async for resource in processor:
            cursor = resource.get_cursor()

            if cursor > upper_bound:
                return

            if cursor <= lower_bound or cursor < last_seen_dt:
                raise RuntimeError(f"Received {name} record {resource.id} with cursor {cursor} out of order after {last_seen_dt}.")

            if (
                cursor > last_seen_dt and
                last_seen_dt != lower_bound and
                count >= CHECKPOINT_INTERVAL
            ):
                yield last_seen_dt
                count = 0

            yield resource
            count += 1
            last_seen_dt = cursor

        remainder = processor.get_remainder()

        if not remainder.meta.has_more or remainder.meta.after_cursor is None:
            return

        params["page[after]"] = remainder.meta.after_cursor


async def fetch_incremental_keyset_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalKeysetPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    horizon = (datetime.now(tz=UTC) - INCREMENTAL_LAG).replace(microsecond=0)

    if horizon - log_cursor < MIN_INCREMENTAL_WINDOW_SIZE:
        return

    generator = fetch_incremental_keyset_paginated_window(
        http, subdomain, name, path, response_model, TimestampedResource, log_cursor, horizon, log,
    )

    last_seen_dt: datetime | None = None

    async for result in generator:
        if isinstance(result, TimestampedResource):
            last_seen_dt = result.get_cursor()
        yield result

    if last_seen_dt is not None:
        yield last_seen_dt


async def backfill_incremental_keyset_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalKeysetPaginatedResponse],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is None:
        lower_bound = start_date
    else:
        assert isinstance(page, int)
        lower_bound = s_to_dt(page)

    generator = fetch_incremental_keyset_paginated_window(
        http, subdomain, name, path, response_model, TimestampedResource, lower_bound, cutoff, log,
    )

    async for result in generator:
        if isinstance(result, datetime):
            yield dt_to_s(result)
        else:
            yield result
