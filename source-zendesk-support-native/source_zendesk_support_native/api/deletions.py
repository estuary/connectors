from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    DeletedTicket,
    DeletedTicketsResponse,
    DeletedUsersResponse,
    TimestampedResource,
)

from .incremental_keyset_paginated import fetch_incremental_keyset_paginated_window
from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    url_base,
)

# Deletions are read 30 minutes behind the present to give Zendesk time to
# settle which records are deleted.
DELETION_LAG = timedelta(minutes=30)

# Each run of the user deletion subtask reads every page of /deleted_users,
# and /deleted_tickets allows only 10 requests per minute. So a run only
# happens once its window covers an hour.
MIN_DELETION_WINDOW_SIZE = timedelta(hours=1)


async def fetch_deleted_users(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    """
    Yields a deletion document for each user deleted after `log_cursor` and at
    or before the horizon, then the horizon. A deleted user's `updated_at` is
    its deletion time.

              log_cursor      log_cursor + 1s       horizon
    ───────────────┼─────────────────┼──────────────────┼──────▶ time (1s ticks)
                   │                 │                  │
    listed ════════╪═════════════════╪══════════════════╪══════▶
    emitted ───────┼─────────────────[══════════════════]
                   │                 │                  └─ now - 30 minutes.
                   │                 │                     Later deletions wait
                   │                 │                     for a later run
                   │                 └─ first second emitted
                   └─ deletions at or before this second
                      were emitted by an earlier run

    /deleted_users can't filter or sort by time, so each run lists every
    deleted user and filters on `updated_at` here.
    """
    assert isinstance(log_cursor, datetime)

    horizon = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)

    if horizon - log_cursor < MIN_DELETION_WINDOW_SIZE:
        return

    url = f"{url_base(subdomain)}/deleted_users"

    params: dict[str, str | int] = {
        "sort": "id",
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
    }

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            "deleted_users.item",
            TimestampedResource,
            DeletedUsersResponse,
        )

        async for user in processor:
            if log_cursor < user.updated_at <= horizon:
                user.meta_ = TimestampedResource.Meta(op="d")
                yield user

        remainder = processor.get_remainder()

        if not remainder.meta.has_more or remainder.meta.after_cursor is None:
            break

        params["page[after]"] = remainder.meta.after_cursor

    yield horizon


async def fetch_deleted_tickets(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[DeletedTicket | LogCursor, None]:
    """
    Yields a deletion document for each ticket deleted after `log_cursor` and
    at or before the horizon, in ascending deletion order, then the horizon.
    Yielded datetimes before the horizon are the keyset sweep's checkpoints.

              log_cursor      log_cursor + 1s       horizon
    ───────────────┼─────────────────┼──────────────────┼──────▶ time (1s ticks)
                   │                 │                  │
    page[after] ───┼─────────────────[══════════════════╪══════▶
    emitted ───────┼─────────────────[══════════════════]
                   │                 │                  └─ now - 30 minutes.
                   │                 │                     Later deletions wait
                   │                 │                     for a later run
                   │                 └─ seek (log_cursor + 1s, 0)
                   └─ deletions at or before this second
                      were emitted by an earlier run

    /deleted_tickets only lists about 30 days of deletions, so a run whose
    window starts earlier than that misses the older ones.
    """
    assert isinstance(log_cursor, datetime)

    horizon = (datetime.now(tz=UTC) - DELETION_LAG).replace(microsecond=0)

    if horizon - log_cursor < MIN_DELETION_WINDOW_SIZE:
        return

    generator = fetch_incremental_keyset_paginated_window(
        http, subdomain, "deleted_tickets", "deleted_tickets", DeletedTicketsResponse, DeletedTicket, log_cursor, horizon, log,
    )

    async for result in generator:
        if isinstance(result, DeletedTicket):
            result.meta_ = DeletedTicket.Meta(op="d")
        yield result

    yield horizon
