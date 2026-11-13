from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    DeletedUsersResponse,
    TimestampedResource,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    url_base,
)

# Deletions are read 30 minutes behind the present to give Zendesk time to
# settle which records are deleted.
DELETION_LAG = timedelta(minutes=30)

# Each run of the user deletion subtask reads every page of /deleted_users, so
# a run only happens once its window covers an hour.
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
