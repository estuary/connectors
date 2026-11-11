from datetime import UTC, datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import Booking, ApiResponse
from .utils import dt_to_str


API_BASE_URL = "https://api.navan.com/v1"
DEFAULT_PAGE_SIZE = 100
NAVAN_FOUNDING_YEAR = 2015

BookingsResponse = ApiResponse[Booking]


async def _fetch_bookings_request(
    http: HTTPSession,
    log: Logger,
    page: int,
    page_size: int,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    updated_from: datetime | None = None,
    updated_to: datetime | None = None,
) -> BookingsResponse:
    url = f"{API_BASE_URL}/bookings"

    params: dict[str, int] = {
        "page": page,
        "size": page_size,
    }

    if created_from is not None:
        params["createdFrom"] = int(created_from.timestamp())

    if created_to is not None:
        params["createdTo"] = int(created_to.timestamp())

    if updated_from is not None:
        params["updatedFrom"] = int(updated_from.timestamp())

    if updated_to is not None:
        params["updatedTo"] = int(updated_to.timestamp())

    return BookingsResponse.model_validate_json(
        await http.request(log, url, params=params)
    )


async def fetch_bookings_page(
    http: HTTPSession,
    log: Logger,
    page_cursor: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Booking | PageCursor, None]:
    assert page_cursor is None or isinstance(page_cursor, int), (
        f"Expected int page_cursor or None, got {type(page_cursor)}"
    )
    assert isinstance(cutoff, datetime), f"Expected datetime cutoff, got {type(cutoff)}"

    page = page_cursor or 0

    log.debug(
        f"Fetching bookings page {page} with cutoff: {dt_to_str(cutoff)})",
    )

    # A booking's `created` never changes, so bounding by it means an update
    # can't remove a booking from the results and shift later pages. Navan
    # rejects `updatedTo` without `updatedFrom`, so we set both ends of the
    # `created` range.
    response = await _fetch_bookings_request(
        http,
        log,
        page=page,
        page_size=DEFAULT_PAGE_SIZE,
        created_from=datetime(NAVAN_FOUNDING_YEAR, 1, 1, tzinfo=UTC),
        created_to=cutoff,
    )

    for booking in response.data:
        yield booking

    if response.page and page + 1 < response.page.totalPages:
        next_page = page + 1
        log.debug(
            f"Page {page} of {response.page.totalPages} complete, continuing to next page",
            extra={
                "current_page": page,
                "total_pages": response.page.totalPages,
                "records_in_page": len(response.data),
            },
        )
        yield next_page


async def fetch_bookings_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Booking | LogCursor, None]:
    assert isinstance(log_cursor, datetime), (
        f"Expected datetime cursor, got {type(log_cursor)}"
    )

    last_seen_modified = log_cursor
    cursor_timestamp = log_cursor

    updated_to = datetime.now(tz=log_cursor.tzinfo)

    log.debug(
        f"Fetching bookings changes in range {dt_to_str(log_cursor)} to {dt_to_str(updated_to)}",
    )

    current_page = 0
    total_pages = 1

    while current_page < total_pages:
        response = await _fetch_bookings_request(
            http,
            log,
            page=current_page,
            page_size=DEFAULT_PAGE_SIZE,
            updated_from=log_cursor,
            updated_to=updated_to,
        )

        if response.page:
            total_pages = response.page.totalPages

        for booking in response.data:
            modified_ts = booking.lastModified

            if modified_ts > last_seen_modified:
                last_seen_modified = modified_ts

            if modified_ts > cursor_timestamp:
                yield booking

        current_page += 1

    if last_seen_modified > cursor_timestamp:
        log.debug(
            f"Cursor updated from {cursor_timestamp} to {last_seen_modified}",
            extra={
                "old_cursor": dt_to_str(cursor_timestamp),
                "new_cursor": dt_to_str(last_seen_modified),
            },
        )
        yield last_seen_modified
