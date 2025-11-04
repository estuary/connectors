from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import Booking, ApiResponse
from .utils import dt_to_str


API_BASE_URL = "https://api.navan.com/v1"
DEFAULT_PAGE_SIZE = 100

BookingsResponse = ApiResponse[Booking]


async def _fetch_bookings_request(
    http: HTTPSession,
    log: Logger,
    page: int,
    page_size: int,
    updated_from: str | None = None,
    updated_to: str | None = None,
) -> BookingsResponse:
    url = f"{API_BASE_URL}/bookings"

    params: dict[str, int | str] = {
        "page": page,
        "pageSize": page_size,
    }

    if updated_from:
        params["updatedFrom"] = updated_from

    if updated_to:
        params["updatedTo"] = updated_to

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

    page = page_cursor if page_cursor else 1
    updated_to = dt_to_str(cutoff)

    log.debug(
        f"Fetching bookings page {page} with cutoff: {updated_to})",
    )

    response = await _fetch_bookings_request(
        http,
        log,
        page=page,
        page_size=DEFAULT_PAGE_SIZE,
        updated_to=updated_to,
    )

    for booking in response.data:
        yield booking

    if response.page and page < response.page.totalPages:
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

    updated_from = dt_to_str(log_cursor)
    updated_to = datetime.now(tz=log_cursor.tzinfo)

    log.debug(
        f"Fetching bookings changes in range {updated_from} to {dt_to_str(updated_to)}",
    )

    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        response = await _fetch_bookings_request(
            http,
            log,
            page=current_page,
            page_size=DEFAULT_PAGE_SIZE,
            updated_from=updated_from,
            updated_to=dt_to_str(updated_to),
        )

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
