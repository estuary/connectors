from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import (
    ApiResponse,
    RealUserMonitoringResource,
)

MAX_PAGE_SIZE = 100
CHECKPOINT_INTERVAL = 1_000
MIN_INCREMENTAL_WINDOW_SIZE = timedelta(minutes=1)


class DatadogWarningException(RuntimeError):
    def __init__(self, code: str, title: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.title = title
        self.detail = detail

    def __str__(self):
        return f"(Code: {self.code}) {self.title} - Detail: {self.detail}"


class DatadogWarningsAsException(RuntimeError):
    """
    Wraps multiple DatadogWarningException instances into a single exception.

    Note:
        This could be removed if the CDK and connector used python version ">=3.11" and
        use ExceptionGroup instead, which preserves the original exception types and stack traces.
    """

    def __init__(self, message: str, exceptions: list[DatadogWarningException]):
        super().__init__(message)
        self.exceptions = exceptions

    def __str__(self):
        lines = [super().__str__()]
        for exception in self.exceptions:
            lines.append(str(exception))
        return "\n".join(lines)


def check_response_for_partial_data(api_response: ApiResponse, log: Logger) -> None:
    """
    Datadog may return a list of warnings and a status of "timeout" in the API response if there were issues in the request.
    This may mean the returned data is a partial response, even though the HTTP status code would not indicate this.
    This will check for those warnings and status of timeout and raise an error instead of progressing, in case the data is incomplete.

    Note:
        Once more users start to use this connector in production, we may see that some warnings do not require failing the connector.
        However, until we can confirm that, we will keep this as is.
    """
    if api_response.meta.warnings:
        warnings_as_exceptions: list[DatadogWarningException] = []
        for warning in api_response.meta.warnings:
            warnings_as_exceptions.append(
                DatadogWarningException(
                    code=warning.code,
                    title=warning.title,
                    detail=warning.detail,
                )
            )

        raise DatadogWarningsAsException(
            "Multiple API warnings treated as errors", warnings_as_exceptions
        )


async def fetch_rum_page(
    http: HTTPSession,
    base_url: str,
    common_headers: dict[str, str],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[RealUserMonitoringResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    url = f"{base_url}/rum/events/search"

    # Note: Datadog's filter[from] is inclusive and filter[to] is exclusive
    request_body = {
        "filter": {
            "from": start_date.isoformat(),
            "to": cutoff.isoformat(),
        },
        "page": {
            "limit": MAX_PAGE_SIZE,
            "cursor": page,
        },
        "sort": "timestamp",
    }

    api_response = ApiResponse[list[RealUserMonitoringResource]].model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            headers=common_headers,
            json=request_body,
        )
    )

    check_response_for_partial_data(api_response, log)

    if api_response.data:
        for rum in api_response.data:
            if rum.attributes.timestamp < cutoff:
                yield rum

    if api_response.cursor:
        yield api_response.cursor


async def fetch_rum_changes(
    http: HTTPSession,
    base_url: str,
    common_headers: dict[str, str],
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[RealUserMonitoringResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{base_url}/rum/events/search"
    last_seen_ts = log_cursor
    max_window_size = timedelta(days=window_size)
    end = min(datetime.now(tz=UTC), log_cursor + max_window_size)

    # Return early if the end of the window is before the start date or if the
    # window is too small. Prevents sending many small API calls.
    if end < log_cursor or (end - log_cursor) < MIN_INCREMENTAL_WINDOW_SIZE:
        return

    # Note: Datadog's filter[from] is inclusive and filter[to] is exclusive
    request_body = {
        "filter": {
            "from": log_cursor.isoformat(),
            "to": end.isoformat(),
        },
        "page": {
            "limit": MAX_PAGE_SIZE,
            "cursor": None,
        },
        "sort": "timestamp",
    }

    log.debug("Fetching RUM events: ", {"request_body": request_body})

    count = 0
    total_count = 0
    while True:
        api_response = ApiResponse[
            list[RealUserMonitoringResource]
        ].model_validate_json(
            await http.request(
                log,
                url,
                method="POST",
                headers=common_headers,
                json=request_body,
            )
        )

        check_response_for_partial_data(api_response, log)

        if api_response.data:
            for rum in api_response.data:
                if rum.attributes.timestamp >= log_cursor:
                    if (
                        count >= CHECKPOINT_INTERVAL
                        and rum.attributes.timestamp > last_seen_ts
                    ):
                        yield last_seen_ts
                        count = 0

                    yield rum
                    count += 1
                    total_count += 1
                    last_seen_ts = rum.attributes.timestamp

        if not api_response.cursor:
            break

        request_body["page"]["cursor"] = api_response.cursor

    # Emit a final checkpoint if we saw any records
    if last_seen_ts > log_cursor and total_count > 0:
        yield last_seen_ts + timedelta(milliseconds=1)
    # If we didn't see any records in this window, only checkpoint if we've checked the entire date window.
    # The connector should reuse the same start on the next invocation but have a more recent end if we
    # haven't checked a maximum size window yet.
    elif (end - log_cursor) >= max_window_size:
        yield end
