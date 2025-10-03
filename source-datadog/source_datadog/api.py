from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import (
    ApiResponse,
    IncrementalResource,
)

TResourceType = TypeVar("TResourceType", bound=IncrementalResource)

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


def check_response_for_partial_data(
    api_response: ApiResponse, log: Logger
) -> None:
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

        raise ExceptionGroup(
            "Multiple API warnings treated as errors", warnings_as_exceptions
        )


def _build_search_request_body(
    start_date: datetime,
    end_date: datetime,
    page_cursor: str | None = None,
    extra_filter_params: dict | None = None,
) -> dict:
    filter_params = {
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
    }

    if extra_filter_params:
        filter_params.update(extra_filter_params)

    return {
        "filter": filter_params,
        "page": {
            "limit": MAX_PAGE_SIZE,
            "cursor": page_cursor,
        },
        "sort": "timestamp",
    }


async def fetch_events_page(
    http: HTTPSession,
    base_url: str,
    common_headers: dict[str, str],
    resource_type: type[TResourceType],
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[TResourceType | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    url = f"{base_url}/{resource_type.PATH}"
    request_body = _build_search_request_body(start_date, cutoff, page)

    api_response = ApiResponse[list[resource_type]].model_validate_json(
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
        for resource in api_response.data:
            if resource.attributes.timestamp < cutoff:
                yield resource

    if api_response.cursor:
        yield api_response.cursor
    else:
        return


async def fetch_events_changes(
    http: HTTPSession,
    base_url: str,
    common_headers: dict[str, str],
    resource_type: type[TResourceType],
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
    extra_filter_params: dict | None = None,
) -> AsyncGenerator[TResourceType | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{base_url}/{resource_type.PATH}"
    last_seen_ts = log_cursor
    max_window_size = timedelta(days=window_size)
    end = min(datetime.now(tz=UTC), log_cursor + max_window_size)

    # Return early if the window is too small. Prevents sending many small API calls.
    if (end - log_cursor) < MIN_INCREMENTAL_WINDOW_SIZE:
        return

    request_body = _build_search_request_body(
        log_cursor, end, None, extra_filter_params
    )

    count = 0
    while True:
        api_response = ApiResponse[list[resource_type]].model_validate_json(
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
            for resource in api_response.data:
                if resource.attributes.timestamp >= log_cursor:
                    if (
                        count >= CHECKPOINT_INTERVAL
                        and resource.attributes.timestamp > last_seen_ts
                    ):
                        yield last_seen_ts
                        count = 0

                    yield resource
                    count += 1
                    last_seen_ts = resource.attributes.timestamp

        if not api_response.cursor:
            break

        request_body["page"]["cursor"] = api_response.cursor

    # Emit a final checkpoint if we saw any records
    if count > 0:
        yield last_seen_ts + timedelta(milliseconds=1)
    # If we didn't see any records in this window, only checkpoint if we've checked the entire date window.
    # The connector should reuse the same start on the next invocation but have a more recent end if we
    # haven't checked a maximum size window yet.
    elif (end - log_cursor) >= max_window_size:
        yield end
