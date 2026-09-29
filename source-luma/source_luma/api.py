from collections.abc import AsyncGenerator
from logging import Logger
from urllib.parse import urlencode

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import (
    Calendar,
    Event,
    EventContext,
    Guest,
    LumaResource,
    PageMeta,
)

API = "https://public-api.luma.com"
# Luma authenticates with a bare key in a custom header rather than a Bearer token.
AUTHORIZATION_HEADER = "x-luma-api-key"
# Luma documents no page-size ceiling ("the server will enforce a maximum
# number"); an over-limit request is accepted silently, and every walk follows
# `has_more`, so a server-side clamp only costs extra requests.
PAGE_SIZE = 100
LUMA_PLATFORM = "luma"


class Self(BaseModel, extra="allow"):
    id: str
    email: str


async def fetch_self(http: HTTPSession, log: Logger) -> Self:
    """The cheapest authenticated call in the API; doubles as the auth probe."""
    return Self.model_validate_json(await http.request(log, f"{API}/v1/users/get-self"))


async def _list_pages[T: LumaResource](
    http: HTTPSession,
    model: type[T],
    request_params: dict[str, str | list[str]],
    log: Logger,
    validation_context: object | None = None,
) -> AsyncGenerator[T, None]:
    """Yield every item of a cursor-paginated Luma list, following `has_more`
    until the walk is exhausted. `next_cursor` never outlives this call."""
    cursor: str | None = None

    while True:
        params: dict[str, str | list[str]] = {
            **request_params,
            "pagination_limit": str(PAGE_SIZE),
        }
        if cursor:
            params["pagination_cursor"] = cursor

        # Array filters are repeated query params (`platforms=luma&platforms=external`),
        # which the CDK's dict-shaped `params` cannot express.
        url = f"{API}{model.PATH}?{urlencode(params, doseq=True)}"
        _, body = await http.request_stream(log, url)
        processor = IncrementalJsonProcessor(
            body(),
            "entries.item",
            model,
            remainder_cls=PageMeta,
            validation_context=validation_context,
        )

        async for item in processor:
            yield item

        meta = processor.get_remainder()
        if not meta.has_more or not meta.next_cursor:
            return

        cursor = meta.next_cursor


async def snapshot_calendars(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Calendar, None]:
    """The one calendar this key is scoped to, as a single-document snapshot."""
    yield Calendar.model_validate_json(await http.request(log, f"{API}{Calendar.PATH}"))


async def snapshot_events(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Event, None]:
    """Every event the calendar manages, across all submission statuses."""
    for status in Event.STATUS_SWEEPS:
        async for event in _list_pages(
            http, Event, {**Event.REQUEST_PARAMS, "status": status}, log
        ):
            yield event


async def snapshot_guests(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Guest, None]:
    """Every guest of every event the calendar manages.

    Guests can only be listed per event, so the sweep costs one request per
    event plus the event listing itself, every interval. Deletions and status
    changes are observed because the whole set is re-listed.

    External events (hosted off Luma, `calev-` ids) have no guest list and
    answer 403 when asked, so only Luma-hosted events fan out.
    """
    event_ids = [
        event.id
        async for event in snapshot_events(http, log)
        if event.platform == LUMA_PLATFORM
    ]

    for event_id in event_ids:
        async for guest in _list_pages(
            http,
            Guest,
            {**Guest.REQUEST_PARAMS, "event_id": event_id},
            log,
            validation_context=EventContext(event_id=event_id),
        ):
            yield guest


async def snapshot_list_resource[T: LumaResource](
    model: type[T], http: HTTPSession, log: Logger
) -> AsyncGenerator[T, None]:
    """Snapshot for a stream that is one plain paginated list of itself."""
    async for item in _list_pages(http, model, model.REQUEST_PARAMS, log):
        yield item
