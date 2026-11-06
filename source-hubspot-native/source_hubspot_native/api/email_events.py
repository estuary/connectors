from datetime import datetime, timedelta
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Dict,
)

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from ..models import (
    EmailEvent,
    EmailEventsResponse,
    TimestampedObject,
)
from .shared import (
    dt_to_ms,
    HUB,
)

async def fetch_email_events_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[EmailEvent | PageCursor, None]:

    assert isinstance(cutoff, datetime)

    url = f"{HUB}/email/public/v1/events"
    input: Dict[str, Any] = {
        "endTimestamp": dt_to_ms(cutoff) - 1,  # endTimestamp is inclusive.
        "limit": 1000,
    }
    if page:
        input["offset"] = page

    result = EmailEventsResponse.model_validate_json(
        await http.request(log, url, params=input)
    )

    for event in result.events:
        yield event

    if result.hasMore:
        yield result.offset


async def _fetch_email_events(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[TimestampedObject[EmailEvent], None]:
    url = f"{HUB}/email/public/v1/events"

    input: Dict[str, Any] = {
        "startTimestamp": dt_to_ms(since),
        "limit": 1000,
    }
    if until:
        input["endTimestamp"] = dt_to_ms(until)

    while True:
        result = EmailEventsResponse.model_validate_json(
            await http.request(log, url, params=input)
        )

        for event in result.events:
            yield TimestampedObject(event.created, event.id, event)

        if not result.hasMore:
            break

        input["offset"] = result.offset


def fetch_recent_email_events(
    log: Logger, http: HTTPSession, _: bool, since: datetime, until: datetime | None
) -> AsyncGenerator[TimestampedObject[EmailEvent], None]:

    return _fetch_email_events(log, http, since + timedelta(milliseconds=1), until)


def fetch_delayed_email_events(
    log: Logger, http: HTTPSession, _: bool, since: datetime, until: datetime
) -> AsyncGenerator[TimestampedObject[EmailEvent], None]:

    return _fetch_email_events(log, http, since, until)
