from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import LogCursor
from pydantic import AwareDatetime


from .models import Event


async def fetch_events(
    http: HTTPSession,
    organization: str,
    entity: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Event | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{organization}.gladly.com/api/v1/events"
    params = {
        "startAt": log_cursor.isoformat(),
        "entities": entity,
    }

    max_ts = log_cursor
    async for line in http.request_lines(log, url, params=params):
        event = Event.model_validate_json(line)
        ts = datetime.fromisoformat(event.timestamp)
        if ts > max_ts:
            max_ts = ts
        event.meta_ = Event.Meta(op="c")
        yield event

    yield max_ts
