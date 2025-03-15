from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
from urllib.parse import urljoin

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from .models import EndpointConfig, Event


# fetch_events gets the latest Events, starting with log_cursor and continuing to as close to the
# present time as the API will provide. The API will return an error if log_cursor is more than 24
# hours in the past. If the connector is unable to make requests and update the cursor for more than
# 24 hours, it will throw errors and require an update of the backfill counter to continue. This
# ensures consistency of data while the connector is in normal operation.
async def fetch_events(
    http: HTTPSession,
    config: EndpointConfig,
    entity: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Event | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    base = config.advanced.base_url if config.advanced.base_url else f"https://{config.organization}.gladly.com"
    url = urljoin(base, "api/v1/events")

    params = {
        "startAt": log_cursor.isoformat(),
        "entities": entity,
    }

    last_ts = log_cursor
    doc_count = 0
    async for line in http.request_lines(log, url, params=params):
        event = Event.model_validate_json(line)
        if event.timestamp < last_ts:
            # Events must be in ascending order with respect to time, so this is an application
            # logic error and should never happen.
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {event.timestamp} vs. prior timestamp {last_ts}"
            )

        doc_count += 1
        last_ts = event.timestamp

        event.meta_ = Event.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no events, we may need to move the cursor forward to keep it from going out
        # of date. The Gladly API docs state that Events may be delayed for up to 15 seconds, so we
        # set an upper boundary on how recent we will advance the cursor.
        last_ts = datetime.now(tz=UTC) - timedelta(minutes=5)

        if last_ts <= log_cursor:
            # Common case of a stream with somewhat frequent events where we have caught up and a
            # new invocation does not return any events, but the most recently fetched event is more
            # recent than our conservative time horizon for cursor recency.
            log.debug(
                f"not updating cursor since last_ts ({last_ts}) <= log_cursor ({log_cursor})"
            )
            return
        elif last_ts - log_cursor < timedelta(hours=6):
            # Only emit an updated checkpoint when there are no documents if the current cursor is
            # sufficiently old. Emitting a checkpoint with no documents will trigger an immediate
            # re-invocation of this task, so it shouldn't be done every time.
            log.debug(
                f"not updating cursor since updated cursor is less than 6 hours newer than prior cursor (updated cursor: {last_ts} vs prior cursor: {log_cursor})"
            )
            return

    else:
        # The Events API has millisecond precision for timestamps, so bump up the log cursor one
        # millisecond to not re-fetch the last event seen again on the next round. The assumption
        # here is that if _any_ events were retrieved with a specific timestamp, then _all_ events
        # having that timestamp were retrieved and further requests would not yield previously
        # unseen events with that timestamp.
        last_ts += timedelta(milliseconds=1)

    yield last_ts
