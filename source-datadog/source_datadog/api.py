from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
import json

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPMixin

from .models import EndpointConfig, RUMEvent

async def fetch_rum_events(
    http: HTTPMixin,
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[RUMEvent | LogCursor, None]:
    """Fetch RUM events from Datadog API starting from the given cursor."""
    assert isinstance(log_cursor, datetime)

    # Use the configured site for the API endpoint
    base_url = f"https://api.{config.site}"
    url = f"{base_url}/api/v2/rum/events/search"

    # Prepare the request body
    body = {
        "filter": {
            "from": log_cursor.isoformat(),
            "to": datetime.now(UTC).isoformat(),
            "query": "*"  # Fetch all events, can be customized based on needs
        },
        "options": {
            "timezone": "UTC"
        },
        "page": {
            "limit": 100  # Maximum allowed by the API
        },
        "sort": "timestamp"
    }

    headers = {
        "DD-API-KEY": config.api_key,
        "DD-APPLICATION-KEY": config.app_key,
        "Content-Type": "application/json"
    }

    last_ts = log_cursor
    doc_count = 0

    while True:
        try:
            response = await http.request(log, url, method="POST", json=body, headers=headers)
            response_data = json.loads(response) if isinstance(response, bytes) else response
            
            if not response_data or not response_data.get("data"):
                break

            for event_data in response_data["data"]:
                # Convert the event data to our RUMEvent model
                event = RUMEvent(
                    id=event_data["id"],
                    type=event_data["type"],
                    timestamp=datetime.fromisoformat(event_data["attributes"]["timestamp"].replace("Z", "+00:00")),
                    attributes=event_data["attributes"],
                    relationships=event_data.get("relationships") or {}
                )

                if event.timestamp < last_ts:
                    raise RuntimeError(
                        f"Received events out of time order: Current event timestamp is {event.timestamp} vs. prior timestamp {last_ts}"
                    )

                doc_count += 1
                last_ts = event.timestamp
                event.meta_ = RUMEvent.Meta(op="c")
                yield event

            # Check if there are more pages
            if not response_data.get("meta", {}).get("page", {}).get("has_more"):
                break

            # Update the cursor for the next page
            body["page"]["cursor"] = response_data["meta"]["page"]["next_cursor"]

        except Exception as e:
            log.error(f"Error fetching RUM events: {str(e)}")
            raise

    if doc_count == 0:
        # If no events were found, advance the cursor to keep it from going stale
        last_ts = datetime.now(UTC) - timedelta(minutes=5)

        if last_ts <= log_cursor:
            log.debug(
                f"not updating cursor since last_ts ({last_ts}) <= log_cursor ({log_cursor})"
            )
            return
        elif last_ts - log_cursor < timedelta(hours=6):
            log.debug(
                f"not updating cursor since updated cursor is less than 6 hours newer than prior cursor (updated cursor: {last_ts} vs prior cursor: {log_cursor})"
            )
            return
    else:
        # Advance the cursor by 1 millisecond to avoid re-fetching the last event
        last_ts += timedelta(milliseconds=1)

    yield last_ts 