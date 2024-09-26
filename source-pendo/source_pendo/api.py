from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
import pendulum
from pydantic import TypeAdapter
from estuary_cdk.capture.common import BaseDocument, LogCursor
from estuary_cdk.http import HTTPSession

from .models import PageEvent, FeatureEvent, TrackEvent, GuideEvent, PollEvent, AggregatedEventResponse,EventResponse, Resource, Metadata

API = "https://app.pendo.io/api/v1"
RESPONSE_LIMIT = 25000


def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


async def fetch_resources(
        http: HTTPSession,
        entity: str,
        log: Logger,
) -> AsyncGenerator[Resource, None]:
    url = f"{API}/{entity}"

    resources = TypeAdapter(list[Resource]).validate_json(await http.request(log, url))

    for resource in resources:
        yield resource


async def fetch_metadata(
        http: HTTPSession,
        entity: str,
        log: Logger,
) -> AsyncGenerator[Metadata, None]:
    url = f"{API}/metadata/schema/{entity}"

    metadata = Metadata.model_validate_json(
        await http.request(log, url)
    )

    yield metadata


async def fetch_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[GuideEvent | PollEvent | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    url = f"{API}/aggregation"
    last_dt = log_cursor
    last_ts = _dt_to_ms(last_dt)

    # Pendo's event aggregation endpoints require a JSON body with various fields.
    # See https://engageapi.pendo.io/#f42c416f-bdd6-4082-b86c-1be3fe8401bf. We use
    # their filtering and sorting capabilities to only return documents we haven't
    # seen yet in ascending order. Each document returned represents a single event.
    body = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "pipeline": [
                {
                    "source": {
                        entity: None,
                        "timeSeries": {
                            "period": "hourRange",
                            "first": f"{last_ts}",
                            "last": "now()"
                        }
                    }
                },
                # Only return events that have occurred since the last event we've seen.
                {
                    "filter": f"guideTimestamp > {last_ts}"
                },
                {
                    "sort": ["guideTimestamp"]
                },
                {
                    "limit": RESPONSE_LIMIT
                }
            ]
        }
    }

    response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    events = response.results

    doc_count = 0
    for event in events:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if event.guideTimestamp < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event date is {event.guideTimestamp} vs. prior date {last_dt}"
            )

        doc_count += 1
        last_dt = event.guideTimestamp

        event.meta_ = model.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    
    # If there was at least one document, update the cursor.
    yield last_dt


async def fetch_aggregated_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[PageEvent | FeatureEvent | TrackEvent | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    url = f"{API}/aggregation"
    last_dt = log_cursor
    last_ts = _dt_to_ms(last_dt)

    # current_hour is used to only get aggregates for the past hour. We add a 15 minute delay
    # when determining the previous hour in an attempt to avoid issues due to distributed clocks.
    current_hour = pendulum.now("UTC").subtract(minutes=15).set(minute=0, second=0).int_timestamp * 1000

    # Pendo's event aggregation endpoints require a JSON body with various fields.
    # See https://engageapi.pendo.io/#f42c416f-bdd6-4082-b86c-1be3fe8401bf. We use
    # their filtering and sorting capabilities to only return documents we haven't
    # seen yet in ascending order. Each document returned represents an aggregation 
    # of events in a specific hour.
    body = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "pipeline": [
                {
                    "source": {
                        entity: None,
                        "timeSeries": {
                            "period": "hourRange",
                            "first": f"{last_ts}",
                            "last": "now()"
                        }
                    }
                },
                # Only return aggregates that have been updated since the last aggregate 
                # we've seen AND aren't "in-progress" during the current hour.
                {
                    "filter": f"lastTime > {last_ts} && hour < {current_hour}"
                },
                {
                    "sort": ["lastTime"]
                },
                {
                    "limit": RESPONSE_LIMIT
                }
            ]
        }
    }

    response = AggregatedEventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    aggregates = response.results

    doc_count = 0
    for aggregate in aggregates:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if aggregate.lastTime < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {aggregate.lastTime} vs. prior timestamp {last_dt}"
            )

        doc_count += 1
        last_dt = aggregate.lastTime

        aggregate.meta_ = model.Meta(op="c")
        yield aggregate

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    
    # If there was at least one document, update the cursor.
    yield last_dt
