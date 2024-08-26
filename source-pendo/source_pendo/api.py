import json
from logging import Logger
from typing import AsyncGenerator, Iterable
import pendulum
from pydantic import TypeAdapter
from estuary_cdk.capture.common import BaseDocument, LogCursor
from estuary_cdk.http import HTTPSession

from .models import PageEvent, FeatureEvent, TrackEvent, GuideEvent, PollEvent, AggregatedEventResponse,EventResponse, Resource, Metadata

API = "https://app.pendo.io/api/v1"
RESPONSE_LIMIT = 25000

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
    assert isinstance(log_cursor, int)
    url = f"{API}/aggregation"
    last_ts = log_cursor

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
        updated_at = getattr(event, "guideTimestamp", -1)
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if updated_at < last_ts:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {updated_at} vs. prior timestamp {last_ts}"
            )

        doc_count += 1
        last_ts = updated_at

        event.meta_ = model.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    
    # If there was at least one document, update the cursor.
    yield last_ts


async def fetch_aggregated_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[PageEvent | FeatureEvent | TrackEvent | GuideEvent | PollEvent | LogCursor, None]:
    assert isinstance(log_cursor, int)
    url = f"{API}/aggregation"
    last_ts = log_cursor
    current_hour = pendulum.now("UTC").set(minute=0, second=0).int_timestamp * 1000

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
    events = response.results

    doc_count = 0
    for event in events:
        updated_at = getattr(event, "lastTime", -1)
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if updated_at < last_ts:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {updated_at} vs. prior timestamp {last_ts}"
            )

        doc_count += 1
        last_ts = updated_at

        event.meta_ = model.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    
    # If there was at least one document, update the cursor.
    yield last_ts