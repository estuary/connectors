import json
from logging import Logger
from typing import AsyncGenerator, Iterable

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from .models import Event, EventResponse, Resource, Metadata

API = "https://app.pendo.io/api/v1"


async def fetch_resources(
        http: HTTPSession,
        entity: str,
        log: Logger,
) -> Iterable[Resource]:
    url = f"{API}/{entity}"

    response = json.loads(await http.request(log, url))

    resources: list[Resource] = []
    for id, resource in enumerate(response):
        r = Resource.model_validate_json(json.dumps(resource))
        r.meta_ = Resource.Meta(op="c", row_id=id)
        resources.append(r)

    log.info(f"Read {len(resources)} documents from stream {entity}.")

    return resources


async def fetch_metadata(
        http: HTTPSession,
        entity: str,
        log: Logger,
) -> Iterable[Metadata]:
    url = f"{API}/metadata/schema/{entity}"

    metadata = Metadata.model_validate_json(
        await http.request(log, url)
    )

    metadata.meta_ = Metadata.Meta(op="c")

    log.info(f"Read 1 document from stream {entity}.")

    return [
        metadata
    ]


async def fetch_events(
        http: HTTPSession,
        entity: str,
        timestamp_field: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[Event | LogCursor, None]:
    assert isinstance(log_cursor, int)
    url = f"{API}/aggregation"
    last_ts = log_cursor

    # Pendo's event aggregation endpoints require a JSON body with various fields.
    # See https://engageapi.pendo.io/#f42c416f-bdd6-4082-b86c-1be3fe8401bf. We use
    # their filtering and sorting capabilities to only return documents we haven't
    # seen yet in ascending order.
    # Note: The Pendo API limits aggregations to 5 minute runtime or 4 GB output.
    # If/when we run up against those limits, a different strategy like requesting
    # smaller time windows will be required.
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
                {
                    "filter": f"{timestamp_field} > {last_ts}"
                },
                {
                    "sort": [f"{timestamp_field}"]
                }
            ]
        }
    }

    response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    events = response.results

    doc_count = 0
    for event in events:
        updated_at = getattr(event, timestamp_field, -1)
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if updated_at < last_ts:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {updated_at} vs. prior timestamp {last_ts}"
            )

        doc_count += 1
        last_ts = updated_at

        event.meta_ = Event.Meta(op="c")
        yield event

    log.info(f"Read {doc_count} documents from stream {entity}.")

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    
    # If there was at least one document, update the cursor.
    yield last_ts
