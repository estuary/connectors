from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator
import pendulum
from pydantic import TypeAdapter
from estuary_cdk.capture.common import BaseDocument, LogCursor, PageCursor
import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.http import HTTPSession

from .models import PageEvent, FeatureEvent, TrackEvent, GuideEvent, PollEvent, AggregatedEventResponse, EventResponse, Resource, Metadata, ResourceResponse

API = "https://app.pendo.io/api/v1"
RESPONSE_LIMIT = 50000
# Event data for a given hour isn't available via the API until ~4-6 hours afterwards.
# This isn't mentioned in Pendo's docs but has been observed empirically. We shift the
# cutoff between backfills & incremental replication back multiple hours to ensure we're
# only backfilling date windows where event data should be available in the API.
API_EVENT_LAG = timedelta(hours=8)
# Use a date window size of 1 day for event backfills since there can be a huge
# number of events in a small timespan.
EVENT_DATE_WINDOW_SIZE_IN_DAYS = 1
# Use a moderately sized date window for resource backfills.
RESOURCE_DATE_WINDOW_SIZE_IN_DAYS = 30


def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)

def generate_events_body(
        entity: str,
        identifying_field:str,
        lower_bound: int,
        upper_bound: int | None = None,
        last_seen_id: str | None = None,
) -> dict[str, Any]:
    """
    Builds the request body to retrieve events from the Pendo API.

    Pendo's event aggregation endpoints require a JSON body with various fields.
    See https://engageapi.pendo.io/#f42c416f-bdd6-4082-b86c-1be3fe8401bf. We use
    their filtering and sorting capabilities to paginate through documents in 
    ascending order. Each document returned represents a single event.
    """

    # If we have an ID that we saw last, get the remaining aggregates for this specific timestamp. 
    # Otherwise, get as many aggregates as we can since the last timestamp.
    #
    # The filter condition uses:
    #    guideTimestamp: the last time the aggregate was updated
    # identifying_field: the unique ID for the associated Pendo resource. Used as a second filter when
    #                    there are more events in Pendo with the same lastTime than we can retrieve in
    #                    a single API query.
    if last_seen_id:
        filter_condition = f"guideTimestamp == {lower_bound} && {identifying_field} >= \"{last_seen_id}\""
    else:
        filter_condition = f"guideTimestamp >= {lower_bound}"
        if upper_bound:
            filter_condition += f" && guideTimestamp <= {upper_bound}"

    body = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "pipeline": [
                {
                    "source": {
                        entity: {
                            # Capture events for all applications within this Pendo subscription.
                            "appId": "expandAppIds(\"*\")",
                        },
                        "timeSeries": {
                            "period": "hourRange",
                            "first": f"{lower_bound}",
                            # If an upper bound isn't specified, retrieve all events up to the present.
                            "last": f"{upper_bound or 'now()'}"
                        }
                    }
                },
                {
                    "filter": filter_condition
                },
                # Events are sorted first by their guideTimestamp, then by their
                # identifying_field if they have the same guideTimestamp.
                {
                    "sort": ["guideTimestamp", f"{identifying_field}"]
                },
                {
                    "limit": RESPONSE_LIMIT
                }
            ]
        }
    }

    return body


def generate_event_aggregates_body(
        entity: str,
        identifying_field: str,
        lower_bound: int,
        upper_bound: int | None = None,
        last_seen_id: str | None = None,
) -> dict[str, Any]:
    """
    Builds the request body to retrieve event aggregates from the Pendo API.

    Pendo's event aggregation endpoints require a JSON body with various fields.
    See https://engageapi.pendo.io/#f42c416f-bdd6-4082-b86c-1be3fe8401bf. We use
    their filtering and sorting capabilities to paginate through documents in 
    ascending order. Each document returned represents an aggregation 
    of events in a specific hour.
    """

    # current_hour is used to only get aggregates for the past hour. We add a 15 minute delay
    # when determining the previous hour in an attempt to avoid issues due to distributed clocks.
    current_hour = _dt_to_ms(pendulum.now("UTC").subtract(minutes=15).set(minute=0, second=0, microsecond=0))

    # If we have an ID that we saw last, get the remaining aggregates for this specific timestamp. 
    # Otherwise, get as many aggregates as we can since the last timestamp.
    #
    # The filter condition uses:
    #          lastTime: The last time the aggregate was updated.
    # identifying_field: The unique ID for the associated Pendo resource. Used as a second filter when
    #                    there are more aggregates in Pendo with the same lastTime than we can retrieve
    #                    in a single API query.
    #              hour: The bottom of the hour for the aggregate. Ensures we don't get "in-progress"
    #                    aggregates for the current hour.
    if last_seen_id:
        filter_condition = f"lastTime == {lower_bound} && {identifying_field} >= \"{last_seen_id}\" && hour < {current_hour}"
    else:
        filter_condition = f"lastTime >= {lower_bound} && hour < {current_hour}"
        if upper_bound:
            filter_condition += f" && lastTime <= {upper_bound}"

    body = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "pipeline": [
                {
                    "source": {
                        entity: {
                            # Capture events for all applications within this Pendo subscription.
                            "appId": "expandAppIds(\"*\")",
                        },
                        "timeSeries": {
                            "period": "hourRange",
                            "first": f"{lower_bound}",
                            # If an upper bound isn't specified, retrieve all events up to the present.
                            "last": f"{upper_bound or 'now()'}"
                        }
                    }
                },
                {
                    "filter": filter_condition
                },
                # Aggregates are sorted first by their lastTime, then by their
                # identifying_field if they have the same lastTime.
                {
                    "sort": ["lastTime", f"{identifying_field}"]
                },
                {
                    "limit": RESPONSE_LIMIT
                }
            ]
        }
    }

    return body


def generate_resources_body(
        entity: str,
        updated_at_field: str,
        identifying_field: str,
        lower_bound: int,
        upper_bound: int | None = None,
        last_seen_id: str | None = None,
) -> dict[str, Any]:
    """
    Builds the request body to retrieve resources from the Pendo API.

    Pendo's resource aggregation endpoints require a JSON body with various fields.
    See https://engageapi.pendo.io/#3f5fdb6d-02da-4441-b4d1-75f795ba248c. We use
    their filtering and sorting capabilities to paginate through documents in 
    ascending order. Each document returned represents a single resource.
    """

    # If we have an ID that we saw last, get the remaining resources for that specific timestamp. 
    # Otherwise, get as many resources as we can since the last timestamp.
    #
    # The filter condition uses:
    #  updated_at_field: the last time the resource was updated
    # identifying_field: the unique ID for the associated Pendo resource. Used as a second filter when
    #                    there are more resources in Pendo with the same updated_at_field than we can retrieve in
    #                    a single API query.
    if last_seen_id:
        filter_condition = f"{updated_at_field} == {lower_bound} && {identifying_field} >= \"{last_seen_id}\""
    else:
        filter_condition = f"{updated_at_field} >= {lower_bound}"
        if upper_bound:
            filter_condition += f" && {updated_at_field} <= {upper_bound}"

    body = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "pipeline": [
                {
                    "source": {
                        entity: {
                            # Capture resources for all applications within this Pendo subscription.
                            "appId": "expandAppIds(\"*\")",
                        },
                    }
                },
                {
                    "filter": filter_condition
                },
                # Resources are sorted first by their updated_at_field, then by their
                # identifying_field if they have the same updated_at_field value.
                {
                    "sort": [f"{updated_at_field}", f"{identifying_field}"]
                },
                {
                    "limit": RESPONSE_LIMIT
                }
            ]
        }
    }

    return body


async def snapshot_resources(
        http: HTTPSession,
        entity: str,
        log: Logger,
) -> AsyncGenerator[Resource, None]:
    url = f"{API}/{entity}"

    params = {
        # The expand query parameter tells Pendo to return data for all
        # applications associated with the Pendo subscription.
        "expand": "*",
    }

    resources = TypeAdapter(list[Resource]).validate_json(await http.request(log, url, params=params))

    for resource in resources:
        yield resource


async def snapshot_metadata(
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
        identifying_field: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[GuideEvent | PollEvent | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    url = f"{API}/aggregation"
    last_dt = log_cursor
    horizon = datetime.now(tz=UTC) - API_EVENT_LAG

    # Avoid requesting data when the current cursor is beyond the eventual consistency horizon.
    # This should only be possible on invocations shortly after the horizon is implemented. Afterwards,
    # the cursor should never go beyond horizon naturally since Pendo shouldn't return data generated
    # after the horizon.
    if last_dt >= horizon:
        return

    last_ts = _dt_to_ms(last_dt)
    upper_bound_ts = _dt_to_ms(horizon)

    body = generate_events_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts)

    response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    events = response.results

    doc_count = 0
    last_seen_id = ""
    for event in events:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if event.guideTimestamp < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event date is {event.guideTimestamp} vs. prior date {last_dt}"
            )

        if event.guideTimestamp > horizon:
            raise RuntimeError(
                f"Received events beyond the eventual consistency horizon: Current event timestamp is {event.guideTimestamp} vs. horizon {horizon}"
            )

        doc_count += 1
        last_dt = event.guideTimestamp

        event.meta_ = model.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    elif last_dt > log_cursor:
        # If there were documents and the last one has a later timestamp than our cursor,
        # update the cursor.
        yield last_dt
    elif last_dt == log_cursor:
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_events_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, last_seen_id=last_seen_id)

            response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
            events = response.results

            doc_count = 0
            for event in events:
                if event.guideTimestamp < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event date is {event.guideTimestamp} vs. prior date {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(event, identifying_field)

                event.meta_ = model.Meta(op="c")
                yield event

            if doc_count < RESPONSE_LIMIT:
                break

        yield last_dt + timedelta(milliseconds=1)


async def backfill_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        identifying_field: str,
        log: Logger,
        page_cursor: PageCursor | None,
        cutoff: LogCursor,
) -> AsyncGenerator[GuideEvent | PollEvent | PageCursor, None]:
    assert isinstance(page_cursor, int)
    assert isinstance(cutoff, datetime)
    url = f"{API}/aggregation"
    last_dt = _ms_to_dt(page_cursor)
    upper_bound_dt = last_dt + timedelta(days=EVENT_DATE_WINDOW_SIZE_IN_DAYS)

    # If we've reached or exceeded the cutoff date, stop backfilling.
    if last_dt >= cutoff:
        return

    last_ts = _dt_to_ms(last_dt)
    upper_bound_ts = _dt_to_ms(upper_bound_dt)

    body = generate_events_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts)

    response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    events = response.results

    doc_count = 0
    last_seen_id = ""
    for event in events:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if event.guideTimestamp < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event date is {event.guideTimestamp} vs. prior date {last_dt}"
            )

        doc_count += 1
        last_dt = event.guideTimestamp
        last_seen_id = getattr(event, identifying_field)

        event.meta_ = model.Meta(op="c")
        yield event

    if doc_count == 0:
        # If there were no documents, we need to move the cursor forward to slide forward our date window.
        yield _dt_to_ms(last_dt + timedelta(days=EVENT_DATE_WINDOW_SIZE_IN_DAYS))
    elif last_dt > _ms_to_dt(page_cursor):
        # If there were documents and the last one has a later timestamp than our cursor,
        # then update the cursor to the later timestamp.
        yield _dt_to_ms(last_dt)
    elif last_dt == _ms_to_dt(page_cursor):
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_events_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts, last_seen_id=last_seen_id)

            response = EventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
            events = response.results

            doc_count = 0
            for event in events:
                if event.guideTimestamp < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event date is {event.guideTimestamp} vs. prior date {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(event, identifying_field)

                event.meta_ = model.Meta(op="c")
                yield event

            if doc_count < RESPONSE_LIMIT:
                break

        yield _dt_to_ms(last_dt + timedelta(milliseconds=1))


async def fetch_aggregated_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        identifying_field: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[PageEvent | FeatureEvent | TrackEvent | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    url = f"{API}/aggregation"
    last_dt = log_cursor
    horizon = datetime.now(tz=UTC) - API_EVENT_LAG

    # Avoid requesting data when the current cursor is beyond the eventual consistency horizon.
    # This should only be possible on invocations shortly after the horizon is implemented. Afterwards,
    # the cursor should never go beyond horizon naturally since Pendo shouldn't return data generated
    # after the horizon.
    if last_dt >= horizon:
        return

    last_ts = _dt_to_ms(last_dt)
    upper_bound_ts = _dt_to_ms(horizon)

    body = generate_event_aggregates_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts)

    response = AggregatedEventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    aggregates = response.results

    doc_count = 0
    last_seen_id = ""
    for aggregate in aggregates:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if aggregate.lastTime < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {aggregate.lastTime} vs. prior timestamp {last_dt}"
            )

        if aggregate.lastTime > horizon:
            raise RuntimeError(
                f"Received events beyond the eventual consistency horizon: Current event timestamp is {aggregate.lastTime} vs. horizon {horizon}"
            )

        doc_count += 1
        last_dt = aggregate.lastTime
        last_seen_id = getattr(aggregate, identifying_field)

        aggregate.meta_ = model.Meta(op="c")
        yield aggregate

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    elif last_dt > log_cursor:
        # If there was at least one document and the last document's timestamp is
        # later than the cursor, update the cursor.
        yield last_dt
    elif last_dt == log_cursor:
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_event_aggregates_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, last_seen_id=last_seen_id)

            response = AggregatedEventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
            aggregates = response.results

            doc_count = 0
            for aggregate in aggregates:
                if aggregate.lastTime < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event timestamp is {aggregate.lastTime} vs. prior timestamp {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(aggregate, identifying_field)

                aggregate.meta_ = model.Meta(op="c")
                yield aggregate

            if doc_count < RESPONSE_LIMIT:
                break

        yield last_dt + timedelta(milliseconds=1)


async def backfill_aggregated_events(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        identifying_field: str,
        log: Logger,
        page_cursor: PageCursor | None,
        cutoff: LogCursor,
) -> AsyncGenerator[PageEvent | FeatureEvent | TrackEvent | PageCursor, None]:
    assert isinstance(page_cursor, int)
    assert isinstance(cutoff, datetime)
    url = f"{API}/aggregation"
    last_dt = _ms_to_dt(page_cursor)
    upper_bound_dt = last_dt + timedelta(days=EVENT_DATE_WINDOW_SIZE_IN_DAYS)

    # If we've reached or exceeded the cutoff date, stop backfilling.
    if last_dt >= cutoff:
        return

    last_ts = _dt_to_ms(last_dt)
    upper_bound_ts = _dt_to_ms(upper_bound_dt)

    body = generate_event_aggregates_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts)

    response = AggregatedEventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
    aggregates = response.results

    doc_count = 0
    last_seen_id = ""
    for aggregate in aggregates:
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if aggregate.lastTime < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {aggregate.lastTime} vs. prior timestamp {last_dt}"
            )

        doc_count += 1
        last_dt = aggregate.lastTime
        last_seen_id = getattr(aggregate, identifying_field)

        aggregate.meta_ = model.Meta(op="c")
        yield aggregate

    if doc_count == 0:
        # If there were no documents, we need to move the cursor forward to slide forward our date window.
        yield _dt_to_ms(last_dt + timedelta(days=EVENT_DATE_WINDOW_SIZE_IN_DAYS))
    elif last_dt > _ms_to_dt(page_cursor):
        # If there were documents and the last one has a later timestamp than our cursor, then update
        # the cursor to the later timestamp.
        yield _dt_to_ms(last_dt)
    elif last_dt == _ms_to_dt(page_cursor):
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_event_aggregates_body(entity=entity, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts, last_seen_id=last_seen_id)

            response = AggregatedEventResponse.model_validate_json(await http.request(log, url, method="POST", json=body))
            aggregates = response.results

            doc_count = 0
            for aggregate in aggregates:
                if aggregate.lastTime < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event timestamp is {aggregate.lastTime} vs. prior timestamp {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(aggregate, identifying_field)

                aggregate.meta_ = model.Meta(op="c")
                yield aggregate

            if doc_count < RESPONSE_LIMIT:
                break

        yield _dt_to_ms(last_dt + timedelta(milliseconds=1))


# _get_field extracts a field from a document. The field could be
# nested somewhere within document. ex: metadata.auto.lastupdated
def _get_field(doc: BaseDocument, field: str, log: Logger,):
    keys = field.split('.')
    value = doc.model_dump()
    for key in keys:
        value = value.get(key)
        if value is None:
            raise RuntimeError(f"No field \"{field}\" present in document.")

    return value


def _extract_updated_at(doc: BaseDocument, updated_at_field: str, log: Logger) -> datetime:
    ms = _get_field(doc, updated_at_field, log)
    assert isinstance(ms, int)
    return _ms_to_dt(ms)


async def fetch_resources(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        updated_at_field: str,
        identifying_field: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[BaseDocument | LogCursor, None]:
    assert isinstance(log_cursor, datetime)
    url = f"{API}/aggregation"
    last_dt = log_cursor
    last_ts = _dt_to_ms(last_dt)

    body = generate_resources_body(entity=entity, updated_at_field=updated_at_field, identifying_field=identifying_field, lower_bound=last_ts)

    response_model = TypeAdapter(ResourceResponse[model])
    response = response_model.validate_json(await http.request(log, url, method="POST", json=body))

    doc_count = 0
    last_seen_id = ""
    for resource in response.results:
        updated_at_dt = _extract_updated_at(resource, updated_at_field, log)
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if updated_at_dt < last_dt:
            raise RuntimeError(
                f"Received resources out of time order: Current resource timestamp is {updated_at_dt} vs. prior timestamp {last_dt}"
            )

        doc_count += 1
        last_dt = updated_at_dt
        last_seen_id = getattr(resource, identifying_field)

        if cache.should_yield(entity, last_seen_id, updated_at_dt):
            yield resource

    if doc_count == 0:
        # If there were no documents, don't update the cursor.
        return
    elif last_dt > log_cursor:
        # If there was at least one document and the last document's timestamp is
        # later than the cursor, update the cursor.
        yield last_dt
    elif last_dt == log_cursor:
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_resources_body(entity=entity, updated_at_field=updated_at_field, identifying_field=identifying_field, lower_bound=last_ts, last_seen_id=last_seen_id)
            response = response_model.validate_json(await http.request(log, url, method="POST", json=body))

            doc_count = 0
            for resource in response.results:
                updated_at_dt = _extract_updated_at(resource, updated_at_field, log)
                if updated_at_dt < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event timestamp is {updated_at_dt} vs. prior timestamp {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(resource, identifying_field)

                if cache.should_yield(entity, last_seen_id, updated_at_dt):
                    yield resource

            if doc_count < RESPONSE_LIMIT:
                break

        yield last_dt + timedelta(milliseconds=1)


async def backfill_resources(
        http: HTTPSession,
        entity: str,
        model: type[BaseDocument],
        updated_at_field: str,
        identifying_field: str,
        log: Logger,
        page_cursor: PageCursor | None,
        cutoff: LogCursor,
) -> AsyncGenerator[BaseDocument | PageCursor, None]:
    assert isinstance(page_cursor, int)
    assert isinstance(cutoff, datetime)
    url = f"{API}/aggregation"
    last_dt = _ms_to_dt(page_cursor)
    upper_bound_dt = last_dt + timedelta(days=RESOURCE_DATE_WINDOW_SIZE_IN_DAYS)

    # If we've reached or exceeded the cutoff date, stop backfilling.
    if last_dt >= cutoff:
        return

    last_ts = _dt_to_ms(last_dt)
    upper_bound_ts = _dt_to_ms(upper_bound_dt)

    body = generate_resources_body(entity=entity, updated_at_field=updated_at_field, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts)
    response_model = TypeAdapter(ResourceResponse[model])
    response = response_model.validate_json(await http.request(log, url, method="POST", json=body))

    doc_count = 0
    last_seen_id = ""
    for resource in response.results:
        updated_at_dt = _extract_updated_at(resource, updated_at_field, log)
        # Due to how we're querying the API with the "sort" and "filter" operators, 
        # we don't expect to receive documents out of order.
        if updated_at_dt < last_dt:
            raise RuntimeError(
                f"Received events out of time order: Current event timestamp is {updated_at_dt} vs. prior timestamp {last_dt}"
            )

        if updated_at_dt >= cutoff:
            return

        doc_count += 1
        last_dt = updated_at_dt
        last_seen_id = getattr(resource, identifying_field)

        yield resource

    if doc_count == 0:
        # If there were no documents, we need to move the cursor forward to slide forward our date window.
        yield _dt_to_ms(last_dt + timedelta(days=RESOURCE_DATE_WINDOW_SIZE_IN_DAYS))
    elif last_dt > _ms_to_dt(page_cursor):
        # If there were documents and the last one has a later timestamp than our cursor, then update
        # the cursor to the later timestamp.
        yield _dt_to_ms(last_dt)
    elif last_dt == _ms_to_dt(page_cursor):
        # If the last document has the same timestamp as our cursor, fetch the remaining documents with
        # this timestamp & increment the cursor by 1 afterwards.
        while True:
            body = generate_resources_body(entity=entity, updated_at_field=updated_at_field, identifying_field=identifying_field, lower_bound=last_ts, upper_bound=upper_bound_ts, last_seen_id=last_seen_id)
            response = response_model.validate_json(await http.request(log, url, method="POST", json=body))

            doc_count = 0
            for resource in response.results:
                updated_at_dt = _extract_updated_at(resource, updated_at_field, log)
                if updated_at_dt < last_dt:
                    raise RuntimeError(
                        f"Received events out of time order: Current event timestamp is {updated_at_dt} vs. prior timestamp {last_dt}"
                    )

                doc_count += 1
                last_seen_id = getattr(resource, identifying_field)

                yield resource

            if doc_count < RESPONSE_LIMIT:
                break

        yield _dt_to_ms(last_dt + timedelta(milliseconds=1))
