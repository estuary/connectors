from datetime import datetime, UTC, timedelta
from copy import deepcopy
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding, ResourceConfig, ResourceState
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .models import RestResponse, RestStream, EndpointConfig, OAUTH2_SPEC, BulkDataResponse

from .api import fetch_incremental_rest, fetch_backfill_rest, fetch_incremental_bulk, fetch_backfill_bulk, BASE_URL


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    discover_streams = []

    selected_streams = [
                    "Account",
                    "Contact",
                    "User",
                    "OpportunityFilledHistory",
                    "LeadHistory",
                    "Opportunity",
                    "Campaign",
                    "Case",
                    "ContactLineItem",
                    "Entitlement",
                    "Lead",
                    "LiveChatTranscript",
                    "MessagingSession",
                    "Quote",
                    "QuoteLineItem",
                    "ServiceAppointment",
                    "ServiceContract",
                    "Task",
                    "UserServicePresence",
                    "WorkOrder",
                    "WorkOrderLineItem"
                    ]

    for stream in selected_streams:
        try:
            describe_url = f"{BASE_URL}/services/data/v57.0/sobjects/{stream}/describe"
            result = await http.request(log, describe_url, method="GET")
            new_stream = RestStream
            discover_streams.append(base_object(new_stream, stream, http))
            log.info("Found REST stream")
        except RuntimeError: # Retuns a 404, meaning it does not have a REST API sobject
            log.info("Found BULK stream")
            new_stream = BulkDataResponse
            discover_streams.append(base_object_bulk(new_stream, stream, http))


    return discover_streams


def base_object(
    cls, stream_name, http: HTTPSession
) -> Resource:


    started_at = datetime.now(tz=UTC)
    stop_date = started_at - timedelta(days=1095)

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental_rest, cls, stream_name, http),
            fetch_page=functools.partial(fetch_backfill_rest, cls, stream_name, stop_date, http),
        )

    return Resource(
        name=stream_name,
        key=["/Id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at - timedelta(days=1095)),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=stream_name),
        schema_inference=True
    )


def base_object_bulk(
    cls, stream_name, http: HTTPSession
) -> Resource:

    started_at = datetime.now(tz=UTC)
    stop_date = started_at - timedelta(days=1095)

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental_bulk, cls, stream_name, http),
            fetch_page=functools.partial(fetch_backfill_bulk, cls, stream_name, stop_date, http),
        )

    return Resource(
        name=stream_name,
        key=["/Id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=stream_name),
        schema_inference=True
    )