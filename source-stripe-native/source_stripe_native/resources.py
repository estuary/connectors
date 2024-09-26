from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools
import re

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding, ResourceConfig, ResourceState
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource, HTTPError

from .api import (
    API,
    fetch_incremental,
    fetch_backfill,
    fetch_incremental_substreams,
    fetch_backfill_substreams,
    fetch_incremental_no_events,
    fetch_backfill_usage_records,
    fetch_incremental_usage_records,
)

from .models import (
    EndpointConfig,
    STREAMS,
    REGIONAL_STREAMS,
    SPLIT_CHILD_STREAM_NAMES,
)

DISABLED_MESSAGE_REGEX = r"Your account is not set up to use"

async def check_accessibility(
    http: HTTPMixin, log: Logger, url: str,
) -> bool:
    """
    Returns a boolean representing whether the provided stream is accessible (True if accessible, False if inaccessible).

    If a stream is inaccessible due to API key permissions, the Stripe API response has a status code of:
    - 401 (live Stripe accounts)
    - 403 (test Stripe accounts)
    
    There are also some streams (like Authorizations, one of the Issuing streams) that can be enabled/disabled separately from API key permissions.
    These streams return a 400 status code & message that contains "Your account is not set up to use X."
    """
    is_accessible = True

    try:
        await http.request(log, url)
    except HTTPError as err:
        is_permission_blocked = err.code == 401 or err.code == 403
        is_disabled = err.code == 400 and bool(re.search(DISABLED_MESSAGE_REGEX, err.message))
        is_accessible = not is_permission_blocked and not is_disabled

    return is_accessible


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    is_restricted_api_key = config.credentials.access_token.startswith("rk_")
    all_streams: list[Resource] = []

    # If a restricted API key was provided, we have to ensure the /events endpoint is accessible.
    # Almost all streams use this endpoint for incremental replication, so we must be able to access it.
    if is_restricted_api_key:
        events_url = f"{API}/events"
        if not await check_accessibility(http, log, events_url):
            raise RuntimeError(f"/events endpoint is not accessible, preventing incremental replication.\nPlease update your restricted API key to have read permissions for Events.")

    for element in STREAMS:
        is_accessible_stream = True
        base_stream = element.get("stream")

        # If we have a restricted API key, we have to check if we can access each stream.
        if is_restricted_api_key:
            url = f"{API}/{base_stream.SEARCH_NAME}"
            is_accessible_stream = await check_accessibility(http, log, url)

        if is_accessible_stream:
            # If the stream class does not have an EVENT_TYPES attribute, then it does not have any associated events.
            resource = base_object(base_stream, http, config.start_date) if hasattr(base_stream, "EVENT_TYPES") else no_events_object(base_stream, http, config.start_date)
            all_streams.append(resource)

            children = element.get("children", [])
            for child in children:
                is_accessible_child = True
                child_stream = child.get("stream")
                # If the child stream has a discoverPath specified, then it's accessibility can be different from its parent.
                if child.get("discoverPath", None):
                    url = f"{API}/{child.get("discoverPath")}"
                    is_accessible_child = await check_accessibility(http, log, url)

                if is_accessible_child:
                    match child_stream.NAME:
                        case "UsageRecords":
                            all_streams.append(usage_records(base_stream, child_stream, http, config.start_date))
                        case _ if child_stream.NAME in SPLIT_CHILD_STREAM_NAMES:
                            all_streams.append(split_child_object(base_stream, child_stream, http, config.start_date))
                        case _:
                            all_streams.append(child_object(base_stream, child_stream, http, config.start_date))

    # Regional streams are only available in certain countries, so we always have
    # check if they accessible regardless of the API key provided.
    for stream in REGIONAL_STREAMS:
        url = f"{API}/{stream.SEARCH_NAME}"
        if await check_accessibility(http, log, url):
            resource = base_object(stream, http, config.start_date)
            all_streams.append(resource)

    return all_streams


def base_object(
    cls, http: HTTPSession, start_date: datetime
) -> Resource:
    """Base Object handles the default case from source-stripe-native
    It requires a single, parent stream with a valid Event API Type
    """

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
            fetch_changes=functools.partial(fetch_incremental, cls, http),
            fetch_page=functools.partial(fetch_backfill, cls, start_date, http),
        )

    cutoff = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )

def child_object(
    cls, child_cls, http: HTTPSession, start_date: datetime
) -> Resource:
    """Child Object handles the default child case from source-stripe-native
    It requires both the parent and child stream, with the parent stream having
    a valid Event API Type
    """

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
            fetch_changes=functools.partial(fetch_incremental_substreams, cls, child_cls, http),
            fetch_page=functools.partial(fetch_backfill_substreams, cls, child_cls, start_date, http),
        )

    cutoff = datetime.now(tz=UTC)

    return Resource(
        name=child_cls.NAME,
        key=["/id"],
        model=child_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=ResourceConfig(name=child_cls.NAME),
        schema_inference=True,
    )

def split_child_object(
    cls, child_cls, http: HTTPSession, start_date: datetime
) -> Resource:
    """
    split_child_object handles the case where a stream is a child stream when backfilling
    but incrementally replicates based off events that contain the child stream resource directly
    in the API response. Meaning, the stream behaves like a non-chid stream incrementally.
    """

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
            fetch_changes=functools.partial(fetch_incremental, child_cls, http),
            fetch_page=functools.partial(fetch_backfill_substreams, cls, child_cls, start_date, http),
        )

    cutoff = datetime.now(tz=UTC)

    return Resource(
        name=child_cls.NAME,
        key=["/id"],
        model=child_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=ResourceConfig(name=child_cls.NAME),
        schema_inference=True,
    )

def usage_records(
    cls, child_cls, http: HTTPSession, start_date: datetime
) -> Resource:
    """ Usage Records handles a specific stream (UsageRecords).
    This is required since Usage Records is a child stream from SubscriptionItem
    and requires special processing.
    """

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
            fetch_changes=functools.partial(fetch_incremental_usage_records, cls, child_cls, http),
            fetch_page=functools.partial(fetch_backfill_usage_records, cls, child_cls, start_date, http),
        )

    cutoff = datetime.now(tz=UTC)

    return Resource(
        name=child_cls.NAME,
        key=["/subscription_item"],
        model=child_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=ResourceConfig(name=child_cls.NAME),
        schema_inference=True,
    )

def no_events_object(
    cls, http: HTTPSession, start_date: datetime
) -> Resource:
    """No Events Object handles a edge-case from source-stripe-native,
    where the given parent stream does not contain a valid Events API type.
    It requires a single, parent stream with a valid list all API endpoint.
    It works very similar to the base object, but without the use of the Events APi.
    """

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
            fetch_changes=functools.partial(fetch_incremental_no_events, cls, http),
            fetch_page=functools.partial(fetch_backfill, cls, start_date, http),
        )

    cutoff = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff)
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )
