import functools
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    ONE_SECOND,
    backfill_calls,
    backfill_contacts,
    fetch_calls,
    fetch_contacts,
    snapshot_list,
    snapshot_object,
)
from .models import (
    SNAPSHOT_LIST_STREAMS,
    SNAPSHOT_OBJECT_STREAMS,
    AircallReferenceEntity,
    Call,
    Contact,
    EndpointConfig,
)
from .shared import API, now

REALTIME = "realtime"
LOOKBACK = "lookback"
# Calls keep changing after they start: they are ended, tagged, commented,
# assigned and gain recordings. The lookback subtask re-reads each call once
# that has settled.
LOOKBACK_LAG = timedelta(hours=24)
# Contacts are re-backfilled daily to pick up changes the `updated_at` walk
# cannot see, such as edits to a contact's phone numbers or emails.
CONTACTS_SCHEDULE = "0 0 * * *"

AircallResource = Resource[
    AircallReferenceEntity, ResourceConfigWithSchedule, ResourceState
]

SnapshotFetchFn = Callable[
    [HTTPSession, type[AircallReferenceEntity], Logger],
    AsyncGenerator[AircallReferenceEntity, None],
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        _ = await http.request(log, f"{API}/ping")
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API ID or API token. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def snapshot_resources(http: HTTPMixin) -> list[AircallResource]:
    def open(
        stream: type[AircallReferenceEntity],
        fetch_fn: SnapshotFetchFn,
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: object,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(fetch_fn, http, stream),
        )

    fetch_fns: list[tuple[list[type[AircallReferenceEntity]], SnapshotFetchFn]] = [
        (SNAPSHOT_LIST_STREAMS, snapshot_list),
        (SNAPSHOT_OBJECT_STREAMS, snapshot_object),
    ]

    resources: list[AircallResource] = [
        SnapshotResource(
            name=stream.name,
            open=functools.partial(open, stream, fetch_fn),
            initial_config=ResourceConfigWithSchedule(
                name=stream.name, interval=timedelta(minutes=5)
            ),
        )
        for streams, fetch_fn in fetch_fns
        for stream in streams
    ]

    return resources


def calls(
    http: HTTPMixin, config: EndpointConfig
) -> common.Resource[Call, ResourceConfigWithSchedule, ResourceState]:
    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: object,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                REALTIME: functools.partial(fetch_calls, http, timedelta(0)),
                LOOKBACK: functools.partial(fetch_calls, http, LOOKBACK_LAG),
            },
            fetch_page=functools.partial(backfill_calls, http, config.start_date),
        )

    cutoff = now().replace(microsecond=0)

    return common.Resource(
        name=Call.name,
        key=["/id"],
        model=Call,
        open=open,
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=cutoff - ONE_SECOND),
                LOOKBACK: ResourceState.Incremental(
                    cursor=cutoff - ONE_SECOND - LOOKBACK_LAG
                ),
            },
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=Call.name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def contacts(
    http: HTTPMixin,
) -> common.Resource[Contact, ResourceConfigWithSchedule, ResourceState]:
    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: object,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_contacts, http),
            fetch_page=functools.partial(backfill_contacts, http),
        )

    cutoff = now().replace(microsecond=0)

    return common.Resource(
        name=Contact.name,
        key=["/id"],
        model=Contact,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - ONE_SECOND),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=Contact.name,
            interval=timedelta(minutes=5),
            schedule=CONTACTS_SCHEDULE,
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource[Any, ResourceConfigWithSchedule, ResourceState]]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    return [
        calls(http, config),
        contacts(http),
        *snapshot_resources(http),
    ]
