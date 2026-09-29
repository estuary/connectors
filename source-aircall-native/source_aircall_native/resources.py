import functools
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    ONE_SECOND,
    backfill_calls,
    backfill_contacts,
    fetch_calls,
    fetch_contacts,
    snapshot_company,
    snapshot_list,
)
from .models import SNAPSHOT_STREAMS, Call, Contact, EndpointConfig
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
INTERVAL = timedelta(minutes=5)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        _ = await http.request(log, f"{API}/ping")
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API ID or API token. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def snapshot_resources(http: HTTPMixin) -> list[common.Resource]:
    def open(
        fetch_snapshot: Callable[[Logger], AsyncGenerator[BaseDocument, None]],
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(binding, binding_index, state, task, fetch_snapshot=fetch_snapshot)

    fetches = {
        name: functools.partial(snapshot_list, http, endpoint)
        for name, endpoint in SNAPSHOT_STREAMS.items()
    }
    fetches["company"] = functools.partial(snapshot_company, http)

    return [
        SnapshotResource(
            name=name,
            open=functools.partial(open, fetch),
            initial_config=ResourceConfigWithSchedule(name=name, interval=INTERVAL),
        )
        for name, fetch in fetches.items()
    ]


def calls(http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
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
        name="calls",
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
        initial_config=ResourceConfigWithSchedule(name="calls", interval=INTERVAL),
        schema_inference=True,
    )


def contacts(http: HTTPMixin) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
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
        name="contacts",
        key=["/id"],
        model=Contact,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - ONE_SECOND),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name="contacts", interval=INTERVAL, schedule=CONTACTS_SCHEDULE
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    return [calls(http, config), contacts(http), *snapshot_resources(http)]
