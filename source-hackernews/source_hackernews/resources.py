import functools
from datetime import timedelta, datetime, UTC
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPSession, HTTPMixin

from .models import (
    EndpointConfig,
    ResourceState, Item, User, ResourceConfig
)
from .api import (
    fetch_page, fetch_user
)


async def all_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        items(http),
        users(http),
    ]


def items(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(fetch_page, http),
        )

    return common.Resource(
        name="items",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="items", interval=timedelta(hours=1)),
        schema_inference=False,
    )


def users(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_user_filtered(log, start_cursor, log_cutoff):
            user = await fetch_user(http, log, start_cursor)
            if user is not None:  # Only yield if we got a valid user
                yield user
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_user_filtered,
        )

    return common.Resource(
        name="users",
        key=["/id"],
        model=User,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="users", interval=timedelta(minutes=5)),
        schema_inference=False,
    )