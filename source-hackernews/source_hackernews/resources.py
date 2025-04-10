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
    fetch_page, fetch_user, fetch_max_item_id, fetch_top_stories,
    fetch_new_stories, fetch_best_stories, fetch_ask_stories,
    fetch_show_stories, fetch_job_stories, fetch_updates
)


async def all_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        items(http),
        users(http),
        top_stories(http),
        new_stories(http),
        best_stories(http),
        ask_stories(http),
        show_stories(http),
        job_stories(http),
        updates(http)
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


def top_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_top_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_top_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_top_stories_filtered,
        )

    return common.Resource(
        name="top_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="top_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def new_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_new_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_new_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_new_stories_filtered,
        )

    return common.Resource(
        name="new_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="new_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def best_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_best_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_best_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_best_stories_filtered,
        )

    return common.Resource(
        name="best_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="best_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def ask_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_ask_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_ask_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_ask_stories_filtered,
        )

    return common.Resource(
        name="ask_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="ask_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def show_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_show_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_show_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_show_stories_filtered,
        )

    return common.Resource(
        name="show_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="show_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def job_stories(http: HTTPSession):
    def open(
            binding: CaptureBinding,
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        async def fetch_job_stories_filtered(log, start_cursor, log_cutoff):
            story_ids = await fetch_job_stories(http, log)
            for story_id in story_ids:
                async for item in fetch_page(http, log, story_id, log_cutoff):
                    yield item
            yield start_cursor + 1

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_job_stories_filtered,
        )

    return common.Resource(
        name="job_stories",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="job_stories", interval=timedelta(minutes=5)),
        schema_inference=False,
    )


def updates(http: HTTPSession):
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
            fetch_page=functools.partial(fetch_updates, http),
        )

    return common.Resource(
        name="updates",
        key=["/id"],
        model=Item,
        open=open,
        initial_state=ResourceState(backfill=ResourceState.Backfill(next_page=1, cutoff=datetime.now(tz=UTC))),
        initial_config=ResourceConfig(name="updates", interval=timedelta(minutes=1)),
        schema_inference=False,
    )
