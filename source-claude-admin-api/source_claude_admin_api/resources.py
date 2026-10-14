"""Resource definitions for the source-claude-admin-api connector.

Resources are registered from per-shape lists: each model carries its endpoint
(api_path) and the registry binds it to the matching generic fetch shape.
"""

import functools
from collections.abc import AsyncGenerator
from datetime import timedelta
from logging import Logger
from typing import Callable

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    fetch_claude_code_usage,
    fetch_cursor_list,
    fetch_singleton,
)
from .models import (
    ANTHROPIC_VERSION_HEADERS,
    BaseClaudeEntity,
    ClaudeCodeUsageRecord,
    EndpointConfig,
    Organization,
    ResourceConfig,
    ResourceState,
    User,
)

SNAPSHOT_INTERVAL = timedelta(hours=1)
USAGE_INTERVAL = timedelta(hours=6)

# Snapshot resources, grouped by the generic fetch shape that serves them.
SNAPSHOT_SINGLETONS: list[type[BaseClaudeEntity]] = [Organization]
SNAPSHOT_LISTS: list[type[BaseClaudeEntity]] = [User]


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=None,
        credentials=AccessToken(access_token=config.admin_api_key),
        authorization_header="x-api-key",
    )


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> None:
    http.token_source = _token_source(config)
    url = f"{config.advanced.base_url.rstrip('/')}/{Organization.api_path}"

    try:
        await http.request(log, url, headers=ANTHROPIC_VERSION_HEADERS)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    f"Invalid Admin API key. Confirm the provided key is a valid Anthropic Admin API key (sk-ant-admin...).\n\n{err.message}"
                ]
            )
        raise ValidationError(
            [f"Encountered error validating Admin API key.\n\n{err.message}"]
        )


def _snapshot_resource(
    model: type[BaseClaudeEntity],
    fetch_snapshot: Callable[[Logger], AsyncGenerator],
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=fetch_snapshot,
            tombstone=model(id="", _meta=model.Meta(op="d")),
        )

    return common.Resource(
        name=model.resource_name,
        key=["/id"],
        model=model,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=model.resource_name, interval=SNAPSHOT_INTERVAL
        ),
        schema_inference=True,
    )


def _claude_code_usage_resource(
    http: HTTPMixin, base_url: str, config: EndpointConfig
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_claude_code_usage, http, base_url),
        )

    return common.Resource(
        name=ClaudeCodeUsageRecord.resource_name,
        key=["/date", "/organization_id", "/actor_id"],
        model=ClaudeCodeUsageRecord,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.advanced.start_date),
        ),
        initial_config=ResourceConfig(
            name=ClaudeCodeUsageRecord.resource_name, interval=USAGE_INTERVAL
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = _token_source(config)
    base_url = config.advanced.base_url.rstrip("/")

    resources = [
        _snapshot_resource(
            model, functools.partial(fetch_singleton, model, http, base_url)
        )
        for model in SNAPSHOT_SINGLETONS
    ]
    resources += [
        _snapshot_resource(
            model, functools.partial(fetch_cursor_list, model, http, base_url)
        )
        for model in SNAPSHOT_LISTS
    ]
    resources.append(_claude_code_usage_resource(http, base_url, config))
    return resources
