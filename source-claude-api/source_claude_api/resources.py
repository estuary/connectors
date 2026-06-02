"""Resource definitions for the source-claude-api connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    ANTHROPIC_VERSION_HEADERS,
    fetch_claude_code_usage,
    fetch_organization,
    fetch_users,
)
from .models import (
    ClaudeCodeUsageRecord,
    EndpointConfig,
    Organization,
    ResourceConfig,
    ResourceState,
    User,
)

SNAPSHOT_INTERVAL = timedelta(hours=1)
USAGE_INTERVAL = timedelta(hours=6)


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
    base_url = config.advanced.base_url.rstrip("/")
    url = f"{base_url}/v1/organizations/me"

    try:
        await http.request(log, url, headers=ANTHROPIC_VERSION_HEADERS)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    "Invalid Admin API key. Confirm the provided key is a valid "
                    f"Anthropic Admin API key (sk-ant-admin...).\n\n{err.message}"
                ]
            )
        raise ValidationError(
            [f"Encountered error validating Admin API key.\n\n{err.message}"]
        )


def organization(http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    base_url = config.advanced.base_url.rstrip("/")

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
            fetch_snapshot=functools.partial(fetch_organization, http, base_url),
            tombstone=Organization(id="", _meta=Organization.Meta(op="d")),
        )

    return common.Resource(
        name="Organization",
        key=["/id"],
        model=Organization,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="Organization", interval=SNAPSHOT_INTERVAL),
        schema_inference=True,
    )


def users(http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    base_url = config.advanced.base_url.rstrip("/")

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
            fetch_snapshot=functools.partial(fetch_users, http, base_url),
            tombstone=User(
                id="",
                added_at=datetime.min.replace(tzinfo=UTC),
                _meta=User.Meta(op="d"),
            ),
        )

    return common.Resource(
        name="Users",
        key=["/id"],
        model=User,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="Users", interval=SNAPSHOT_INTERVAL),
        schema_inference=True,
    )


def claude_code_usage_report(
    http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    base_url = config.advanced.base_url.rstrip("/")

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
        name="ClaudeCodeUsageReport",
        key=["/date", "/organization_id", "/actor_id"],
        model=ClaudeCodeUsageRecord,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.advanced.start_date),
        ),
        initial_config=ResourceConfig(
            name="ClaudeCodeUsageReport", interval=USAGE_INTERVAL
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = _token_source(config)

    return [
        organization(http, config),
        users(http, config),
        claude_code_usage_report(http, config),
    ]
