import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Type, TypeVar

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_issues,
    fetch_issues,
    list_entity,
)
from .models import (
    FULL_REFRESH_RESOURCES,
    EndpointConfig,
    Environment,
    FullRefreshResource,
    Issue,
    ResourceConfig,
    ResourceState,
    SentryEntity,
)


async def validate_credentials(http: HTTPMixin, config: EndpointConfig, log: Logger):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        await anext(list_entity(Environment, http, config.organization, log))
    except HTTPError as err:
        msg = f"Encountered error validating credentials.\n\n{err.message}"
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"

        raise ValidationError([msg])


T = TypeVar("T", bound=SentryEntity)


async def all_resources(
    _log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    # API works at the seconds scale
    start_date = config.start_date.replace(microsecond=0)
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=AccessToken(
            credentials_title=config.credentials.credentials_title,
            access_token=config.credentials.access_token,
        ),
    )

    def open_full_refresh_bindings(
        entity: Type[SentryEntity],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                list_entity, entity, http, config.organization
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d")),
        )

    def open_issue_binding(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_issues, http, config.organization),
            fetch_page=functools.partial(
                backfill_issues, http, config.organization, config.advanced.window_size
            ),
        )

    return [
        common.Resource(
            name=resource.resource_name,
            key=["/_meta/row_id"],
            model=resource,
            open=functools.partial(open_full_refresh_bindings, resource),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=resource.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for resource in FULL_REFRESH_RESOURCES
    ] + [
        common.Resource(
            name="Issues",
            key=["/id"],
            model=Issue,
            open=open_issue_binding,
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(
                    cutoff=cutoff,
                    next_page=start_date.isoformat(),
                ),
                inc=common.ResourceState.Incremental(
                    cursor=cutoff - timedelta(seconds=1)  # Time bounds are exclusive
                ),
            ),
            initial_config=ResourceConfig(name="Issues", interval=timedelta(minutes=5)),
            schema_inference=True,
        )
    ]
