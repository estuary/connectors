import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Type, TypeVar

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    MAX_EXPLORE_FULL_FIDELITY_WINDOW,
    backfill_explore_query,
    backfill_issues,
    fetch_explore_query,
    fetch_issues,
    list_entity,
    validate_explore_query,
)
from .models import (
    FULL_REFRESH_RESOURCES,
    EndpointConfig,
    Environment,
    ExploreQuery,
    FullRefreshResource,
    Issue,
    ResourceConfig,
    ResourceState,
    SentryEntity,
    dataset_pk,
    dataset_row_model,
    explore_query_stream_name,
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


async def validate_explore_queries(
    http: HTTPMixin, config: EndpointConfig, log: Logger
):
    """Probe each configured explore query with a single request so invalid
    field names or query syntax are reported at config time."""
    if not config.explore_queries:
        return

    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    errors: list[str] = []
    for explore_query in config.explore_queries:
        try:
            await validate_explore_query(http, config.organization, explore_query, log)
        except HTTPError as err:
            errors.append(
                f"Explore query '{explore_query_stream_name(explore_query.name)}' was rejected by Sentry: {err.message}"
            )

    if errors:
        raise ValidationError(errors)


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
                backfill_issues,
                http,
                config.organization,
                config.advanced.window_size,
                start_date,
            ),
        )

    def open_explore_query_binding(
        explore_query: ExploreQuery,
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
            fetch_changes=functools.partial(
                fetch_explore_query, http, config.organization, explore_query
            ),
            fetch_page=functools.partial(
                backfill_explore_query, http, config.organization, explore_query, start_date
            ),
        )

    # Seed the incremental cursor 1 minute into the MAX_EXPLORE_FULL_FIDELITY_WINDOW
    # to avoid tripping fetch_explore_query's stale cursor warning.
    explore_cutoff = cutoff - MAX_EXPLORE_FULL_FIDELITY_WINDOW + timedelta(minutes=1)

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
                    next_page=None,
                ),
                inc=common.ResourceState.Incremental(
                    cursor=cutoff - timedelta(seconds=1)  # Time bounds are exclusive
                ),
            ),
            initial_config=ResourceConfig(name="Issues", interval=timedelta(minutes=5)),
            schema_inference=True,
        )
    ] + [
        common.Resource(
            name=explore_query_stream_name(explore_query.name),
            key=dataset_pk(explore_query.dataset),
            model=dataset_row_model(explore_query.dataset),
            open=functools.partial(open_explore_query_binding, explore_query),
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(
                    cutoff=explore_cutoff,
                    next_page=None,
                ),
                inc=common.ResourceState.Incremental(
                    # Time bounds are exclusive; start incremental at the later of
                    # start_date and the full-fidelity window's edge.
                    cursor=max(start_date, explore_cutoff) - timedelta(seconds=1),
                ),
            ),
            initial_config=ResourceConfig(
                name=explore_query_stream_name(explore_query.name), interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        )
        for explore_query in config.explore_queries
    ]
