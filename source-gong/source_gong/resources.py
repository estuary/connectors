import functools
from datetime import timedelta, datetime, UTC
from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import ResourceConfigWithSchedule
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from source_gong.models import (
    EndpointConfig,
    GongResource,
    IncrementalGongResource,
    Call,
    User,
    Scorecard,
    ScorecardDefinition,
)
from source_gong.api import (
    GongRateLimiter,
    snapshot_resource,
    fetch_page,
    fetch_changes,
    fetch_calls_changes,
)

FULL_REFRESH_MODELS: list[type[GongResource]] = [ScorecardDefinition]
INCREMENTAL_MODELS: list[type[IncrementalGongResource]] = [User, Scorecard]


async def validate_credentials(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    rate_limiter: GongRateLimiter,
) -> None:
    """Validate credentials by making a test API call."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )
    url = f"{config.base_url}/v2/users"

    try:
        await rate_limiter.acquire()
        await http.request(log, url)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    f"Invalid credentials. Please confirm the provided Access Key and Access Key Secret are correct.\n\n{err.message}"
                ]
            )
        else:
            raise ValidationError([f"Encountered error validating credentials.\n\n{err.message}"])


async def full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    rate_limiter: GongRateLimiter,
) -> list[common.Resource]:
    """Build full refresh resource definitions."""

    def open(
        cls: type[GongResource],
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ) -> None:
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resource,
                cls,
                cls.URL_PATH,
                cls.ITEMS_KEY,
                http,
                rate_limiter,
                config.base_url,
            ),
            tombstone=GongResource(_meta=GongResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=cls.NAME,
            key=cls.KEY,
            model=cls,
            open=functools.partial(open, cls),
            initial_state=common.ResourceState(),
            initial_config=ResourceConfigWithSchedule(name=cls.NAME, interval=timedelta(hours=1)),
            schema_inference=True,
        )
        for cls in FULL_REFRESH_MODELS
    ]


async def incremental_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    rate_limiter: GongRateLimiter,
) -> list[common.Resource]:
    """Build incremental resource definitions."""

    def open(
        cls: type[IncrementalGongResource],
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ) -> None:
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_page,
                cls,
                http,
                rate_limiter,
                config.base_url,
                config.start_date,
            ),
            fetch_changes=functools.partial(
                fetch_changes,
                cls,
                http,
                rate_limiter,
                config.base_url,
            ),
        )

    def open_calls(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ) -> None:
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_page,
                Call,
                http,
                rate_limiter,
                config.base_url,
                config.start_date,
            ),
            fetch_changes=functools.partial(
                fetch_calls_changes,
                http,
                rate_limiter,
                config.base_url,
                config.calls_lookback_window,
            ),
        )

    cutoff = datetime.now(tz=UTC)

    resources: list[common.Resource] = [
        common.Resource(
            name=Call.NAME,
            key=Call.KEY,
            model=Call,
            open=open_calls,
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=int(cutoff.timestamp())),
                backfill=common.ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfigWithSchedule(
                name=Call.NAME,
                interval=timedelta(minutes=2),
            ),
            schema_inference=True,
        ),
    ]

    for cls in INCREMENTAL_MODELS:
        schedule = "0 0 * * *" if cls is User else None
        config_kwargs: dict = {
            "name": cls.NAME,
            "interval": timedelta(minutes=2),
        }
        if schedule:
            config_kwargs["schedule"] = schedule

        resources.append(
            common.Resource(
                name=cls.NAME,
                key=cls.KEY,
                model=cls,
                open=functools.partial(open, cls),
                initial_state=common.ResourceState(
                    inc=common.ResourceState.Incremental(cursor=int(cutoff.timestamp())),
                    backfill=common.ResourceState.Backfill(cutoff=cutoff, next_page=None),
                ),
                initial_config=ResourceConfigWithSchedule(**config_kwargs),
                schema_inference=True,
            )
        )

    return resources


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    rate_limiter: GongRateLimiter,
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )

    return [
        *await full_refresh_resources(log, http, config, rate_limiter),
        *await incremental_resources(log, http, config, rate_limiter),
    ]
