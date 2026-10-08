import functools
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from pydantic import TypeAdapter

from .api import (
    BACKFILL_SETTLE,
    backfill_system_log,
    base_url,
    fetch_system_log,
    snapshot_applications,
)
from .models import (
    AfterToken,
    EndpointConfig,
    OktaApplication,
    ResourceConfig,
    SystemLogEvent,
)
from .utils import floor_to_ms

# Okta API tokens use their own authorization scheme in place of `Bearer`.
AUTHORIZATION_TOKEN_TYPE = "SSWS"

OktaDocument = OktaApplication | SystemLogEvent
OktaResource = common.Resource[OktaDocument, ResourceConfig, ResourceState]
OktaSnapshotResource = common.SnapshotResource[OktaDocument, ResourceConfig]

# Right after a poll, the system_log tail's cursor already trails the present
# by Okta's ~45-65 s polling holdback, so a shorter interval can't hold back
# the CDK's immediate re-poll. No other stream gains from a shorter one.
MIN_INTERVAL = timedelta(minutes=2)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API token authenticates against the Okta org."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )
    url = f"{base_url(config.domain)}/users"

    try:
        _ = await http.request(log, url, params={"limit": 1})
    except HTTPError as err:
        if err.code == 401:
            msg = (
                "Invalid API token. Confirm the token is correct, was created in the "
                f"Okta org at {config.domain}, and has not been revoked after 30 days "
                f"without use.\n\n{err.message}"
            )
        elif err.code == 403:
            msg = (
                "The API token is valid but its admin role cannot list users. Create "
                f"the token from an account with a read-only admin role or higher.\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating API token.\n\n{err.message}"

        raise ValidationError([msg])


def validate_intervals(configs: Iterable[ResourceConfig]) -> None:
    too_short = [config.name for config in configs if config.interval < MIN_INTERVAL]
    if too_short:
        minimum = TypeAdapter(timedelta).dump_python(MIN_INTERVAL, mode="json")
        raise ValidationError(
            [f"Interval must be at least {minimum} for: {', '.join(too_short)}."]
        )


def system_log_resource(
    http: HTTPMixin, config: EndpointConfig, cutoff: datetime
) -> OktaResource:
    """The System_Log stream: polling-mode tail plus a bounded backfill.

    Backfill covers `published` in [start_date, cutoff − 1 ms]; the first poll
    starts at the first event persisted at or after `cutoff`.
    Persistence never precedes publication, so no event falls between the two.
    """

    def open(
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
                fetch_system_log,
                http,
                config.domain,
                binding.resourceConfig.interval,
            ),
            fetch_page=functools.partial(
                backfill_system_log, http, config.domain, config.start_date
            ),
        )

    return OktaResource(
        name="System_Log",
        key=["/uuid"],
        model=SystemLogEvent,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(
                cursor=AfterToken.from_timestamp(cutoff).to_cursor()
            ),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name="System_Log", interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def applications_resource(
    http: HTTPMixin, config: EndpointConfig
) -> OktaSnapshotResource:
    """The applications stream: a full re-list every interval; deletions are inferred."""

    def open(
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
                snapshot_applications, http, config.domain
            ),
        )

    return OktaSnapshotResource(
        name="applications",
        open=open,
        initial_config=ResourceConfig(
            name="applications", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[OktaResource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )
    cutoff = floor_to_ms(datetime.now(tz=UTC) - BACKFILL_SETTLE)

    return [
        applications_resource(http, config),
        system_log_resource(http, config, cutoff),
    ]
