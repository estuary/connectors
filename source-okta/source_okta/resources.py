import functools
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    INDEX_SETTLE,
    SystemLogAccess,
    backfill_groups,
    backfill_system_log,
    backfill_users,
    base_url,
    fetch_groups,
    fetch_system_log,
    fetch_users,
    snapshot_applications,
)
from .models import (
    EndpointConfig,
    Group,
    IdEntityDocument,
    OktaApplication,
    OktaFilterableEntity,
    SystemLogEvent,
    User,
)
from .utils import floor_to_ms, to_rfc3339_ms

# Okta API tokens use their own authorization scheme in place of `Bearer`.
AUTHORIZATION_TOKEN_TYPE = "SSWS"

OktaDocument = IdEntityDocument | OktaApplication | SystemLogEvent
OktaResource = common.Resource[OktaDocument, ResourceConfig, ResourceState]
OktaSnapshotResource = common.SnapshotResource[OktaDocument, ResourceConfig]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API token authenticates against the Okta org."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )
    url = f"{base_url(config.domain)}/{User.PATH}"

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


@dataclass(frozen=True)
class EntityStream[T: OktaFilterableEntity]:
    """A users or groups stream: the parse model and the fetch functions
    written for it, bound together so they cannot be paired across streams."""

    model: type[T]
    fetch_changes: Callable[
        [HTTPSession, str, SystemLogAccess, timedelta, Logger, LogCursor],
        AsyncGenerator[T | IdEntityDocument | LogCursor, None],
    ]
    fetch_page: Callable[
        [HTTPSession, str, datetime, timedelta, Logger, PageCursor, LogCursor],
        AsyncGenerator[T | PageCursor, None],
    ]


USERS = EntityStream(User, fetch_users, backfill_users)
GROUPS = EntityStream(Group, fetch_groups, backfill_groups)


def id_entity_resource[T: OktaFilterableEntity](
    http: HTTPMixin,
    config: EndpointConfig,
    cutoff: datetime,
    logs_access: SystemLogAccess,
    stream: EntityStream[T],
) -> OktaResource:
    """A users or groups stream: windowed backfill plus incremental windows
    that also carry hard deletes read from the System Log.

    Backfill covers `(start_date, cutoff − 1s]` and the cursor is seeded at
    `cutoff − 1s`, so the first emitted second is `cutoff` and the seam is
    gapless. The cutoff sits behind the same INDEX_SETTLE as every incremental
    horizon.
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
                stream.fetch_changes,
                http,
                config.domain,
                logs_access,
                binding.resourceConfig.interval,
            ),
            fetch_page=functools.partial(
                stream.fetch_page,
                http,
                config.domain,
                config.start_date,
                config.advanced.window_size,
            ),
        )

    return OktaResource(
        name=stream.model.NAME,
        key=["/id"],
        model=IdEntityDocument,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1)),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(
            name=stream.model.NAME, interval=timedelta(minutes=5)
        ),
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


def system_log_resource(
    http: HTTPMixin, config: EndpointConfig, cutoff: datetime, has_log_access: bool
) -> OktaResource:
    """The system_log stream: polling-mode tail plus a bounded backfill.

    Backfill covers `published` in [start_date, cutoff − 1 ms]; the first poll
    seeds `since=cutoff`, which Okta applies inclusively on persistence time.
    Persistence never precedes publication, so no event falls between the two.

    Discovered disabled when the token cannot read the System Log, so a new
    capture does not start with a binding that is known to fail.
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
            fetch_changes=functools.partial(fetch_system_log, http, config.domain),
            fetch_page=functools.partial(
                backfill_system_log, http, config.domain, config.start_date
            ),
        )

    return OktaResource(
        name="system_log",
        key=["/uuid"],
        model=SystemLogEvent,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=(to_rfc3339_ms(cutoff),)),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name="system_log", interval=timedelta(minutes=5)),
        schema_inference=True,
        disable=not has_log_access,
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
    now = datetime.now(tz=UTC)
    log_cutoff = floor_to_ms(now - INDEX_SETTLE)
    entity_cutoff = now.replace(microsecond=0) - INDEX_SETTLE
    # Probed up front so that discovery and validation already warn when
    # deletions cannot be captured; the fetch path reuses the answer.
    log_access = SystemLogAccess(config.domain)
    has_log_access = await log_access.allowed(log, http)

    return [
        id_entity_resource(http, config, entity_cutoff, log_access, USERS),
        id_entity_resource(http, config, entity_cutoff, log_access, GROUPS),
        applications_resource(http, config),
        system_log_resource(http, config, log_cutoff, has_log_access),
    ]
