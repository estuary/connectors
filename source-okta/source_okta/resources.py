import functools
from collections.abc import AsyncGenerator, Callable, Iterable
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
from pydantic import TypeAdapter

from .api import (
    INDEX_SETTLE,
    MIN_INTERVAL,
    CommittedHorizon,
    SystemLogAccess,
    backfill_groups,
    backfill_system_log,
    backfill_users,
    base_url,
    fetch_group_deletes,
    fetch_group_updates,
    fetch_system_log,
    fetch_user_deletes,
    fetch_user_updates,
    snapshot_application_users,
    snapshot_applications,
)
from .models import (
    AfterToken,
    ApplicationUser,
    EndpointConfig,
    Group,
    IdEntityDocument,
    OktaApplication,
    OktaSearchableEntity,
    SystemLogEvent,
    User,
)
from .utils import floor_to_ms

# Okta API tokens use their own authorization scheme in place of `Bearer`.
AUTHORIZATION_TOKEN_TYPE = "SSWS"

OktaDocument = IdEntityDocument | OktaApplication | ApplicationUser | SystemLogEvent
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


def validate_intervals(configs: Iterable[ResourceConfig]) -> None:
    too_short = [config.name for config in configs if config.interval < MIN_INTERVAL]
    if too_short:
        minimum = TypeAdapter(timedelta).dump_python(MIN_INTERVAL, mode="json")
        raise ValidationError(
            [f"Interval must be at least {minimum} for: {', '.join(too_short)}."]
        )


@dataclass(frozen=True)
class EntityStream[T: OktaSearchableEntity]:
    """A users or groups stream: the parse model and the fetch functions
    written for it, bound together so they cannot be paired across streams."""

    model: type[T]
    fetch_changes: Callable[
        [HTTPSession, str, CommittedHorizon, Logger, LogCursor],
        AsyncGenerator[T | LogCursor, None],
    ]
    fetch_deletes: Callable[
        [HTTPSession, str, SystemLogAccess, CommittedHorizon, Logger, LogCursor],
        AsyncGenerator[IdEntityDocument | LogCursor, None],
    ]
    fetch_page: Callable[
        [HTTPSession, str, datetime, Logger, PageCursor, LogCursor],
        AsyncGenerator[T | PageCursor, None],
    ]


USERS = EntityStream(User, fetch_user_updates, fetch_user_deletes, backfill_users)
GROUPS = EntityStream(Group, fetch_group_updates, fetch_group_deletes, backfill_groups)

# Subtask ids of a users or groups binding's incremental state.
UPDATES_SUBTASK = "updates"
DELETES_SUBTASK = "deletes"


def id_entity_resource[T: OktaSearchableEntity](
    http: HTTPMixin,
    config: EndpointConfig,
    cutoff: datetime,
    logs_access: SystemLogAccess,
    stream: EntityStream[T],
) -> OktaResource:
    """An id-ordered backfill plus two incremental subtasks, one for updates
    read from `search` and one for hard deletes read from the System Log.

    The deletes tail starts at the first event persisted at or after `cutoff`:
    an object deleted earlier is already gone from `search` when the backfill
    reads it, so it is never captured.

    The deletes subtask only emits deletes up to the updates horizon that is
    already committed, which the two share through a `CommittedHorizon`
    instance. A delete document therefore never lands ahead of a stale copy of
    the same object.
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        assert isinstance(state.inc, dict)
        updates = state.inc[UPDATES_SUBTASK]
        assert updates is not None and isinstance(updates.cursor, datetime)
        committed_horizon = CommittedHorizon(updates.cursor)

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                UPDATES_SUBTASK: functools.partial(
                    stream.fetch_changes,
                    http,
                    config.domain,
                    committed_horizon,
                ),
                DELETES_SUBTASK: functools.partial(
                    stream.fetch_deletes,
                    http,
                    config.domain,
                    logs_access,
                    committed_horizon,
                ),
            },
            fetch_page=functools.partial(
                stream.fetch_page,
                http,
                config.domain,
                config.start_date,
            ),
        )

    return OktaResource(
        name=stream.model.NAME,
        key=["/id"],
        model=IdEntityDocument,
        open=open,
        initial_state=ResourceState(
            inc={
                UPDATES_SUBTASK: ResourceState.Incremental(
                    cursor=cutoff - timedelta(seconds=1)
                ),
                DELETES_SUBTASK: ResourceState.Incremental(
                    cursor=AfterToken.from_timestamp(cutoff).to_cursor()
                ),
            },
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


def application_users_resource(
    http: HTTPMixin, config: EndpointConfig
) -> OktaSnapshotResource:
    """The application_users stream: a full re-list of every app's assignments
    every interval."""

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
                snapshot_application_users, http, config.domain
            ),
        )

    return OktaSnapshotResource(
        name=ApplicationUser.NAME,
        open=open,
        initial_config=ResourceConfig(
            name=ApplicationUser.NAME, interval=timedelta(hours=1)
        ),
        schema_inference=True,
    )


def system_log_resource(
    http: HTTPMixin, config: EndpointConfig, cutoff: datetime
) -> OktaResource:
    """The system_log stream: polling-mode tail plus a bounded backfill.

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
        name="system_log",
        key=["/uuid"],
        model=SystemLogEvent,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(
                cursor=AfterToken.from_timestamp(cutoff).to_cursor()
            ),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name="system_log", interval=timedelta(minutes=5)),
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
    now = datetime.now(tz=UTC)
    log_cutoff = floor_to_ms(now - INDEX_SETTLE)
    entity_cutoff = now.replace(microsecond=0) - INDEX_SETTLE
    # Probed up front so that discovery and validation already warn when
    # deletions cannot be captured; the fetch path reuses the answer.
    log_access = SystemLogAccess(config.domain)

    resources = [
        id_entity_resource(http, config, entity_cutoff, log_access, USERS),
        id_entity_resource(http, config, entity_cutoff, log_access, GROUPS),
        applications_resource(http, config),
        application_users_resource(http, config),
    ]

    if await log_access.allowed(log, http):
        resources.append(system_log_resource(http, config, log_cutoff))

    return resources
