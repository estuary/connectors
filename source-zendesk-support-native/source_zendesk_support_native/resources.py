from datetime import datetime, timedelta, UTC
import functools
from logging import Logger
from typing import Any, AsyncGenerator, Callable

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    AccountSettings,
    AuditLog,
    ClientSideIncrementalOffsetPaginatedResponse,
    ClientSideIncrementalCursorPaginatedResponse,
    ConnectorState,
    CursorExportStatePatch,
    EndpointConfig,
    FilterParam,
    FullRefreshResponse,
    FullRefreshOffsetPaginatedResponse,
    FullRefreshCursorPaginatedResponse,
    FullRefreshResource,
    IncrementalCursorPaginatedResponse,
    IncrementalTimeExportResponse,
    TalkIncrementalExportResponse,
    ResourceConfig,
    ResourceState,
    SideConversation,
    TimestampedResource,
    ZendeskResource,
    EPOCH,
    CLIENT_SIDE_FILTERED_OFFSET_PAGINATED_RESOURCES,
    CLIENT_SIDE_FILTERED_CURSOR_PAGINATED_RESOURCES,
    FULL_REFRESH_RESOURCES,
    FULL_REFRESH_OFFSET_PAGINATED_RESOURCES,
    FULL_REFRESH_CURSOR_PAGINATED_RESOURCES,
    INCREMENTAL_TIME_EXPORT_RESOURCES,
    INCREMENTAL_KEYSET_PAGINATED_RESOURCES,
    IncrementalKeysetPaginatedResponse,
    INCREMENTAL_CURSOR_EXPORT_RESOURCES,
    INCREMENTAL_CURSOR_EXPORT_TYPES,
    INCREMENTAL_CURSOR_PAGINATED_RESOURCES,
    TALK_INCREMENTAL_EXPORT_RESOURCES,
    OAUTH2_SPEC,
    TICKET_CHILD_RESOURCES,
    TicketMetricEventsResponse,
    POST_CHILD_RESOURCES,
    TicketsListResponse,
    UsersListResponse,
    update_oauth_spec,
)
from .api import (
    backfill_audit_logs,
    backfill_incremental_time_export_resources,
    backfill_incremental_keyset_paginated_resources,
    backfill_incremental_keyset_paginated_ticket_children,
    backfill_incremental_cursor_export_resources,
    backfill_incremental_cursor_paginated_resources,
    backfill_satisfaction_ratings,
    backfill_talk_incremental_export_resources,
    backfill_side_conversations,
    backfill_ticket_child_resources,
    backfill_ticket_metrics,
    cursor_export_cursor_to_dt,
    fetch_audit_logs,
    fetch_child_resources_of_tickets,
    fetch_client_side_incremental_offset_paginated_resources,
    fetch_client_side_incremental_cursor_paginated_resources,
    fetch_deleted_tickets,
    fetch_deleted_users,
    fetch_incremental_time_export_resources,
    fetch_incremental_keyset_paginated_resources,
    fetch_incremental_keyset_paginated_ticket_children,
    fetch_incremental_cursor_export_resources,
    fetch_incremental_cursor_paginated_resources,
    fetch_post_child_resources,
    fetch_post_comment_votes,
    fetch_metric_sets_of_tickets,
    fetch_satisfaction_ratings,
    fetch_side_conversations,
    fetch_side_conversations_of_tickets,
    fetch_talk_incremental_export_resources,
    fetch_ticket_child_resources,
    fetch_ticket_metrics,
    snapshot_resources,
    snapshot_offset_paginated_resources,
    snapshot_cursor_paginated_resources,
    url_base,
    dt_to_s,
    INCREMENTAL_LAG,
    TICKET_METRIC_EVENTS_LAG,
    TIME_PARAMETER_DELAY,
)


ENTERPRISE_STREAMS = [
    "audit_logs",
    "account_attributes",
    "schedules",
    "sla_policies",
]


HELP_DESK_STREAMS = [
    "posts",
    "post_comments",
    "post_comment_votes",
    "post_votes",
    "topics",
]


TALK_STREAMS = [
    "calls",
    "call_legs",
]


SIDE_CONVERSATION_STREAMS = [
    "side_conversations",
]


async def _is_enterprise_account(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> bool:
    try:
        await http.request(log, f"{url_base(config.subdomain)}/audit_logs")
    except HTTPError as err:
        if err.code == 403 and "You do not have access to this page." in err.message:
            return False
        else:
            raise err

    return True


async def _is_account_with_help_desk(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> bool:
    try:
        await http.request(log, f"{url_base(config.subdomain)}/community/posts")
    except HTTPError as err:
        if err.code == 404 and "The page you were looking for doesn't exist" in err.message:
            return False
        else:
            raise err

    return True


async def _is_talk_enabled(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> bool:
    try:
        await http.request(log, f"{url_base(config.subdomain)}/channels/voice/stats/account_overview")
    except HTTPError as err:
        if err.code == 403 or err.code == 404:
            return False
        else:
            raise err

    return True


async def _is_talk_export_available(
        log: Logger, http: HTTPMixin, config: EndpointConfig, name: str, path: str
) -> bool:
    # Zendesk is ending third-party access to its incremental exports, and we
    # don't know which client error it will return once it does. So any client
    # error counts as the export being unavailable.
    start_time = dt_to_s(datetime.now(tz=UTC) - 2 * TIME_PARAMETER_DELAY)
    try:
        await http.request(
            log,
            f"{url_base(config.subdomain)}/channels/voice/stats/incremental/{path}",
            params={"start_time": start_time},
        )
    except HTTPError as err:
        if 400 <= err.code < 500:
            log.warning("Talk incremental export is not accessible, so the stream will not be discovered", {
                "stream": name,
                "status_code": err.code,
                "error": err.message,
            })
            return False
        else:
            raise err

    return True


# I'm not sure if this is the best way to determine if side conversations are enabled
# for a given Zendesk instance, but I couldn't find a better check. If we see lots of
# tasks attempting to capture side_conversations but are hitting 4xx errors, we should
# re-evaluate this check.
async def _is_side_conversations_enabled(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> bool:
    body = await http.request(log, f"{url_base(config.subdomain)}/account/settings")
    settings = AccountSettings.model_validate_json(body)
    return settings.settings.side_conversations.show_in_context_panel


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    url = f"{url_base(config.subdomain)}/account/settings"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def audit_logs(
        log: Logger, http: HTTPMixin, config: EndpointConfig
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
            fetch_changes=functools.partial(
                fetch_audit_logs,
                http,
                config.subdomain,
            ),
            fetch_page=functools.partial(
                backfill_audit_logs,
                http,
                config.subdomain,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - INCREMENTAL_LAG

    return common.Resource(
        name="audit_logs",
        key=["/id"],
        model=AuditLog,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="audit_logs", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def ticket_metrics(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        migrated = _migrate_keyset_cursor_to_export(state)

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated state from the keyset sweep to the cursor-based export.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_ticket_metrics,
                http,
                config.subdomain,
            ),
            fetch_page=functools.partial(
                backfill_ticket_metrics,
                http,
                config.subdomain,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(dt_to_s(cutoff)),)

    return common.Resource(
        name="ticket_metrics",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=initial_state),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="ticket_metrics", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[FullRefreshResponse],
            binding: CaptureBinding[ResourceConfig],
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
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                config.subdomain,
                path,
                response_model,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources = [
        common.SnapshotResource(
            name=name,
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
        )
        for (name, path, response_model) in FULL_REFRESH_RESOURCES
    ]

    return resources


def full_refresh_offset_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[FullRefreshOffsetPaginatedResponse],
            binding: CaptureBinding[ResourceConfig],
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
            fetch_snapshot=functools.partial(
                snapshot_offset_paginated_resources,
                http,
                config.subdomain,
                path,
                response_model,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources = [
        common.SnapshotResource(
            name=name,
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
        )
        for (name, path, response_model) in FULL_REFRESH_OFFSET_PAGINATED_RESOURCES
    ]

    return resources


def full_refresh_cursor_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[FullRefreshCursorPaginatedResponse],
            binding: CaptureBinding[ResourceConfig],
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
            fetch_snapshot=functools.partial(
                snapshot_cursor_paginated_resources,
                http,
                config.subdomain,
                path,
                response_model,
        ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources = [
        common.SnapshotResource(
            name=name,
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
        )
        for (name, path, response_model) in FULL_REFRESH_CURSOR_PAGINATED_RESOURCES
    ]

    return resources


def client_side_filtered_offset_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[ClientSideIncrementalOffsetPaginatedResponse],
            binding: CaptureBinding[ResourceConfig],
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
            fetch_changes=functools.partial(
                fetch_client_side_incremental_offset_paginated_resources,
                http,
                config.subdomain,
                path,
                response_model,
            ),
        )

    resources = [
        common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, path, response_model),
            initial_state=ResourceState(
                # Set the initial state of these streams to be the epoch so all results are initially 
                # emitted, then only updated results are emitted on subsequent sweeps.
                inc=ResourceState.Incremental(cursor=EPOCH)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=30)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in CLIENT_SIDE_FILTERED_OFFSET_PAGINATED_RESOURCES
    ]

    return resources


def client_side_filtered_cursor_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            path: str,
            additional_query_params: dict[str, Any] | None,
            response_model: type[ClientSideIncrementalCursorPaginatedResponse],
            binding: CaptureBinding[ResourceConfig],
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
            fetch_changes=functools.partial(
                fetch_client_side_incremental_cursor_paginated_resources,
                http,
                config.subdomain,
                path,
                additional_query_params,
                response_model,
            ),
        )

    resources = [
        common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, path, additional_query_params, response_model),
            initial_state=ResourceState(
                # Set the initial state of these streams to be the epoch so all results are initially 
                # emitted, then only updated results are emitted on subsequent sweeps.
                inc=ResourceState.Incremental(cursor=EPOCH)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        )
        for (name, path, additional_query_params, response_model) in CLIENT_SIDE_FILTERED_CURSOR_PAGINATED_RESOURCES
    ]

    return resources


def satisfaction_ratings(
        log: Logger, http: HTTPMixin, config: EndpointConfig
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
            fetch_changes=functools.partial(
                fetch_satisfaction_ratings,
                http,
                config.subdomain,
            ),
            fetch_page=functools.partial(
                backfill_satisfaction_ratings,
                http,
                config.subdomain,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - max(INCREMENTAL_LAG, TIME_PARAMETER_DELAY)

    return common.Resource(
        name="satisfaction_ratings",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="satisfaction_ratings", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def incremental_cursor_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        path: str,
        filter_param: FilterParam,
        cursor_field: str,
        response_model: type[IncrementalCursorPaginatedResponse],
        lag: timedelta,
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
            fetch_changes=functools.partial(
                fetch_incremental_cursor_paginated_resources,
                http,
                config.subdomain,
                path,
                filter_param,
                cursor_field,
                response_model,
                lag,
            ),
            fetch_page=functools.partial(
                backfill_incremental_cursor_paginated_resources,
                http,
                config.subdomain,
                path,
                filter_param,
                cursor_field,
                response_model,
                config.start_date,
            )
        )

    resources: list[common.Resource] = []

    for (name, path, filter_param, cursor_field, response_model) in INCREMENTAL_CURSOR_PAGINATED_RESOURCES:
        cutoff = datetime.now(tz=UTC) - max(INCREMENTAL_LAG, TIME_PARAMETER_DELAY)

        resources.append(
            common.Resource(
                name=name,
                key=["/id"],
                model=ZendeskResource,
                open=functools.partial(open, path, filter_param, cursor_field, response_model, INCREMENTAL_LAG),
                initial_state=ResourceState(
                    inc=ResourceState.Incremental(cursor=cutoff),
                    backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
                ),
                initial_config=ResourceConfig(
                    name=name, interval=timedelta(minutes=5)
                ),
                schema_inference=True,
            )
        )

    return resources


def ticket_metric_events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
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
            fetch_changes=functools.partial(
                fetch_incremental_cursor_paginated_resources,
                http,
                config.subdomain,
                "incremental/ticket_metric_events",
                FilterParam.START_TIME,
                "time",
                TicketMetricEventsResponse,
                TICKET_METRIC_EVENTS_LAG,
            ),
            fetch_page=functools.partial(
                backfill_incremental_cursor_paginated_resources,
                http,
                config.subdomain,
                "incremental/ticket_metric_events",
                FilterParam.START_TIME,
                "time",
                TicketMetricEventsResponse,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - max(TICKET_METRIC_EVENTS_LAG, TIME_PARAMETER_DELAY)

    return common.Resource(
        name="ticket_metric_events",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="ticket_metric_events", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def incremental_time_export_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        name: str,
        path: str,
        response_model: type[IncrementalTimeExportResponse],
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
            fetch_changes=functools.partial(
                fetch_incremental_time_export_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
            ),
            fetch_page=functools.partial(
                backfill_incremental_time_export_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - max(INCREMENTAL_LAG, TIME_PARAMETER_DELAY)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, name, path, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in INCREMENTAL_TIME_EXPORT_RESOURCES
    ]

    return resources


# Nothing calls this yet. A later PR adds the feature flag that switches
# organizations from the time-based export to these resources (#5357).
def incremental_keyset_paginated_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        name: str,
        path: str,
        response_model: type[IncrementalKeysetPaginatedResponse],
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
            fetch_changes=functools.partial(
                fetch_incremental_keyset_paginated_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
            ),
            fetch_page=functools.partial(
                backfill_incremental_keyset_paginated_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
                config.start_date,
            )
        )

    cutoff = (datetime.now(tz=UTC) - INCREMENTAL_LAG).replace(microsecond=0)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, name, path, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in INCREMENTAL_KEYSET_PAGINATED_RESOURCES
    ]

    return resources


def _migrate_export_state_to_keyset(
    state: ResourceState,
    subtask_ids: tuple[str, ...],
    margin: timedelta,
) -> ResourceState | None:
    """
    Returns the state to checkpoint when `state` is the cursor-based export's,
    or None when it's already the keyset sweep's. Each subtask and an
    in-progress backfill start `margin` before the export's cursor.
    """
    if isinstance(state.inc, dict):
        return None

    assert isinstance(state.inc, ResourceState.Incremental) and isinstance(state.inc.cursor, tuple)
    export_cursor = state.inc.cursor[0]
    assert isinstance(export_cursor, str)

    cursor = cursor_export_cursor_to_dt(export_cursor) - margin

    inc: dict[str, ResourceState.Incremental | None] = {
        # The runtime merge-patches checkpoints, so None removes the export's
        # cursor from the saved state.
        "cursor": None,
    }
    for subtask_id in subtask_ids:
        inc[subtask_id] = ResourceState.Incremental(cursor=cursor)

    migrated = ResourceState(inc=inc)

    if isinstance(state.backfill, ResourceState.Backfill) and isinstance(state.backfill.next_page, str):
        page = cursor_export_cursor_to_dt(state.backfill.next_page) - margin
        migrated.backfill = ResourceState.Backfill(
            cutoff=state.backfill.cutoff,
            next_page=dt_to_s(page),
        )

    return migrated


# Nothing calls this yet. A later PR adds the feature flag that switches
# users from the cursor-based export to this resource (#5357).
def users(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # The export's cursor can stop partway through a second, so the sweep
        # starts a second earlier and reads that whole second again.
        migrated = _migrate_export_state_to_keyset(
            state, ("users", "deleted_users"), timedelta(seconds=1),
        )

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated users state from the cursor-based export to the keyset sweep.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                "users": functools.partial(
                    fetch_incremental_keyset_paginated_resources,
                    http,
                    config.subdomain,
                    "users",
                    "users",
                    UsersListResponse,
                ),
                "deleted_users": functools.partial(
                    fetch_deleted_users,
                    http,
                    config.subdomain,
                ),
            },
            fetch_page=functools.partial(
                backfill_incremental_keyset_paginated_resources,
                http,
                config.subdomain,
                "users",
                "users",
                UsersListResponse,
                config.start_date,
            )
        )

    cutoff = (datetime.now(tz=UTC) - INCREMENTAL_LAG).replace(microsecond=0)

    return common.Resource(
        name="users",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc={
                "users": ResourceState.Incremental(cursor=cutoff),
                "deleted_users": ResourceState.Incremental(cursor=cutoff),
            },
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="users", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


# Nothing calls this yet. A later PR adds the feature flag that switches
# tickets from the cursor-based export to this resource (#5357).
def tickets(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # The export orders tickets by generated_timestamp, which can be later
        # than a change's updated_at. So a ticket updated before the export's
        # cursor may not have been exported yet, and the sweep starts
        # INCREMENTAL_LAG earlier.
        migrated = _migrate_export_state_to_keyset(
            state, ("tickets", "deleted_tickets"), INCREMENTAL_LAG,
        )

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated tickets state from the cursor-based export to the keyset sweep.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                "tickets": functools.partial(
                    fetch_incremental_keyset_paginated_resources,
                    http,
                    config.subdomain,
                    "tickets",
                    "tickets",
                    TicketsListResponse,
                ),
                "deleted_tickets": functools.partial(
                    fetch_deleted_tickets,
                    http,
                    config.subdomain,
                ),
            },
            fetch_page=functools.partial(
                backfill_incremental_keyset_paginated_resources,
                http,
                config.subdomain,
                "tickets",
                "tickets",
                TicketsListResponse,
                config.start_date,
            )
        )

    cutoff = (datetime.now(tz=UTC) - INCREMENTAL_LAG).replace(microsecond=0)

    return common.Resource(
        name="tickets",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc={
                "tickets": ResourceState.Incremental(cursor=cutoff),
                "deleted_tickets": ResourceState.Incremental(cursor=cutoff),
            },
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="tickets", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def _migrate_export_cursor_to_keyset(state: ResourceState, margin: timedelta) -> ResourceState | None:
    """
    Returns the state to checkpoint when `state` holds a cursor-based export
    cursor, or None when it already holds the keyset sweep's datetime. The
    cursor and an in-progress backfill start `margin` before the export's.
    """
    assert isinstance(state.inc, ResourceState.Incremental)

    if isinstance(state.inc.cursor, datetime):
        return None

    assert isinstance(state.inc.cursor, tuple)
    export_cursor = state.inc.cursor[0]
    assert isinstance(export_cursor, str)

    migrated = ResourceState(
        inc=ResourceState.Incremental(cursor=cursor_export_cursor_to_dt(export_cursor) - margin),
    )

    if isinstance(state.backfill, ResourceState.Backfill) and isinstance(state.backfill.next_page, str):
        page = cursor_export_cursor_to_dt(state.backfill.next_page) - margin
        migrated.backfill = ResourceState.Backfill(
            cutoff=state.backfill.cutoff,
            next_page=dt_to_s(page),
        )

    return migrated


# Nothing calls this yet. A later PR adds the feature flag that switches the
# ticket child streams from the cursor-based export to these resources (#5357).
def incremental_keyset_paginated_ticket_child_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def open(
        fetch_children: Callable[[list[int], Logger], AsyncGenerator[Any, None]],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        migrated = _migrate_export_cursor_to_keyset(state, INCREMENTAL_LAG)

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated state from the cursor-based export to the keyset sweep.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_incremental_keyset_paginated_ticket_children,
                http,
                config.subdomain,
                fetch_children,
                INCREMENTAL_LAG,
            ),
            fetch_page=functools.partial(
                backfill_incremental_keyset_paginated_ticket_children,
                http,
                config.subdomain,
                fetch_children,
                config.start_date,
            )
        )

    cutoff = (datetime.now(tz=UTC) - INCREMENTAL_LAG).replace(microsecond=0)

    def resource(
        name: str,
        model: type[FullRefreshResource],
        fetch_children: Callable[[list[int], Logger], AsyncGenerator[Any, None]],
    ) -> common.Resource:
        return common.Resource(
            name=name,
            key=["/id"],
            model=model,
            open=functools.partial(open, fetch_children),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )

    return [
        *(
            resource(
                name,
                ZendeskResource,
                functools.partial(fetch_child_resources_of_tickets, http, config.subdomain, path, response_model),
            )
            for (name, path, response_model) in TICKET_CHILD_RESOURCES
        ),
        resource(
            "side_conversations",
            SideConversation,
            functools.partial(fetch_side_conversations_of_tickets, http, config.subdomain),
        ),
        resource(
            "ticket_metrics",
            ZendeskResource,
            functools.partial(fetch_metric_sets_of_tickets, http, config.subdomain),
        ),
    ]


def talk_incremental_export_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        name: str,
        path: str,
        response_model: type[TalkIncrementalExportResponse],
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
            fetch_changes=functools.partial(
                fetch_talk_incremental_export_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
            ),
            fetch_page=functools.partial(
                backfill_talk_incremental_export_resources,
                http,
                config.subdomain,
                name,
                path,
                response_model,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - max(INCREMENTAL_LAG, TIME_PARAMETER_DELAY)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, name, path, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in TALK_INCREMENTAL_EXPORT_RESOURCES
    ]

    return resources


def _migrate_keyset_state_to_export(state: ResourceState) -> CursorExportStatePatch | None:
    """
    Returns the patch to checkpoint when `state` is the keyset sweep's, or None
    when it's already the cursor-based export's.
    """
    if not isinstance(state.inc, dict):
        return None

    subtask_cursors: list[datetime] = []
    for subtask in state.inc.values():
        if subtask is not None:
            assert isinstance(subtask.cursor, datetime)
            subtask_cursors.append(subtask.cursor)

    # A deletion subtask can trail the other subtask by over an hour, and list
    # endpoints never return deleted records. Starting the export at the
    # earliest subtask cursor reads the deletions in between again.
    patch = CursorExportStatePatch(
        inc={
            **{subtask_id: None for subtask_id in state.inc},
            "cursor": (str(dt_to_s(min(subtask_cursors))),),
        },
    )

    if isinstance(state.backfill, ResourceState.Backfill) and isinstance(state.backfill.next_page, int):
        patch.backfill = ResourceState.Backfill(
            cutoff=state.backfill.cutoff,
            next_page=str(state.backfill.next_page),
        )

    return patch


def _migrate_keyset_cursor_to_export(state: ResourceState) -> ResourceState | None:
    """
    Returns the state to checkpoint when `state` holds the keyset sweep's
    datetime cursor, or None when it already holds a cursor-based export cursor.
    """
    assert isinstance(state.inc, ResourceState.Incremental)

    if not isinstance(state.inc.cursor, datetime):
        return None

    migrated = ResourceState(
        inc=ResourceState.Incremental(cursor=(str(dt_to_s(state.inc.cursor)),)),
    )

    if isinstance(state.backfill, ResourceState.Backfill) and isinstance(state.backfill.next_page, int):
        migrated.backfill = ResourceState.Backfill(
            cutoff=state.backfill.cutoff,
            next_page=str(state.backfill.next_page),
        )

    return migrated


def incremental_cursor_export_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def open(
        name: INCREMENTAL_CURSOR_EXPORT_TYPES,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        patch = _migrate_keyset_state_to_export(state)

        if patch is not None:
            cursor = patch.inc["cursor"]
            assert cursor is not None
            state.inc = ResourceState.Incremental(cursor=cursor)
            if patch.backfill is not None:
                state.backfill = patch.backfill

            task.log.info(f"Migrated {name} state from the keyset sweep to the cursor-based export.", {
                "migrated_state": patch.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                common.ConnectorState[CursorExportStatePatch](bindingStateV1={binding.stateKey: patch}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_incremental_cursor_export_resources,
                http,
                config.subdomain,
                name,
            ),
            fetch_page=functools.partial(
                backfill_incremental_cursor_export_resources,
                http,
                config.subdomain,
                name,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(dt_to_s(cutoff)),)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=ZendeskResource,
            open=functools.partial(open, name),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=initial_state),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name) in INCREMENTAL_CURSOR_EXPORT_RESOURCES
    ]

    return resources


def ticket_child_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def open(
        path: str,
        response_model: type[IncrementalCursorPaginatedResponse],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        migrated = _migrate_keyset_cursor_to_export(state)

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated state from the keyset sweep to the cursor-based export.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_ticket_child_resources,
                http,
                config.subdomain,
                path,
                response_model,
            ),
            fetch_page=functools.partial(
                backfill_ticket_child_resources,
                http,
                config.subdomain,
                path,
                response_model,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(dt_to_s(cutoff)),)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=ZendeskResource,
            open=functools.partial(open, path, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=initial_state),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in TICKET_CHILD_RESOURCES
    ]

    return resources


def side_conversations(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        migrated = _migrate_keyset_cursor_to_export(state)

        if migrated is not None:
            state.inc = migrated.inc
            if migrated.backfill is not None:
                state.backfill = migrated.backfill

            task.log.info("Migrated state from the keyset sweep to the cursor-based export.", {
                "migrated_state": migrated.model_dump(mode="json", exclude_unset=True),
            })
            await task.checkpoint(
                ConnectorState(bindingStateV1={binding.stateKey: migrated}),
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_side_conversations,
                http,
                config.subdomain,
            ),
            fetch_page=functools.partial(
                backfill_side_conversations,
                http,
                config.subdomain,
                config.start_date,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as a Unix timestamp.
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(dt_to_s(cutoff)),)

    return common.Resource(
        name="side_conversations",
        key=["/id"],
        model=SideConversation,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=initial_state),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
        ),
        initial_config=ResourceConfig(
            name="side_conversations", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def post_child_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        path_segment: str,
        response_model: type[IncrementalCursorPaginatedResponse],
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
            fetch_changes=functools.partial(
                fetch_post_child_resources,
                http,
                config.subdomain,
                path_segment,
                response_model,
            ),
        )

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=ZendeskResource,
            open=functools.partial(open, path_segment, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=30)
            ),
            schema_inference=True,
        )
        for (name, path_segment, response_model) in POST_CHILD_RESOURCES
    ]

    return resources


def post_comment_votes(
    log: Logger, http: HTTPMixin, config: EndpointConfig
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
            fetch_changes=functools.partial(
                fetch_post_comment_votes,
                http,
                config.subdomain,
            ),
        )

    return common.Resource(
        name="post_comment_votes",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name="post_comment_votes", interval=timedelta(minutes=30)
        ),
        schema_inference=True,
    )


def _generate_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig,
) -> list[common.Resource]:
    return [
        audit_logs(log, http, config),
        ticket_metrics(log, http, config),
        *full_refresh_resources(log, http, config),
        *full_refresh_offset_paginated_resources(log, http, config),
        *full_refresh_cursor_paginated_resources(log, http, config),
        *client_side_filtered_offset_paginated_resources(log, http, config),
        *client_side_filtered_cursor_paginated_resources(log, http, config),
        satisfaction_ratings(log, http, config),
        *incremental_cursor_paginated_resources(log, http, config),
        ticket_metric_events(log, http, config),
        *incremental_time_export_resources(log, http, config),
        *talk_incremental_export_resources(log, http, config),
        *incremental_cursor_export_resources(log, http, config),
        *ticket_child_resources(log, http, config),
        side_conversations(log, http, config),
        *post_child_resources(log, http, config),
        post_comment_votes(log, http, config),
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    update_oauth_spec(config.subdomain)
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    resources = _generate_resources(log, http, config)

    excluded_streams: set[str] = set()

    if not await _is_enterprise_account(log, http, config):
        excluded_streams.update(ENTERPRISE_STREAMS)

    if not await _is_account_with_help_desk(log, http, config):
        excluded_streams.update(HELP_DESK_STREAMS)

    if not await _is_talk_enabled(log, http, config):
        excluded_streams.update(TALK_STREAMS)
    else:
        for (name, path, _) in TALK_INCREMENTAL_EXPORT_RESOURCES:
            if not await _is_talk_export_available(log, http, config, name, path):
                excluded_streams.add(name)

    if not await _is_side_conversations_enabled(log, http, config):
        excluded_streams.update(SIDE_CONVERSATION_STREAMS)

    if excluded_streams:
        resources = [r for r in resources if r.name not in excluded_streams]

    return resources


async def enabled_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig, bindings: list[common._ResolvableBinding]
) -> list[common.Resource]:
    update_oauth_spec(config.subdomain)
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    enabled_binding_names: list[str] = []

    for binding in bindings:
        path: list[str] = binding.resourceConfig.path()
        enabled_binding_names.append(path[0])

    enabled_resources: list[common.Resource] = [r for r in _generate_resources(log, http, config) if r.name in enabled_binding_names]

    return enabled_resources
