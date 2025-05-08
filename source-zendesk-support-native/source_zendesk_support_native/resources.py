from datetime import datetime, timedelta, UTC
import functools
from logging import Logger
from typing import Any

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    AuditLog,
    ClientSideIncrementalOffsetPaginatedResponse,
    ClientSideIncrementalCursorPaginatedResponse,
    EndpointConfig,
    FilterParam,
    FullRefreshResponse,
    FullRefreshOffsetPaginatedResponse,
    FullRefreshCursorPaginatedResponse,
    FullRefreshResource,
    IncrementalCursorPaginatedResponse,
    IncrementalTimeExportResponse,
    ResourceConfig,
    ResourceState,
    TimestampedResource,
    ZendeskResource,
    EPOCH,
    CLIENT_SIDE_FILTERED_OFFSET_PAGINATED_RESOURCES,
    CLIENT_SIDE_FILTERED_CURSOR_PAGINATED_RESOURCES,
    FULL_REFRESH_RESOURCES,
    FULL_REFRESH_OFFSET_PAGINATED_RESOURCES,
    FULL_REFRESH_CURSOR_PAGINATED_RESOURCES,
    INCREMENTAL_TIME_EXPORT_RESOURCES,
    INCREMENTAL_CURSOR_EXPORT_RESOURCES,
    INCREMENTAL_CURSOR_EXPORT_TYPES,
    INCREMENTAL_CURSOR_PAGINATED_RESOURCES,
    OAUTH2_SPEC,
    TICKET_CHILD_RESOURCES,
    POST_CHILD_RESOURCES,
)
from .api import (
    backfill_audit_logs,
    backfill_incremental_time_export_resources,
    backfill_incremental_cursor_export_resources,
    backfill_incremental_cursor_paginated_resources,
    backfill_satisfaction_ratings,
    backfill_ticket_child_resources,
    backfill_ticket_metrics,
    fetch_audit_logs,
    fetch_client_side_incremental_offset_paginated_resources,
    fetch_client_side_incremental_cursor_paginated_resources,
    fetch_incremental_time_export_resources,
    fetch_incremental_cursor_export_resources,
    fetch_incremental_cursor_paginated_resources,
    fetch_post_child_resources,
    fetch_post_comment_votes,
    fetch_satisfaction_ratings,
    fetch_ticket_child_resources,
    fetch_ticket_metrics,
    snapshot_resources,
    snapshot_offset_paginated_resources,
    snapshot_cursor_paginated_resources,
    url_base,
    _dt_to_s,
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

    cutoff = datetime.now(tz=UTC)

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
                fetch_ticket_metrics,
                http,
                config.subdomain,
                config.advanced.incremental_export_page_size,
            ),
            fetch_page=functools.partial(
                backfill_ticket_metrics,
                http,
                config.subdomain,
                config.start_date,
                config.advanced.incremental_export_page_size,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(_dt_to_s(cutoff)),)

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
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
            schema_inference=True,
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
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
            schema_inference=True,
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
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=60)
            ),
            schema_inference=True,
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
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY

    return common.Resource(
        name="satisfaction_ratings",
        key=["/id"],
        model=ZendeskResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=_dt_to_s(config.start_date))
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

    cutoff = datetime.now(tz=UTC)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=ZendeskResource,
            open=functools.partial(open, path, filter_param, cursor_field, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None)
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, filter_param, cursor_field, response_model) in INCREMENTAL_CURSOR_PAGINATED_RESOURCES
    ]

    return resources


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
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
            open=functools.partial(open, name, path, response_model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=_dt_to_s(config.start_date))
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model) in INCREMENTAL_TIME_EXPORT_RESOURCES
    ]

    return resources


def incremental_cursor_export_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        name: INCREMENTAL_CURSOR_EXPORT_TYPES,
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
                fetch_incremental_cursor_export_resources,
                http,
                config.subdomain,
                name,
                config.advanced.incremental_export_page_size,
            ),
            fetch_page=functools.partial(
                backfill_incremental_cursor_export_resources,
                http,
                config.subdomain,
                name,
                config.start_date,
                config.advanced.incremental_export_page_size,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(_dt_to_s(cutoff)),)

    resources = [
            common.Resource(
            name=name,
            key=["/id"],
            model=TimestampedResource,
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

    def open(
        path: str,
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
                fetch_ticket_child_resources,
                http,
                config.subdomain,
                path,
                response_model,
                config.advanced.incremental_export_page_size,
            ),
            fetch_page=functools.partial(
                backfill_ticket_child_resources,
                http,
                config.subdomain,
                path,
                response_model,
                config.start_date,
                config.advanced.incremental_export_page_size,
            )
        )

    cutoff = datetime.now(tz=UTC) - TIME_PARAMETER_DELAY
    # Initial state is the stringified version of the cutoff as an timestamp. Ex: "1738126891"
    # This is done to maintain the strictly increasing nature of yielded LogCursors.
    initial_state = (str(_dt_to_s(cutoff)),)

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


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    resources = [
        audit_logs(log, http, config),
        ticket_metrics(log, http, config),
        *full_refresh_resources(log, http, config),
        *full_refresh_offset_paginated_resources(log, http, config),
        *full_refresh_cursor_paginated_resources(log, http, config),
        *client_side_filtered_offset_paginated_resources(log, http, config),
        *client_side_filtered_cursor_paginated_resources(log, http, config),
        satisfaction_ratings(log, http, config),
        *incremental_cursor_paginated_resources(log, http, config),
        *incremental_time_export_resources(log, http, config),
        *incremental_cursor_export_resources(log, http, config),
        *ticket_child_resources(log, http, config),
        *post_child_resources(log, http, config),
        post_comment_votes(log, http, config),
    ]

    if not await _is_enterprise_account(log, http, config):
        resources = [r for r in resources if r.name not in ENTERPRISE_STREAMS]

    if not await _is_account_with_help_desk(log, http, config):
        resources = [r for r in resources if r.name not in HELP_DESK_STREAMS]

    return resources
