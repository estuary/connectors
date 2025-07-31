import functools
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, ResourceConfig, open_binding
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_backfill_analytics,
    fetch_backfill_reviews,
    fetch_incremental_analytics,
    fetch_incremental_reviews,
)
from .auth import AppleJWTTokenSource
from .client import AppleAppStoreClient
from .models import (
    ApiFetchChangesFn,
    ApiFetchPageFn,
    AppCrashesReportRow,
    AppDownloadsReportRow,
    AppleAnalyticsRow,
    AppleResource,
    AppReview,
    AppSessionsReportRow,
    AppStoreDiscoveryAndEngagementReportRow,
    AppStoreInstallsReportRow,
    ConnectorState,
    EndpointConfig,
    ResourceState,
)

ANALYTICS_RESOURCES: list[type[AppleAnalyticsRow]] = [
    AppSessionsReportRow,
    AppCrashesReportRow,
    AppDownloadsReportRow,
    AppStoreInstallsReportRow,
    AppStoreDiscoveryAndEngagementReportRow,
]
API_RESOURCES: list[
    tuple[type[AppleResource], ApiFetchPageFn, ApiFetchChangesFn]
] = [
    (AppReview, fetch_backfill_reviews, fetch_incremental_reviews),
]


async def validate_credentials(
    log: Logger, http: HTTPMixin, config: EndpointConfig
):
    client = AppleAppStoreClient(log, config.credentials, http)

    try:
        apps = await client.list_apps()
        log.info(f"Successfully validated credentials - found {len(apps)} apps")
    except Exception as err:
        msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err}"
        raise ValidationError([msg])


def _create_initial_state(app_ids: list[str]) -> ResourceState:
    cutoff = datetime.now(tz=UTC)

    initial_state = ResourceState(
        inc={
            app_id: ResourceState.Incremental(cursor=cutoff)
            for app_id in app_ids
        },
        backfill={
            app_id: ResourceState.Backfill(next_page=None, cutoff=cutoff)
            for app_id in app_ids
        },
    )

    return initial_state


def _reconcile_connector_state(
    app_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
):
    if (
        isinstance(state.inc, dict)
        and isinstance(state.backfill, dict)
        and isinstance(initial_state.inc, dict)
        and isinstance(initial_state.backfill, dict)
    ):
        should_checkpoint = False

        for app_id in app_ids:
            inc_state_exists = app_id in state.inc
            backfill_state_exists = app_id in state.backfill

            if not inc_state_exists and not backfill_state_exists:
                task.log.info(
                    f"Initializing new subtask state for app id {app_id}."
                )
                state.inc[app_id] = deepcopy(initial_state.inc[app_id])
                state.backfill[app_id] = deepcopy(
                    initial_state.backfill[app_id]
                )
                should_checkpoint = True

        if should_checkpoint:
            task.log.info(
                f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}."
            )
            task.checkpoint(
                ConnectorState(
                    bindingStateV1={binding.stateKey: state},
                )
            )


async def analytics_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    client = AppleAppStoreClient(log, config.credentials, http)

    if config.app_ids:
        app_ids = config.app_ids
    else:
        app_ids = await client.list_apps()

    initial_state = _create_initial_state(app_ids)

    def open(
        model: type[AppleAnalyticsRow],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        fetch_changes_fns = {}
        fetch_page_fns = {}

        for app_id in app_ids:
            fetch_changes_fns[app_id] = functools.partial(
                fetch_incremental_analytics,
                client,
                app_id,
                model,
            )
            fetch_page_fns[app_id] = functools.partial(
                fetch_backfill_analytics,
                client,
                app_id,
                model,
            )

        _reconcile_connector_state(app_ids, binding, state, initial_state, task)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_changes_fns,
            fetch_page=fetch_page_fns,
        )

    return [
        Resource(
            name=model.resource_name,
            key=sorted(model.primary_keys),
            model=model,
            open=functools.partial(open, model),
            initial_state=initial_state,
            initial_config=ResourceConfig(
                name=model.resource_name, interval=timedelta(hours=1)
            ),
            schema_inference=True,
        )
        for model in ANALYTICS_RESOURCES
    ]


async def api_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    client = AppleAppStoreClient(log, config.credentials, http)

    if config.app_ids:
        app_ids = config.app_ids
    else:
        app_ids = await client.list_apps()

    initial_state = _create_initial_state(app_ids)

    def open(
        app_ids: list[str],
        fetch_changes_fn: ApiFetchChangesFn,
        fetch_page_fn: ApiFetchPageFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        fetch_changes_fns = {}
        fetch_page_fns = {}

        for app_id in app_ids:
            fetch_changes_fns[app_id] = functools.partial(
                fetch_changes_fn,
                client,
                app_id,
            )
            fetch_page_fns[app_id] = functools.partial(
                fetch_page_fn,
                client,
                app_id,
            )

        _reconcile_connector_state(app_ids, binding, state, initial_state, task)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_changes_fns,
            fetch_page=fetch_page_fns,
        )

    return [
        Resource(
            name=model.name,
            key=sorted(model.primary_keys),
            model=model,
            open=functools.partial(
                open, app_ids, fetch_changes_fn, fetch_page_fn
            ),
            initial_state=initial_state,
            initial_config=ResourceConfig(
                name=model.name, interval=timedelta(hours=1)
            ),
            schema_inference=True,
        )
        for model, fetch_page_fn, fetch_changes_fn in API_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = AppleJWTTokenSource(config.credentials)

    return [
        *await analytics_resources(log, http, config),
        *await api_resources(log, http, config),
    ]
