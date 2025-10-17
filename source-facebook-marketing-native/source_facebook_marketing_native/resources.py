import functools
from datetime import datetime, UTC
from logging import Logger
from copy import deepcopy
from typing import Callable

from estuary_cdk.capture.common import (
    Resource,
    ResourceState,
    open_binding,
    ResourceConfig,
    FetchSnapshotFn,
)
from estuary_cdk.capture import Task
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, TokenSource

from estuary_cdk.flow import ValidationError

from .auth import validate_credentials, validate_access_to_accounts
from .client import FacebookAPIClient
from .job_manager import FacebookInsightsJobManager
from .api import (
    snapshot_resource,
    snapshot_ad_creatives,
    fetch_page,
    fetch_changes,
    fetch_page_reverse_order,
    fetch_changes_reverse_order,
    fetch_page_activities,
    fetch_changes_activities,
    fetch_page_insights,
    fetch_changes_insights,
)
from .models import (
    EndpointConfig,
    OAUTH2_SPEC,
    ConnectorState,
    FacebookResource,
    FacebookInsightsResource,
    FullRefreshFetchFn,
    IncrementalFetchPageFn,
    IncrementalFetchChangesFn,
    AdAccount,
    AdCreative,
    CustomConversions,
    Campaigns,
    AdSets,
    Ads,
    Activities,
    Images,
    Videos,
    AdsInsights,
    AdsInsightsAgeAndGender,
    AdsInsightsCountry,
    AdsInsightsRegion,
    AdsInsightsDma,
    AdsInsightsPlatformAndDevice,
    AdsInsightsActionType,
    InsightsConfig,
    build_custom_ads_insights_model,
)
from .fields import (
    AD_INSIGHTS_VALID_FIELDS,
    AD_INSIGHTS_VALID_BREAKDOWNS,
    AD_INSIGHTS_VALID_ACTION_BREAKDOWNS,
)


FULL_REFRESH_RESOURCES: set[tuple[type[FacebookResource], FullRefreshFetchFn]] = {
    (AdAccount, snapshot_resource),
    (AdCreative, snapshot_ad_creatives),
    (CustomConversions, snapshot_resource),
}

STANDARD_INCREMENTAL_RESOURCES: set[
    tuple[type[FacebookResource], IncrementalFetchPageFn, IncrementalFetchChangesFn]
] = {
    (Campaigns, fetch_page, fetch_changes),
    (AdSets, fetch_page, fetch_changes),
    (Ads, fetch_page, fetch_changes),
    (Activities, fetch_page_activities, fetch_changes_activities),
}

REVERSE_ORDER_INCREMENTAL_RESOURCES: set[
    tuple[type[FacebookResource], IncrementalFetchPageFn, IncrementalFetchChangesFn]
] = {
    (Images, fetch_page_reverse_order, fetch_changes_reverse_order),
    (Videos, fetch_page_reverse_order, fetch_changes_reverse_order),
}

INSIGHTS_RESOURCES: set[type[FacebookInsightsResource]] = {
    AdsInsights,
    AdsInsightsAgeAndGender,
    AdsInsightsCountry,
    AdsInsightsRegion,
    AdsInsightsDma,
    AdsInsightsPlatformAndDevice,
    AdsInsightsActionType,
}


def _create_initial_state(account_ids: str | list[str]) -> ResourceState:
    cutoff = datetime.now(tz=UTC)

    if len(account_ids) == 1:
        return ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        )
    else:
        return ResourceState(
            inc={
                account_id: ResourceState.Incremental(cursor=cutoff)
                for account_id in account_ids
            },
            backfill={
                account_id: ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for account_id in account_ids
            },
        )


def _reconcile_connector_state(
    account_ids: list[str],
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

        for account_id in account_ids:
            inc_state_exists = account_id in state.inc
            backfill_state_exists = account_id in state.backfill

            if not inc_state_exists and not backfill_state_exists:
                task.log.info(
                    f"Initializing new subtask state for account id {account_id}."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                state.backfill[account_id] = deepcopy(
                    initial_state.backfill[account_id]
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


async def validate_custom_insights(
    log: Logger,
    custom_insights: list[InsightsConfig],
) -> None:
    """
    Validate custom insights configuration before running the connector.

    This ensures:
    - Fields are valid
    - Breakdowns are valid
    - Action breakdowns are valid
    - No duplicate names with default insights
    - Proper configuration structure
    """
    if not custom_insights:
        return

    errors: list[str] = []

    default_insight_names = [model.name for model in INSIGHTS_RESOURCES]
    custom_insight_names: set[str] = set()

    for insight in custom_insights:
        if insight.name in default_insight_names:
            errors.append(
                f'Custom insight name "{insight.name}" conflicts with a default insights stream. '
                f"Please rename your custom insight."
            )

        if insight.name in custom_insight_names:
            errors.append(
                f'Duplicate custom insight name "{insight.name}". '
                f"Each custom insight must have a unique name."
            )
        custom_insight_names.add(insight.name)

        if insight.fields:
            invalid_fields = [
                f for f in insight.fields if f not in AD_INSIGHTS_VALID_FIELDS
            ]
            if invalid_fields:
                errors.append(
                    f'Custom insight "{insight.name}" has invalid fields: {", ".join(invalid_fields)}. '
                    f"See https://developers.facebook.com/docs/marketing-api/insights/parameters for valid fields."
                )

        if insight.breakdowns:
            invalid_breakdowns = [
                b for b in insight.breakdowns if b not in AD_INSIGHTS_VALID_BREAKDOWNS
            ]
            if invalid_breakdowns:
                errors.append(
                    f'Custom insight "{insight.name}" has invalid breakdowns: {", ".join(invalid_breakdowns)}. '
                    f"Valid breakdowns are: {', '.join(sorted(AD_INSIGHTS_VALID_BREAKDOWNS))}"
                )

            # Warn about breakdown limitations
            if len(insight.breakdowns) > 1:
                log.warn(
                    f'Custom insight "{insight.name}" has {len(insight.breakdowns)} breakdowns. '
                    f"Note that Facebook may not return breakdown fields if no data exists for those dimensions. "
                    f"Consider using single breakdowns for more reliable results."
                )

        if insight.action_breakdowns:
            invalid_action_breakdowns = [
                ab
                for ab in insight.action_breakdowns
                if ab not in AD_INSIGHTS_VALID_ACTION_BREAKDOWNS
            ]
            if invalid_action_breakdowns:
                errors.append(
                    f'Custom insight "{insight.name}" has invalid action_breakdowns: {", ".join(invalid_action_breakdowns)}. '
                    f"Valid action_breakdowns are: {', '.join(sorted(AD_INSIGHTS_VALID_ACTION_BREAKDOWNS))}"
                )

        if insight.level not in ["ad", "adset", "campaign"]:
            errors.append(
                f'Custom insight "{insight.name}" has invalid level "{insight.level}". '
                f"Valid levels are: ad, adset, campaign"
            )

    if errors:
        raise ValidationError(errors)


async def full_refresh_resource(
    model: type[FacebookResource],
    client: FacebookAPIClient,
    accounts: list[str],
    snapshot_fn: FullRefreshFetchFn,
) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
        snapshot_fn: FetchSnapshotFn,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fn,
            tombstone=FacebookResource(_meta=FacebookResource.Meta(op="d")),
        )

    return Resource(
        name=model.name,
        key=["/_meta/row_id"],
        model=model,
        open=functools.partial(
            open,
            snapshot_fn=functools.partial(
                snapshot_fn,
                client,
                model,
                accounts,
            ),
        ),
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=model.name,
            interval=model.interval,
        ),
        schema_inference=True,
    )


async def incremental_resource(
    model: type[FacebookResource],
    accounts: list[str],
    initial_state: ResourceState,
    create_fetch_page_fn: Callable[[str], Callable],
    create_fetch_changes_fn: Callable[[str], Callable],
) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        assert len(accounts) > 0, "At least one account ID is required"

        if len(accounts) > 1:
            _reconcile_connector_state(accounts, binding, state, initial_state, task)

            fetch_page_fns = {}
            fetch_changes_fns = {}

            for account_id in accounts:
                fetch_page_fns[account_id] = create_fetch_page_fn(account_id)
                fetch_changes_fns[account_id] = create_fetch_changes_fn(account_id)
        else:
            fetch_page_fns = create_fetch_page_fn(accounts[0])
            fetch_changes_fns = create_fetch_changes_fn(accounts[0])

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_page_fns,
            fetch_changes=fetch_changes_fns,
            tombstone=FacebookResource(_meta=FacebookResource.Meta(op="d")),
        )

    return Resource(
        name=model.name,
        key=model.primary_keys,
        model=model,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(
            name=model.name,
            interval=model.interval,
        ),
        schema_inference=True,
    )


async def incremental_resources(
    client: FacebookAPIClient,
    job_manager: FacebookInsightsJobManager,
    start_date: datetime,
    accounts: list[str],
    initial_state: ResourceState,
    insights_lookback_window: int,
    custom_insights: list[InsightsConfig] | None = None,
) -> list[Resource]:
    resources = []

    async def create_resource(
        model: type[FacebookResource],
        page_fn: IncrementalFetchPageFn,
        changes_fn: IncrementalFetchChangesFn,
    ) -> Resource:
        def create_fetch_page(account_id: str) -> Callable:
            return functools.partial(
                page_fn,
                client,
                model,
                account_id,
                start_date,
            )

        def create_fetch_changes(account_id: str) -> Callable:
            return functools.partial(
                changes_fn,
                client,
                model,
                account_id,
            )

        return await incremental_resource(
            model=model,
            accounts=accounts,
            initial_state=initial_state,
            create_fetch_page_fn=create_fetch_page,
            create_fetch_changes_fn=create_fetch_changes,
        )

    async def create_insights_resource(model: type[FacebookResource]) -> Resource:
        assert issubclass(model, AdsInsights), "Model must be a subclass of AdsInsights"

        def create_fetch_page(account_id: str) -> Callable:
            return functools.partial(
                fetch_page_insights,
                job_manager,
                model,
                account_id,
                start_date,
            )

        def create_fetch_changes(account_id: str) -> Callable:
            return functools.partial(
                fetch_changes_insights,
                job_manager,
                model,
                account_id,
                lookback_window=insights_lookback_window,
            )

        return await incremental_resource(
            model=model,
            accounts=accounts,
            initial_state=initial_state,
            create_fetch_page_fn=create_fetch_page,
            create_fetch_changes_fn=create_fetch_changes,
        )

    for model, page_fn, changes_fn in STANDARD_INCREMENTAL_RESOURCES:
        resources.append(await create_resource(model, page_fn, changes_fn))

    for model, page_fn, changes_fn in REVERSE_ORDER_INCREMENTAL_RESOURCES:
        resources.append(await create_resource(model, page_fn, changes_fn))

    for model in INSIGHTS_RESOURCES:
        resources.append(await create_insights_resource(model))

    if custom_insights:
        for custom_insight in custom_insights:
            resources.append(
                await create_insights_resource(
                    build_custom_ads_insights_model(custom_insight)
                )
            )

    return resources


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )
    client = FacebookAPIClient(http, log, config.include_deleted)
    job_manager = FacebookInsightsJobManager(
        client=client,
        max_concurrent_jobs=10,
        poll_interval=30,
        max_retries=3,
    )

    await validate_credentials(log, http)
    await validate_access_to_accounts(log, http, config.accounts)

    if config.custom_insights:
        await validate_custom_insights(log, config.custom_insights)

    initial_state = _create_initial_state(config.accounts)

    return [
        *[
            await full_refresh_resource(model, client, config.accounts, snapshot_fn)
            for model, snapshot_fn in FULL_REFRESH_RESOURCES
        ],
        *await incremental_resources(
            client=client,
            job_manager=job_manager,
            start_date=config.start_date,
            accounts=config.accounts,
            initial_state=initial_state,
            insights_lookback_window=config.insights_lookback_window or 28,
            custom_insights=config.custom_insights,
        ),
    ]
