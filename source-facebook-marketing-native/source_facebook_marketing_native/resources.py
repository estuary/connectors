import functools
from datetime import datetime, UTC, timedelta
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
from estuary_cdk.http import HTTPMixin, HTTPSession, TokenSource

from estuary_cdk.flow import ValidationError

from .constants import BASE_URL
from .utils import validate_credentials, validate_access_to_accounts, str_to_list
from .client import FacebookAPIClient
from .insights import FacebookInsightsJobManager
from .api import (
    snapshot_resource,
    snapshot_ad_creatives,
    fetch_page,
    fetch_changes,
    fetch_page_reverse_order,
    fetch_changes_reverse_order,
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
    FacebookInsightsResource,
    AdsInsights,
    AdsInsightsAgeAndGender,
    AdsInsightsCountry,
    AdsInsightsRegion,
    AdsInsightsDma,
    AdsInsightsPlatformAndDevice,
    AdsInsightsActionType,
    InsightsConfig,
    FeatureFlag,
    build_custom_ads_insights_model,
    DEFAULT_LOOKBACK_WINDOW,
)
from .fields import (
    AD_INSIGHTS_VALID_FIELDS,
    AD_INSIGHTS_VALID_BREAKDOWNS,
    AD_INSIGHTS_VALID_ACTION_BREAKDOWNS,
)


FULL_REFRESH_RESOURCES: set[type[FacebookResource]] = {
    AdAccount,
    AdCreative,
    CustomConversions,
}

STANDARD_INCREMENTAL_RESOURCES: set[
    tuple[type[FacebookResource], IncrementalFetchPageFn, IncrementalFetchChangesFn]
] = {
    (Campaigns, fetch_page, fetch_changes),
    (AdSets, fetch_page, fetch_changes),
    (Ads, fetch_page, fetch_changes),
    (Activities, fetch_page, fetch_changes),
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

TEST_CONFIG_CLIENT_ID = "placeholder_client_id"
TEST_CONFIG_CLIENT_SECRET = "placeholder_client_secret"


def create_insights_job_manager(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    account_id: str,
) -> FacebookInsightsJobManager:
    """
    Create a job manager instance for a specific account.

    Each account gets its own job manager with independent concurrency
    control (semaphore). This aligns with Facebook's per-account rate
    limiting and provides isolation between accounts.
    """
    return FacebookInsightsJobManager(
        http=http,
        base_url=base_url,
        log=log.getChild(f"insights.{account_id}"),
        account_id=account_id,
    )


def _create_initial_state(account_ids: str | list[str]) -> ResourceState:
    assert len(account_ids) > 0, "At least one account ID is required"
    cutoff = datetime.now(tz=UTC)

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


def validate_custom_insights(
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
        fields = str_to_list(insight.fields) if insight.fields else []
        breakdowns = str_to_list(insight.breakdowns) if insight.breakdowns else []
        action_breakdowns = (
            str_to_list(insight.action_breakdowns) if insight.action_breakdowns else []
        )

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

        if fields:
            invalid_fields = [f for f in fields if f not in AD_INSIGHTS_VALID_FIELDS]
            if invalid_fields:
                errors.append(
                    f'Custom insight "{insight.name}" has invalid fields: {", ".join(invalid_fields)}. '
                    f"See https://developers.facebook.com/docs/marketing-api/insights/parameters for valid fields."
                )

        if breakdowns:
            invalid_breakdowns = [
                b for b in breakdowns if b not in AD_INSIGHTS_VALID_BREAKDOWNS
            ]
            if invalid_breakdowns:
                errors.append(
                    f'Custom insight "{insight.name}" has invalid breakdowns: {", ".join(invalid_breakdowns)}. '
                    f"Valid breakdowns are: {', '.join(sorted(AD_INSIGHTS_VALID_BREAKDOWNS))}"
                )

            # Warn about breakdown limitations
            if len(breakdowns) > 1:
                log.warning(
                    f'Custom insight "{insight.name}" has {len(breakdowns)} breakdowns. '
                    f"Note that Facebook may not return breakdown fields if no data exists for those dimensions. "
                    f"Consider using single breakdowns for more reliable results."
                )

        if action_breakdowns:
            invalid_action_breakdowns = [
                ab
                for ab in action_breakdowns
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


def full_refresh_resource(
    model: type[FacebookResource],
    client: FacebookAPIClient,
    accounts: list[str],
    snapshot_fn: FullRefreshFetchFn,
    use_sourced_schemas: bool = False,
) -> Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
        snapshot_fn: FetchSnapshotFn,
        use_sourced_schemas: bool,
    ):
        if use_sourced_schemas and hasattr(model, 'sourced_schema'):
            task.sourced_schema(binding_index, model.sourced_schema())
            task.checkpoint(state=ConnectorState())

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
            use_sourced_schemas=use_sourced_schemas,
        ),
        initial_state=ResourceState(),
        initial_config=ResourceConfig(
            name=model.name,
            interval=timedelta(hours=1),
        ),
        schema_inference=True,
    )


def full_refresh_resources(
    client: FacebookAPIClient,
    accounts: list[str],
    include_deleted: bool,
    use_sourced_schemas: bool = False,
) -> list[Resource]:
    resources = []

    for model in FULL_REFRESH_RESOURCES:
        if model is AdCreative:
            snapshot_fn = functools.partial(
                snapshot_ad_creatives,
                include_deleted=include_deleted,
                fetch_thumbnail_images=False,
            )
        else:
            snapshot_fn = functools.partial(
                snapshot_resource,
                include_deleted=include_deleted,
            )

        resources.append(
            full_refresh_resource(
                model,
                client,
                accounts,
                snapshot_fn,
                use_sourced_schemas=use_sourced_schemas,
            )
        )

    return resources


def incremental_resource(
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

        _reconcile_connector_state(accounts, binding, state, initial_state, task)

        fetch_page_fns = {}
        fetch_changes_fns = {}

        for account_id in accounts:
            fetch_page_fns[account_id] = create_fetch_page_fn(account_id)
            fetch_changes_fns[account_id] = create_fetch_changes_fn(account_id)

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
            interval=timedelta(hours=1),
        ),
        schema_inference=True,
    )


def incremental_resources(
    client: FacebookAPIClient,
    http: HTTPSession,
    base_url: str,
    log: Logger,
    start_date: datetime,
    accounts: list[str],
    initial_state: ResourceState,
    insights_lookback_window: int,
    include_deleted: bool,
    custom_insights: list[InsightsConfig] | None = None,
) -> list[Resource]:
    resources = []

    # Create one job manager per account for isolated concurrency control
    job_managers: dict[str, FacebookInsightsJobManager] = {
        account_id: create_insights_job_manager(http, base_url, log, account_id)
        for account_id in accounts
    }

    def create_resource(
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
                include_deleted,
            )

        def create_fetch_changes(account_id: str) -> Callable:
            return functools.partial(
                changes_fn,
                client,
                model,
                account_id,
                include_deleted,
            )

        return incremental_resource(
            model=model,
            accounts=accounts,
            initial_state=initial_state,
            create_fetch_page_fn=create_fetch_page,
            create_fetch_changes_fn=create_fetch_changes,
        )

    def create_insights_resource(model: type[FacebookResource]) -> Resource:
        assert issubclass(model, FacebookInsightsResource), "Model must be a subclass of FacebookInsightsResource"

        def create_fetch_page(account_id: str) -> Callable:
            return functools.partial(
                fetch_page_insights,
                job_managers[account_id],
                model,
                account_id,
                start_date,
            )

        def create_fetch_changes(account_id: str) -> Callable:
            return functools.partial(
                fetch_changes_insights,
                job_managers[account_id],
                model,
                account_id,
                lookback_window=insights_lookback_window,
            )

        return incremental_resource(
            model=model,
            accounts=accounts,
            initial_state=initial_state,
            create_fetch_page_fn=create_fetch_page,
            create_fetch_changes_fn=create_fetch_changes,
        )

    for model, page_fn, changes_fn in STANDARD_INCREMENTAL_RESOURCES:
        resources.append(create_resource(model, page_fn, changes_fn))

    for model, page_fn, changes_fn in REVERSE_ORDER_INCREMENTAL_RESOURCES:
        resources.append(create_resource(model, page_fn, changes_fn))

    for model in INSIGHTS_RESOURCES:
        resources.append(create_insights_resource(model))

    if custom_insights:
        for custom_insight in custom_insights:
            resources.append(
                create_insights_resource(
                    build_custom_ads_insights_model(custom_insight)
                )
            )

    return resources


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_validate_access: bool = False,
) -> list[Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )
    client = FacebookAPIClient(
        http,
        log,
    )

    if (
        should_validate_access
        and config.credentials.client_id != TEST_CONFIG_CLIENT_ID
        and config.credentials.client_secret != TEST_CONFIG_CLIENT_SECRET
    ):
        await validate_credentials(log, http)
        await validate_access_to_accounts(log, http, config.accounts)

    if config.custom_insights:
        validate_custom_insights(log, config.custom_insights)

    initial_state = _create_initial_state(config.accounts)

    return [
        *full_refresh_resources(
            client=client,
            accounts=config.accounts,
            include_deleted=config.advanced.include_deleted,
            use_sourced_schemas=config.advanced.has_feature_flag(FeatureFlag.SOURCED_SCHEMAS),
        ),
        *incremental_resources(
            client=client,
            http=http,
            base_url=BASE_URL,
            log=log,
            start_date=config.start_date,
            accounts=config.accounts,
            initial_state=initial_state,
            insights_lookback_window=config.insights_lookback_window
            or DEFAULT_LOOKBACK_WINDOW,
            custom_insights=config.custom_insights,
            include_deleted=config.advanced.include_deleted,
        ),
    ]
