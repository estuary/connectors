import functools
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from logging import Logger
from pydantic import AwareDatetime

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    AccessToken,
    CaptureBinding,
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

import source_shopify_native.graphql as gql

from .models import (
    OAUTH2_SPEC,
    ConnectorState,
    AccessScopes,
    EndpointConfig,
    ShopifyGraphQLResource,
    ShopDetails,
    PlanName,
    StoreConfig,
    create_response_data_model,
)
from .api import (
    bulk_fetch_incremental,
    fetch_incremental_unsorted,
    fetch_incremental,
    backfill_incremental,
)


from .graphql.common import dt_to_str


class StoreHTTP(HTTPMixin):
    """Per-store HTTP session sharing the underlying client but with its own token_source."""

    def __init__(self, http: HTTPMixin, token_source: TokenSource):
        self.inner = http.inner
        self.rate_limiter = http.rate_limiter
        self.token_source = token_source


AUTHORIZATION_HEADER = "X-Shopify-Access-Token"

INCREMENTAL_RESOURCES: list[type[ShopifyGraphQLResource]] = [
    gql.AbandonedCheckouts,
    gql.Customers,
    gql.CustomerMetafields,
    gql.Products,
    gql.ProductMedia,
    gql.ProductMetafields,
    gql.ProductVariants,
    gql.ProductVariantMetafields,
    gql.FulfillmentOrders,
    gql.Fulfillments,
    gql.Orders,
    gql.OrderAgreements,
    gql.OrderMetafields,
    gql.OrderTransactions,
    gql.OrderRefunds,
    gql.OrderRisks,
    gql.InventoryItems,
    gql.InventoryLevels,
    gql.CustomCollections,
    gql.SmartCollections,
    gql.CustomCollectionMetafields,
    gql.SmartCollectionMetafields,
    gql.Locations,
    gql.LocationMetafields,
    gql.SubscriptionContracts,
]


PII_RESOURCES: list[type[ShopifyGraphQLResource]] = [
    gql.Customers,
    gql.Orders,
    gql.FulfillmentOrders,
]


@dataclass
class StoreContext:
    """Context for a single Shopify store including credentials and API clients."""
    store: str
    store_config: StoreConfig
    http: HTTPMixin
    client: gql.ShopifyGraphQLClient
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager
    granted_scopes: set[str]
    can_access_pii: bool
    available_resources: set[type[ShopifyGraphQLResource]] = field(default_factory=set)


async def _create_store_context(
    log: Logger,
    http: HTTPMixin,
    store_config: StoreConfig,
    should_cancel_ongoing_job: bool,
) -> StoreContext:
    token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=store_config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    store_http = StoreHTTP(http, token_source)

    client = gql.ShopifyGraphQLClient(store_http, store_config.store)
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(client, log)

    if should_cancel_ongoing_job:
        await bulk_job_manager.cancel_current()

    granted_scopes = await _get_granted_scopes(store_http, client.url, log)
    can_access_pii = await _can_access_pii_for_store(store_http, client.url, log, store_config)

    return StoreContext(
        store=store_config.store,
        store_config=store_config,
        http=store_http,
        client=client,
        bulk_job_manager=bulk_job_manager,
        granted_scopes=granted_scopes,
        can_access_pii=can_access_pii,
    )


async def _create_all_store_contexts(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_cancel_ongoing_job: bool,
) -> dict[str, StoreContext]:
    contexts: dict[str, StoreContext] = {}
    for store_config in config.stores:
        log.info(f"Initializing context for store: {store_config.store}")
        ctx = await _create_store_context(log, http, store_config, should_cancel_ongoing_job)
        contexts[store_config.store] = ctx
    return contexts


def _get_store_available_resources(ctx: StoreContext) -> set[type[ShopifyGraphQLResource]]:
    """Determine which resources this store can access based on scopes and plan."""
    available: set[type[ShopifyGraphQLResource]] = set()
    for resource in INCREMENTAL_RESOURCES:
        if resource.REQUIRED_SCOPE not in ctx.granted_scopes:
            continue
        # If using AccessToken and can't access PII, skip PII resources
        if isinstance(ctx.store_config.credentials, AccessToken):
            if resource in PII_RESOURCES and not ctx.can_access_pii:
                continue
        available.add(resource)
    return available


def _create_multi_store_initial_state(
    store_ids: list[str],
    start_date: AwareDatetime,
    use_backfill: bool = False,
) -> ResourceState:
    cutoff = datetime.now(tz=UTC)

    if use_backfill:
        incremental_state = {}
        backfill_state = {}

        for store_id in store_ids:
            incremental_state[store_id] = ResourceState.Incremental(cursor=cutoff)
            backfill_state[store_id] = ResourceState.Backfill(
                next_page=dt_to_str(start_date),
                cutoff=cutoff,
            )

        return ResourceState(
            inc=incremental_state,
            backfill=backfill_state,
        )
    else:
        return ResourceState(
            inc={store_id: ResourceState.Incremental(cursor=start_date) for store_id in store_ids},
        )


def _reconcile_connector_state(
    store_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
) -> None:
    should_checkpoint = False

    if (
        isinstance(state.inc, dict)
        and isinstance(state.backfill, dict)
        and isinstance(initial_state.inc, dict)
        and isinstance(initial_state.backfill, dict)
    ):
        for store_id in store_ids:
            inc_state_exists = store_id in state.inc
            backfill_state_exists = store_id in state.backfill

            if not inc_state_exists and not backfill_state_exists:
                task.log.info(f"Initializing new subtask state for store: {store_id}")
                state.inc[store_id] = deepcopy(initial_state.inc[store_id])
                state.backfill[store_id] = deepcopy(initial_state.backfill[store_id])
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


async def _can_access_pii_for_store(
    http: HTTPMixin,
    url: str,
    log: Logger,
    store_config: StoreConfig,
) -> bool:
    """Check if a store can access PII based on plan and credential type."""
    # OAuth apps can always access PII if scopes are granted
    if not isinstance(store_config.credentials, AccessToken):
        return True
    return await _can_access_pii(http, url, log)


async def _can_access_pii(
    http: HTTPMixin,
    url: str,
    log: Logger,
) -> bool:
    response = ShopDetails.model_validate_json(
        await http.request(
            log, url, method="POST", json={"query": ShopDetails.query()}
        )
    )

    plan = response.data.shop.plan

    if plan.partnerDevelopment or plan.shopifyPlus:
        return True

    match plan.displayName:
        case PlanName.BASIC | PlanName.STARTER:
            return False
        case _:
            if plan.displayName in PlanName:
                return True
            else:
                log.warning(
                    f"Shopify plan '{plan.displayName}' is not recognized. "
                    f"Assuming access to PII is supported on the {plan.displayName} plan."
                )
                return True


async def _get_granted_scopes(
    http: HTTPMixin,
    url: str,
    log: Logger,
) -> set[str]:
    """Query the currentAppInstallation to determine which scopes are granted."""
    response = AccessScopes.model_validate_json(
        await http.request(
            log, url, method="POST", json={"query": AccessScopes.query()}
        )
    )
    scopes = response.get_scope_handles()
    log.info(f"Access token has scopes: {scopes}")
    return scopes


def _incremental_resources_multi_store(
    config: EndpointConfig,
    store_contexts: dict[str, StoreContext],
) -> list[Resource]:
    """Create resources with dict-based fetch functions for multi-store support.

    Each resource stream will have fetch functions for each store that has access
    to that resource. The CDK handles per-store state management automatically
    when dict-based fetch functions are provided.
    """

    def create_open_fn(
        model: type[ShopifyGraphQLResource],
        stores_with_access: list[str],
        initial_state: ResourceState,
    ):
        def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            _all_bindings=None,
        ):
            # Reconcile state for added/removed stores
            _reconcile_connector_state(stores_with_access, state, initial_state, task)

            data_model = create_response_data_model(model)

            if model.SHOULD_USE_BULK_QUERIES:
                # Create fetch_changes dict keyed by store_id
                fetch_changes_fns: dict[str, functools.partial] = {}
                for store_id in stores_with_access:
                    ctx = store_contexts[store_id]
                    fetch_changes_fns[store_id] = functools.partial(
                        bulk_fetch_incremental,
                        ctx.http,
                        config.advanced.window_size,
                        ctx.bulk_job_manager,
                        model,
                        store_id,
                    )

                open_binding(
                    binding,
                    binding_index,
                    state,
                    task,
                    fetch_changes=fetch_changes_fns,  # type: ignore[arg-type]
                )
            else:
                # Non-bulk queries cannot be used for queries that have nested
                # connections. Any query with "edges { node { blah } }" in it
                # contains a nested connection.
                if "edges" in model.QUERY.lower():
                    raise RuntimeError("Implementation error: Non-bulk queries cannot contain nested connections.")

                if model.SORT_KEY is None:
                    fetch_changes_fns = {}
                    for store_id in stores_with_access:
                        ctx = store_contexts[store_id]
                        fetch_changes_fns[store_id] = functools.partial(
                            fetch_incremental_unsorted,
                            ctx.client,
                            model,
                            data_model,
                            store_id,
                        )

                    open_binding(
                        binding,
                        binding_index,
                        state,
                        task,
                        fetch_changes=fetch_changes_fns,  # type: ignore[arg-type]
                    )
                else:
                    fetch_changes_fns = {}
                    fetch_page_fns: dict[str, functools.partial] = {}
                    for store_id in stores_with_access:
                        ctx = store_contexts[store_id]
                        fetch_changes_fns[store_id] = functools.partial(
                            fetch_incremental,
                            ctx.client,
                            model,
                            data_model,
                            store_id,
                        )
                        fetch_page_fns[store_id] = functools.partial(
                            backfill_incremental,
                            ctx.client,
                            model,
                            data_model,
                            store_id,
                        )

                    open_binding(
                        binding,
                        binding_index,
                        state,
                        task,
                        fetch_changes=fetch_changes_fns,  # type: ignore[arg-type]
                        fetch_page=fetch_page_fns,  # type: ignore[arg-type]
                    )

        return open

    resources: list[Resource] = []

    for model in INCREMENTAL_RESOURCES:
        # Find stores that can access this resource
        stores_with_access = [
            store_id for store_id, ctx in store_contexts.items()
            if model in ctx.available_resources
        ]

        # Skip if NO store has access (don't discover this stream)
        if not stores_with_access:
            continue

        # Determine if this model needs backfill mode
        use_backfill = not model.SHOULD_USE_BULK_QUERIES and model.SORT_KEY is not None

        # Create initial state with per-store cursors
        initial_state = _create_multi_store_initial_state(
            stores_with_access,
            config.start_date,
            use_backfill=use_backfill,
        )

        resources.append(
            Resource(
                name=model.NAME,
                key=["/_meta/store", "/id"],
                model=ShopifyGraphQLResource,
                open=create_open_fn(model, stores_with_access, initial_state),
                initial_state=initial_state,
                initial_config=ResourceConfig(name=model.NAME, interval=timedelta(minutes=5)),
                schema_inference=True,
            )
        )

    return resources


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Validate credentials for all configured stores."""
    errors: list[str] = []

    for store_config in config.stores:
        http.token_source = TokenSource(
            oauth_spec=OAUTH2_SPEC,
            credentials=store_config.credentials,
            authorization_header=AUTHORIZATION_HEADER,
        )
        client = gql.ShopifyGraphQLClient(http, store_config.store)
        bulk_job_manager = gql.bulk_job_manager.BulkJobManager(client, log)

        try:
            await bulk_job_manager._get_currently_running_job()
        except HTTPError as err:
            if err.code == 401:
                errors.append(
                    f"Store '{store_config.store}': Invalid credentials. "
                    f"Please confirm the provided credentials are correct.\n\n{err.message}"
                )
            else:
                errors.append(
                    f"Store '{store_config.store}': Encountered error validating access token.\n\n{err.message}"
                )

    if errors:
        raise ValidationError(errors)


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_cancel_ongoing_job: bool = False,
) -> list[Resource]:
    """Discover all available resources across all configured stores.

    Uses union of scopes - a stream is discovered if ANY store has access.
    Per-store filtering happens during fetch via dict-based fetch functions.
    """
    # Create contexts for all stores
    store_contexts = await _create_all_store_contexts(
        log, http, config, should_cancel_ongoing_job
    )

    # Compute which resources each store can access
    for store_id, ctx in store_contexts.items():
        ctx.available_resources = _get_store_available_resources(ctx)
        excluded = [
            m.NAME for m in INCREMENTAL_RESOURCES
            if m not in ctx.available_resources
        ]
        if excluded:
            log.info(
                f"Store '{store_id}' excluding {len(excluded)} stream(s) "
                f"due to missing scopes or plan restrictions: {excluded}"
            )

    # Log union of available resources
    all_available = set()
    for ctx in store_contexts.values():
        all_available.update(ctx.available_resources)

    log.info(
        f"Discovered {len(all_available)} stream(s) across {len(store_contexts)} store(s)"
    )

    # Build resources using multi-store pattern
    return _incremental_resources_multi_store(config, store_contexts)
