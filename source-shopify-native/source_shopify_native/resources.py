import functools
from copy import deepcopy
from datetime import datetime, timedelta, UTC
from logging import Logger
from dataclasses import dataclass
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
    AccessScopes,
    ConnectorState,
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


@dataclass
class StoreContext:
    """Context for a single Shopify store containing HTTP client, GraphQL client, and available resources."""

    http: StoreHTTP
    client: gql.ShopifyGraphQLClient
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager
    scopes: set[str]
    available_resources: set[type[ShopifyGraphQLResource]]


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

PII_RESOURCES: set[type[ShopifyGraphQLResource]] = {
    gql.Customers,
    gql.Orders,
    gql.FulfillmentOrders,
}


async def _create_store_context(
    log: Logger,
    http: HTTPMixin,
    store_config: StoreConfig,
    should_cancel_ongoing_job: bool,
) -> tuple[str, StoreContext]:
    """Create context for a single store. Returns (store_id, store_context)."""
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

    # OAuth apps can always access PII; AccessToken apps depend on plan
    can_access_pii = True
    uses_access_token = isinstance(store_config.credentials, AccessToken)
    if uses_access_token:
        can_access_pii = await _check_plan_allows_pii(store_http, client.url, log)

    available_resources = _get_available_resources(granted_scopes, uses_access_token, can_access_pii)

    store_context = StoreContext(
        http=store_http,
        client=client,
        bulk_job_manager=bulk_job_manager,
        scopes=granted_scopes,
        available_resources=available_resources,
    )

    return store_config.store, store_context


def _get_available_resources(
    granted_scopes: set[str],
    uses_access_token: bool,
    can_access_pii: bool,
) -> set[type[ShopifyGraphQLResource]]:
    """Determine which resources are available based on scopes and plan."""
    available: set[type[ShopifyGraphQLResource]] = set()

    for resource in INCREMENTAL_RESOURCES:
        # Skip if none of the qualifying scopes are granted
        if resource.QUALIFYING_SCOPES.isdisjoint(granted_scopes):
            continue
        # Skip PII resources for AccessToken auth when plan doesn't allow PII
        if uses_access_token and resource in PII_RESOURCES and not can_access_pii:
            continue
        available.add(resource)

    return available


def _create_initial_state(
    store_ids: list[str],
    start_date: AwareDatetime,
    use_backfill: bool,
) -> ResourceState:
    """Create initial state for a resource.

    State format is always dictionary-based: {"inc": {"store_id": {"cursor": "..."}}}

    This ensures consistency across all captures (new and legacy) and allows
    seamless addition of stores without requiring backfill when only state format changes.

    Args:
        store_ids: List of store IDs to create state for.
        start_date: The start date for data replication.
        use_backfill: Whether to create backfill state (for non-bulk queries with SORT_KEY).
    """
    cutoff = datetime.now(tz=UTC)

    if use_backfill:
        return ResourceState(
            inc={sid: ResourceState.Incremental(cursor=cutoff) for sid in store_ids},
            backfill={sid: ResourceState.Backfill(next_page=dt_to_str(start_date), cutoff=cutoff) for sid in store_ids},
        )
    return ResourceState(
        inc={sid: ResourceState.Incremental(cursor=start_date) for sid in store_ids},
    )


def _migrate_flat_to_dict_state(
    state: ResourceState,
    store_id: str,
    log: Logger,
) -> bool:
    """Migrate legacy flat state format to dict-based format.

    Converts:
    - Flat: {"inc": {"cursor": "..."}, "backfill": {...}}
    - Dict: {"inc": {"store_id": {"cursor": "..."}}, "backfill": {"store_id": {...}}}

    Args:
        state: The resource state to migrate (modified in place).
        store_id: The store ID to use as the key for migrated state.
        log: Logger for info messages.

    Returns:
        True if migration was performed, False if state was already dict-based.
    """
    if not isinstance(state.inc, ResourceState.Incremental):
        return False  # Already dict-based or None

    log.info(f"Migrating from flat to dict state using store: {store_id}")

    # Convert incremental state to dict format
    state.inc = {store_id: state.inc}

    # Convert backfill state if present
    if isinstance(state.backfill, ResourceState.Backfill):
        state.backfill = {store_id: state.backfill}

    return True


def _add_missing_store_entries(
    state: ResourceState,
    store_ids: list[str],
    initial_state: ResourceState,
    log: Logger,
) -> bool:
    """Add state entries for newly configured stores.

    Args:
        state: The resource state to update (modified in place).
        store_ids: List of all store IDs that should have state entries.
        initial_state: Initial state template to copy for new stores.
        log: Logger for info messages.

    Returns:
        True if any entries were added, False otherwise.

    Raises:
        RuntimeError: If state is not in expected dict-based format.
    """
    if not isinstance(state.inc, dict):
        raise RuntimeError(
            f"Cannot add store entries: state.inc is not dict-based (got {type(state.inc).__name__}). "
            "This indicates a bug in state migration."
        )

    added_any = False

    for store_id in store_ids:
        inc_exists = store_id in state.inc
        backfill_exists = isinstance(state.backfill, dict) and store_id in state.backfill

        if not inc_exists and not backfill_exists:
            log.info(f"Initializing new state for store: {store_id}")
            state.inc[store_id] = deepcopy(initial_state.inc[store_id])
            if isinstance(state.backfill, dict) and isinstance(initial_state.backfill, dict):
                state.backfill[store_id] = deepcopy(initial_state.backfill[store_id])
            added_any = True
        elif not inc_exists and backfill_exists:
            # Edge case: backfill exists but incremental doesn't
            log.info(f"Reinitializing state for store {store_id} due to missing incremental state")
            state.inc[store_id] = deepcopy(initial_state.inc[store_id])
            if isinstance(state.backfill, dict) and isinstance(initial_state.backfill, dict):
                state.backfill[store_id] = deepcopy(initial_state.backfill[store_id])
            added_any = True

    return added_any


async def _reconcile_connector_state(
    store_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
) -> None:
    """Reconcile connector state: migrate format if needed and add missing stores.

    This handles two scenarios:
    1. Migration from flat to dict-based state (legacy single-store captures)
    2. Adding missing store entries for newly configured stores

    Args:
        store_ids: List of all store IDs that should have state entries.
        binding: The capture binding being reconciled.
        state: The current resource state (modified in place).
        initial_state: Initial state template for new stores.
        task: The capture task for logging and checkpointing.

    Raises:
        RuntimeError: If initial_state or state is in an unexpected format.
    """
    if not isinstance(initial_state.inc, dict):
        raise RuntimeError(
            f"Invalid initial_state for {binding.stateKey}: expected dict-based inc, "
            f"got {type(initial_state.inc).__name__}. This indicates a bug in state creation."
        )

    # Step 1: Migrate flat state to dict-based if needed
    migrated = _migrate_flat_to_dict_state(state, store_ids[0], task.log)

    # Step 2: Add missing store entries
    added = _add_missing_store_entries(state, store_ids, initial_state, task.log)

    # Step 3: Checkpoint if any changes were made
    if migrated or added:
        task.log.info(f"Checkpointing reconciled state for {binding.stateKey}")
        await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


async def _check_plan_allows_pii(http: HTTPMixin, url: str, log: Logger) -> bool:
    """Check if the Shopify plan allows access to PII data."""
    response = ShopDetails.model_validate_json(
        await http.request(log, url, method="POST", json={"query": ShopDetails.query()})
    )
    plan = response.data.shop.plan

    if plan.partnerDevelopment or plan.shopifyPlus:
        return True

    if plan.displayName in (PlanName.BASIC, PlanName.STARTER):
        return False

    if plan.displayName in PlanName:
        return True

    log.warning(
        f"Shopify plan '{plan.displayName}' is not recognized. "
        f"Assuming PII access is supported."
    )
    return True


async def _get_granted_scopes(http: HTTPMixin, url: str, log: Logger) -> set[str]:
    """Query the currentAppInstallation to determine which scopes are granted."""
    response = AccessScopes.model_validate_json(
        await http.request(log, url, method="POST", json={"query": AccessScopes.query()})
    )
    scopes = response.get_scope_handles()
    log.info(f"Access token has scopes: {scopes}")
    return scopes


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
            log.debug(f"Validating credentials for store: {store_config.store}", extra={"credentials": store_config.credentials.access_token, "store_config": store_config})
            await bulk_job_manager._get_running_jobs()
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
    use_store_in_key: bool = True,
) -> list[Resource]:
    """Discover all available resources across all configured stores.

    State format is always dictionary-based: {"inc": {"store_id": {...}}}

    Collection key format depends on use_store_in_key:
    - True: ["/_meta/store", "/id"]
    - False: ["/id"]
    """
    store_contexts: dict[str, StoreContext] = {}
    for store_config in config.stores:
        log.info(f"Initializing context for store: {store_config.store}")
        store_id, store_context = await _create_store_context(
            log, http, store_config, should_cancel_ongoing_job
        )

        excluded = [m.NAME for m in INCREMENTAL_RESOURCES if m not in store_context.available_resources]
        if excluded:
            log.info(f"Store '{store_id}' excluding {len(excluded)} stream(s) due to missing scopes or plan restrictions")

        store_contexts[store_id] = store_context

    # Determine which resources are available across all stores (union)
    all_available: set[type[ShopifyGraphQLResource]] = set()
    for ctx in store_contexts.values():
        all_available.update(ctx.available_resources)

    log.info(f"Discovered {len(all_available)} stream(s) across {len(store_contexts)} store(s)")

    # Build resources
    resources: list[Resource] = []
    key = ["/_meta/store", "/id"] if use_store_in_key else ["/id"]

    for model in INCREMENTAL_RESOURCES:
        stores_with_access = [
            store_id for store_id, ctx in store_contexts.items()
            if model in ctx.available_resources
        ]

        if not stores_with_access:
            continue

        use_backfill = not model.SHOULD_USE_BULK_QUERIES and model.SORT_KEY is not None
        initial_state = _create_initial_state(stores_with_access, config.start_date, use_backfill)

        def create_open_fn(
            model: type[ShopifyGraphQLResource],
            stores_with_access: list[str],
            initial_state: ResourceState,
        ):
            async def open(
                binding: CaptureBinding[ResourceConfig],
                binding_index: int,
                state: ResourceState,
                task: Task,
                _all_bindings=None,
            ):
                await _reconcile_connector_state(
                    stores_with_access, binding, state, initial_state, task
                )

                # Warn if FulfillmentOrders has partial scope coverage
                if model == gql.FulfillmentOrders:
                    fo_scopes = gql.FulfillmentOrders.QUALIFYING_SCOPES
                    for store_id in stores_with_access:
                        granted = store_contexts[store_id].scopes
                        if fo_scopes & granted and not fo_scopes <= granted:
                            missing = fo_scopes - granted
                            task.log.warning(
                                f"Store '{store_id}': FulfillmentOrders has partial scopes. "
                                f"Missing: {missing}. Only matching fulfillment orders will be captured."
                            )

                if not model.SHOULD_USE_BULK_QUERIES and "edges" in model.QUERY.lower():
                    raise RuntimeError("Non-bulk queries cannot contain nested connections.")

                data_model = create_response_data_model(model)

                # Always use dict-based fetch functions
                fetch_changes: dict[str, functools.partial] = {}
                fetch_page: dict[str, functools.partial] = {}

                for store_id in stores_with_access:
                    ctx = store_contexts[store_id]

                    if model.SHOULD_USE_BULK_QUERIES:
                        fetch_changes[store_id] = functools.partial(
                            bulk_fetch_incremental,
                            ctx.http, config.advanced.window_size, ctx.bulk_job_manager, model, store_id,
                        )
                    elif model.SORT_KEY is None:
                        fetch_changes[store_id] = functools.partial(
                            fetch_incremental_unsorted,
                            ctx.client, model, data_model, store_id,
                        )
                    else:
                        fetch_changes[store_id] = functools.partial(
                            fetch_incremental,
                            ctx.client, model, data_model, store_id,
                        )
                        fetch_page[store_id] = functools.partial(
                            backfill_incremental,
                            ctx.client, model, data_model, store_id,
                        )

                open_binding(
                    binding,
                    binding_index,
                    state,
                    task,
                    fetch_changes=fetch_changes,
                    fetch_page=fetch_page if fetch_page else None,
                )

            return open

        resources.append(
            Resource(
                name=model.NAME,
                key=key,
                model=ShopifyGraphQLResource,
                open=create_open_fn(model, stores_with_access, initial_state),
                initial_state=initial_state,
                initial_config=ResourceConfig(name=model.NAME, interval=timedelta(minutes=5)),
                schema_inference=True,
            )
        )

    return resources
