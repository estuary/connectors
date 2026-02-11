import asyncio
import functools
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import (
    AccessToken,
    CaptureBinding,
    OAuth2TokenFlowSpec,
    ValidationError,
)
from estuary_cdk.http import HTTPError, HTTPMixin, RateLimiter, TokenSource
from pydantic import AwareDatetime

import source_shopify_native.graphql as gql
from .graphql.bulk_job_manager import BulkJobManager

from .api import (
    backfill_incremental,
    bulk_fetch_incremental,
    fetch_incremental,
    fetch_incremental_unsorted,
)
from .utils import dt_to_str
from .models import (
    AccessScopes,
    ConnectorState,
    EndpointConfig,
    PlanName,
    ShopDetails,
    ShopifyClientCredentials,
    ShopifyGraphQLResource,
    StoreConfig,
    create_response_data_model,
)


class StoreInitError(Exception):
    """Raised when one or more stores fail to initialize.

    Current behavior is fail-fast: any single store failure blocks the entire capture.
    This surfaces configuration errors clearly rather than silently skipping a store.
    TODO: Consider graceful degradation — continue with healthy stores and surface
    errors for failed ones without blocking the entire capture.
    """

    def __init__(
        self, failed_stores: dict[str, BaseException], initialized_stores: list[str]
    ):
        self.failed_stores = failed_stores
        self.initialized_stores = initialized_stores
        store_errors = "\n".join(
            f"  - {store}: {error}" for store, error in failed_stores.items()
        )
        super().__init__(
            f"Failed to initialize {len(failed_stores)} store(s):\n{store_errors}"
        )


class StoreHTTP(HTTPMixin):
    """Per-store HTTP wrapper that shares a parent session with per-store auth and rate limiting.

    Sets the three attributes that HTTPMixin's methods depend on:
    - inner: shared aiohttp.ClientSession from the parent (for connection pooling)
    - rate_limiter: independent per-store rate limiter
    - token_source: per-store authentication credentials

    All HTTP methods (request, request_lines, etc.) are inherited via the class hierarchy.
    """

    def __init__(self, http: HTTPMixin, token_source: TokenSource):
        # super().__init__() is intentionally not called. HTTPMixin is a pure
        # attribute-based mixin whose initialization happens via _mixin_enter()
        # context manager hooks (estuary_cdk/http.py:481), not constructors.
        # We directly set the three attributes that HTTPMixin's methods depend on.
        self.inner = http.inner
        self.rate_limiter = RateLimiter()
        self.token_source = token_source


@dataclass(frozen=True, slots=True)
class StoreContext:
    """Context for a single Shopify store containing HTTP client, GraphQL client, and available resources."""

    http: StoreHTTP
    client: gql.ShopifyGraphQLClient
    bulk_job_manager: BulkJobManager
    scopes: set[str]
    available_resources: set[type[ShopifyGraphQLResource]]


AUTHORIZATION_HEADER = "X-Shopify-Access-Token"


def _build_token_source(store_config: StoreConfig) -> TokenSource:
    """Construct the appropriate TokenSource based on the credential type.

    Each store gets its own independent TokenSource to avoid race conditions.

    - AccessToken (legacy shpat_* tokens): Static token, no OAuth flow needed.
    - ShopifyClientCredentials: Uses the CDK's built-in client_credentials
      OAuth2 flow with a store-specific token endpoint URL. The CDK handles
      token exchange, caching, and refresh automatically.
    """
    if isinstance(store_config.credentials, ShopifyClientCredentials):
        spec = OAuth2TokenFlowSpec(
            accessTokenUrlTemplate=f"https://{store_config.store}.myshopify.com/admin/oauth/access_token",
            accessTokenResponseMap={"access_token": "/access_token"},
        )
        return TokenSource(
            oauth_spec=spec,
            credentials=store_config.credentials,
            authorization_header=AUTHORIZATION_HEADER,
        )
    else:  # AccessToken
        return TokenSource(
            oauth_spec=None,
            credentials=store_config.credentials,
            authorization_header=AUTHORIZATION_HEADER,
        )


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
) -> StoreContext:
    """Create context for a single store."""
    token_source = _build_token_source(store_config)
    store_http = StoreHTTP(http, token_source)
    client = gql.ShopifyGraphQLClient(store_http, store_config.store)
    bulk_job_manager = BulkJobManager(client, log)

    if should_cancel_ongoing_job:
        await bulk_job_manager.cancel_current()

    granted_scopes = await _get_granted_scopes(store_http, client.url, log)

    # Both AccessToken and ShopifyClientCredentials are custom app tokens
    # subject to plan restrictions on PII access.
    uses_custom_app_token = isinstance(
        store_config.credentials, (AccessToken, ShopifyClientCredentials)
    )
    can_access_pii = True
    if uses_custom_app_token:
        can_access_pii = await _check_plan_allows_pii(store_http, client.url, log)

    available_resources = _get_available_resources(
        granted_scopes, uses_custom_app_token, can_access_pii
    )

    store_context = StoreContext(
        http=store_http,
        client=client,
        bulk_job_manager=bulk_job_manager,
        scopes=granted_scopes,
        available_resources=available_resources,
    )

    return store_context


def _get_available_resources(
    granted_scopes: set[str],
    uses_custom_app_token: bool,
    can_access_pii: bool,
) -> set[type[ShopifyGraphQLResource]]:
    """Determine which resources are available based on scopes and plan."""
    available: set[type[ShopifyGraphQLResource]] = set()

    for resource in INCREMENTAL_RESOURCES:
        # Skip if none of the qualifying scopes are granted
        if resource.QUALIFYING_SCOPES.isdisjoint(granted_scopes):
            continue
        # Skip PII resources for custom app tokens when plan doesn't allow PII
        if uses_custom_app_token and resource in PII_RESOURCES and not can_access_pii:
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
            backfill={
                sid: ResourceState.Backfill(
                    next_page=dt_to_str(start_date), cutoff=cutoff
                )
                for sid in store_ids
            },
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
    state.inc = {store_id: state.inc, "cursor": None}

    # Convert backfill state
    if isinstance(state.backfill, ResourceState.Backfill):
        state.backfill = {store_id: state.backfill, "cutoff": None, "next_page": None}
    elif state.backfill is None:
        # Completed backfill in flat mode → represent as dict with None entry
        state.backfill = {store_id: None}

    return True


async def _reconcile_connector_state(
    store_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
    legacy_store_id: str,
) -> None:
    """Reconcile connector state: migrate format if needed and add missing stores.

    This handles two scenarios:
    1. Migration from flat to dict-based state (legacy single-store captures)
    2. Adding missing store entries for newly configured stores
    """
    # Step 1: Migrate flat state to dict-based if needed.
    migrated = _migrate_flat_to_dict_state(state, legacy_store_id, task.log)

    # Step 2: Add missing store entries for newly configured stores.
    # Not all resources use backfill (bulk query resources don't), so backfill
    # may be None for both state and initial_state — only reconcile it when both are dicts.
    should_checkpoint = migrated

    if isinstance(state.inc, dict) and isinstance(initial_state.inc, dict):
        for store_id in store_ids:
            if store_id not in state.inc:
                task.log.info(f"Initializing new subtask state for store {store_id}.")
                state.inc[store_id] = deepcopy(initial_state.inc[store_id])
                should_checkpoint = True

    if isinstance(state.backfill, dict) and isinstance(initial_state.backfill, dict):
        for store_id in store_ids:
            if store_id not in state.backfill:
                state.backfill[store_id] = deepcopy(initial_state.backfill[store_id])
                should_checkpoint = True

    if should_checkpoint:
        task.log.info(
            f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}."
        )
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
        await http.request(
            log, url, method="POST", json={"query": AccessScopes.query()}
        )
    )
    scopes = response.get_scope_handles()
    log.info(f"Access token has scopes: {scopes}")
    return scopes


async def _validate_store(
    log: Logger,
    http: HTTPMixin,
    store_config: StoreConfig,
) -> str | None:
    """Validate credentials for a single store. Returns an error message or None."""
    token_source = _build_token_source(store_config)
    store_http = StoreHTTP(http, token_source)
    client = gql.ShopifyGraphQLClient(store_http, store_config.store)
    bulk_job_manager = BulkJobManager(client, log)

    try:
        await bulk_job_manager.check_connectivity()
    except HTTPError as err:
        if err.code == 401:
            return (
                f"Store '{store_config.store}': Invalid credentials. "
                f"Please confirm the provided credentials are correct.\n\n{err.message}"
            )
        return f"Store '{store_config.store}': Encountered error validating access token.\n\n{err.message}"

    return None


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Validate credentials for all configured stores in parallel."""
    results = await asyncio.gather(
        *[_validate_store(log, http, store_config) for store_config in config.stores],
        return_exceptions=True,
    )

    errors: list[str] = []
    for store_config, result in zip(config.stores, results):
        if isinstance(result, BaseException):
            raise RuntimeError(f"Store '{store_config.store}': {result}") from result
        if result is not None:
            errors.append(result)

    if errors:
        raise ValidationError(errors)


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_cancel_ongoing_job: bool = False,
) -> list[Resource]:
    """Discover all available resources across all configured stores.

    State format is always dictionary-based: {"inc": {"store_id": {...}}}

    Collection key format depends on config.advanced.should_use_composite_key:
    - True: ["/_meta/store", "/id"]
    - False: ["/id"]
    """
    store_contexts: dict[str, StoreContext] = {}

    # Initialize all store contexts in parallel
    results = await asyncio.gather(
        *[
            _create_store_context(log, http, store_config, should_cancel_ongoing_job)
            for store_config in config.stores
        ],
        return_exceptions=True,
    )

    failed_stores: dict[str, BaseException] = {}
    for store_config, result in zip(config.stores, results):
        if isinstance(result, BaseException):
            log.error(f"Failed to initialize store '{store_config.store}': {result}")
            failed_stores[store_config.store] = result
        else:
            excluded = [
                m.NAME
                for m in INCREMENTAL_RESOURCES
                if m not in result.available_resources
            ]
            if excluded:
                log.info(
                    f"Store '{store_config.store}' excluding {len(excluded)} stream(s) due to missing scopes or plan restrictions"
                )
            store_contexts[store_config.store] = result

    if not store_contexts:
        raise StoreInitError(failed_stores, [])

    if failed_stores:
        raise StoreInitError(failed_stores, list(store_contexts.keys()))

    # Determine which resources are available across all stores (union)
    all_available: set[type[ShopifyGraphQLResource]] = set()
    for ctx in store_contexts.values():
        all_available.update(ctx.available_resources)

    log.info(
        f"Discovered {len(all_available)} stream(s) across {len(store_contexts)} store(s)"
    )

    # Build resources
    resources: list[Resource] = []
    key = (
        ["/_meta/store", "/id"]
        if config.advanced.should_use_composite_key
        else ["/id"]
    )

    for model in INCREMENTAL_RESOURCES:
        stores_with_access = [
            store_id
            for store_id, ctx in store_contexts.items()
            if model in ctx.available_resources
        ]

        if not stores_with_access:
            continue

        use_backfill = not model.SHOULD_USE_BULK_QUERIES and model.SORT_KEY is not None
        initial_state = _create_initial_state(
            stores_with_access, config.start_date, use_backfill
        )
        legacy_store_id = config._legacy_store or config.stores[0].store

        def create_open_fn(
            model: type[ShopifyGraphQLResource],
            stores_with_access: list[str],
            initial_state: ResourceState,
            legacy_store_id: str,
        ):
            async def open(
                binding: CaptureBinding[ResourceConfig],
                binding_index: int,
                state: ResourceState,
                task: Task,
                _all_bindings,
            ):
                await _reconcile_connector_state(
                    stores_with_access,
                    binding,
                    state,
                    initial_state,
                    task,
                    legacy_store_id=legacy_store_id,
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
                    raise RuntimeError(
                        "Non-bulk queries cannot contain nested connections."
                    )

                data_model = create_response_data_model(model)

                # Subtask creation is driven by fetch_changes/fetch_page dict keys
                # (from stores_with_access), NOT by state keys. Orphaned state for
                # removed stores is inert and intentionally preserved.
                fetch_changes: dict[str, functools.partial] = {}
                fetch_page: dict[str, functools.partial] = {}

                for store_id in stores_with_access:
                    ctx = store_contexts[store_id]

                    if model.SHOULD_USE_BULK_QUERIES:
                        fetch_changes[store_id] = functools.partial(
                            bulk_fetch_incremental,
                            ctx.http,
                            config.advanced.window_size,
                            ctx.bulk_job_manager,
                            model,
                            store_id,
                        )
                    elif model.SORT_KEY is None:
                        fetch_changes[store_id] = functools.partial(
                            fetch_incremental_unsorted,
                            ctx.client,
                            model,
                            data_model,
                            store_id,
                        )
                    else:
                        fetch_changes[store_id] = functools.partial(
                            fetch_incremental,
                            ctx.client,
                            model,
                            data_model,
                            store_id,
                        )
                        fetch_page[store_id] = functools.partial(
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
                    fetch_changes=fetch_changes,
                    fetch_page=fetch_page if fetch_page else None,
                )

            return open

        resources.append(
            Resource(
                name=model.NAME,
                key=key,
                model=ShopifyGraphQLResource,
                open=create_open_fn(
                    model, stores_with_access, initial_state, legacy_store_id
                ),
                initial_state=initial_state,
                schema_inference=True,
                initial_config=ResourceConfig(
                    name=model.NAME, interval=timedelta(minutes=5)
                ),
            )
        )

    return resources
