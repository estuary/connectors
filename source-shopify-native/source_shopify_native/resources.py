import functools
from datetime import datetime, timedelta, UTC
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
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
    AccessToken,
    EndpointConfig,
    ShopifyGraphQLResource,
    ShopDetails,
    PlanName,
    create_response_data_model,
)
from .api import (
    bulk_fetch_incremental,
    fetch_incremental_unsorted,
    fetch_incremental,
    backfill_incremental,
)
from.graphql.common import dt_to_str

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
]


PII_RESOURCES: list[type[ShopifyGraphQLResource]] = [
    gql.Customers,
    gql.Orders,
    gql.FulfillmentOrders,
]


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


def _incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
    client: gql.ShopifyGraphQLClient,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
) -> list[Resource]:
    def open(
        model: type[ShopifyGraphQLResource],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings=None,
    ):
        data_model = create_response_data_model(model)

        if model.SHOULD_USE_BULK_QUERIES:
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=functools.partial(
                    bulk_fetch_incremental,
                    http,
                    config.advanced.window_size,
                    bulk_job_manager,
                    model,
                ),
            )
        else:
            # Non-bulk queries cannot be used for queries that have nested
            # connections. Any query with "edges { node { blah } }" in it
            # contains a nested connection.
            if "edges" in model.QUERY.lower():
                raise RuntimeError("Implementation error: Non-bulk queries cannot contain nested connections.")

            if model.SORT_KEY is None:
                open_binding(
                    binding,
                    binding_index,
                    state,
                    task,
                    fetch_changes=functools.partial(
                        fetch_incremental_unsorted,
                        client,
                        model,
                        data_model,
                    ),
                )
            else:
                open_binding(
                    binding,
                    binding_index,
                    state,
                    task,
                    fetch_changes=functools.partial(
                        fetch_incremental,
                        client,
                        model,
                        data_model,
                    ),
                    fetch_page=functools.partial(
                        backfill_incremental,
                        client,
                        model,
                        data_model,
                    )
                )

    cutoff = datetime.now(tz=UTC)

    resources: list[Resource] = []

    for model in INCREMENTAL_RESOURCES:
        if model.SHOULD_USE_BULK_QUERIES or model.SORT_KEY is None:
            initial_state = ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date)
            )
        else:
            initial_state = ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
            )

        resources.append(
            Resource(
                name=model.NAME,
                key=["/id"],
                model=ShopifyGraphQLResource,
                open=functools.partial(open, model),
                initial_state=initial_state,
                initial_config=ResourceConfig(name=model.NAME, interval=timedelta(minutes=5)),
                schema_inference=True,
            )
        )

    return resources


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    client = gql.ShopifyGraphQLClient(http, config.store)
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(client, log)

    try:
        await bulk_job_manager._get_currently_running_job()
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating access token.\n\n{err.message}"

        raise ValidationError([msg])


async def all_resources(
        log: Logger,
        http: HTTPMixin,
        config: EndpointConfig,
        should_cancel_ongoing_job: bool = False
) -> list[Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    client = gql.ShopifyGraphQLClient(http, config.store)
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(client, log)

    # Before opening bindings, cancel any ongoing bulk query jobs before the 
    # connector starts submitting its own bulk query jobs.
    if should_cancel_ongoing_job:
        await bulk_job_manager.cancel_current()


    resources = [
        *_incremental_resources(http, config, client, bulk_job_manager),
    ]

    # If the user is authenticating with an access token from their custom Shopify app,
    # they may not be able to access certain PII data.
    # https://help.shopify.com/en/manual/apps/app-types/custom-apps
    # https://community.shopify.com/c/shopify-discussions/no-more-customer-pii-in-custom-app-integrations-for-shopify/td-p/2496209
    if isinstance(config.credentials, AccessToken) and not await _can_access_pii(http, client.url, log):
        for model in PII_RESOURCES:
            resources = [
                r for r in resources if r.name != model.NAME
            ]

    return resources
