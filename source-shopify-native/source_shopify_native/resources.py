import functools
from datetime import timedelta
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
    EndpointConfig,
    ShopifyGraphQLResource,
)
from .api import (
    fetch_incremental,
)

AUTHORIZATION_HEADER = "X-Shopify-Access-Token"

INCREMENTAL_RESOURCES: list[tuple[str, type[ShopifyGraphQLResource]]] = [
    ("abandoned_checkouts", gql.AbandonedCheckouts),
    ("customers", gql.Customers),
    ("customer_metafields", gql.CustomerMetafields),
    ("products", gql.Products),
    ("product_media", gql.ProductMedia),
    ("product_metafields", gql.ProductMetafields),
    ("product_variants", gql.ProductVariants),
    ("fulfillment_orders", gql.FulfillmentOrders),
    ("fulfillments", gql.Fulfillments),
    ("orders", gql.Orders),
    ("order_agreements", gql.OrderAgreements),
    ("order_metafields", gql.OrderMetafields),
    ("order_transactions", gql.OrderTransactions),
    ("order_refunds", gql.OrderRefunds),
    ("order_risks", gql.OrderRisks),
    ("inventory_items", gql.InventoryItems),
    ("inventory_levels", gql.InventoryLevels),
    ("custom_collections", gql.CustomCollections),
    ("smart_collections", gql.SmartCollections),
    ("custom_collection_metafields", gql.CustomCollectionMetafields),
    ("smart_collection_metafields", gql.SmartCollectionMetafields),
    ("locations", gql.Locations),
    ("location_metafields", gql.LocationMetafields),
]


def _incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
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
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_incremental,
                http,
                config.advanced.window_size,
                bulk_job_manager,
                model,
            ),
        )

    return [
        Resource(
            name=name,
            key=["/id"],
            model=ShopifyGraphQLResource,
            open=functools.partial(open, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(name=name, interval=timedelta(minutes=5)),
            schema_inference=True,
        )
        for name, model in INCREMENTAL_RESOURCES
    ]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(http, log, config.store)

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
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(http, log, config.store)

    # Before opening bindings, cancel any ongoing bulk query jobs before the 
    # connector starts submitting its own bulk query jobs.
    if should_cancel_ongoing_job:
        await bulk_job_manager.cancel_current()

    return [
        *_incremental_resources(http, config, bulk_job_manager),
    ]
