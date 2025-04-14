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
    FullRefreshResource,
    ShopifyGraphQlResource,
    IncrementalResource,
)
from .api import (
    fetch_full_refresh,
    fetch_incremental,
)

AUTHORIZATION_HEADER = "X-Shopify-Access-Token"

INCREMENTAL_RESOURCES: list[tuple[str, type[ShopifyGraphQlResource]]] = [
    ("abandoned_checkouts", gql.abandoned_checkouts.AbandonedCheckouts),
    ("products", gql.products.Products),
    ("customers", gql.customers.Customers),
    ("orders", gql.orders.Orders),
    ("inventory_items", gql.inventory_items.InventoryItems),
    ("custom_collections", gql.custom_collections.CustomCollections),
]
FULL_REFRESH_RESOURCES: list[tuple[str, type[ShopifyGraphQlResource]]] = [
    ("locations", gql.locations.Locations),
]


async def _incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
) -> list[Resource]:
    def open_bulk(
        model: type[ShopifyGraphQlResource],
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
            model=IncrementalResource,
            open=functools.partial(open_bulk, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(name=name, interval=timedelta(minutes=5)),
            schema_inference=True,
        )
        for name, model in INCREMENTAL_RESOURCES
    ]


async def _full_refresh_resources(
    http: HTTPMixin,
    config: EndpointConfig,
    bulk_job_manager: gql.bulk_job_manager.BulkJobManager,
) -> list[Resource]:
    def open(
        model: type[ShopifyGraphQlResource],
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
            fetch_snapshot=functools.partial(
                fetch_full_refresh,
                http,
                config.start_date,
                config.advanced.window_size,
                bulk_job_manager,
                model,
            ),
            tombstone=model(_meta=model.Meta(op="d")),
        )

    return [
        Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(name=name, interval=timedelta(hours=1)),
            schema_inference=True,
        )
        for name, model in FULL_REFRESH_RESOURCES
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


async def all_resources(log: Logger, http: HTTPMixin, config: EndpointConfig) -> list:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    bulk_job_manager = gql.bulk_job_manager.BulkJobManager(http, log, config.store)

    # Cancel any ongoing bulk query jobs before the connector starts submitting its own bulk query jobs.
    await bulk_job_manager.cancel_current()

    return [
        *await _incremental_resources(http, config, bulk_job_manager),
        *await _full_refresh_resources(http, config, bulk_job_manager),
    ]
