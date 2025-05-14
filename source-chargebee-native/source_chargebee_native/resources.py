from datetime import timedelta, datetime, UTC
from logging import Logger
import functools
from typing import Literal

from estuary_cdk.capture import common, Task
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from source_chargebee_native.models import (
    EndpointConfig,
    ChargebeeResource,
    IncrementalChargebeeResource,
    AssociationConfig,
    ChargebeeConfiguration,
    APIResponse,
    Customer,
    Comment,
    Subscription,
    Addon,
    AttachedItem,
    Coupon,
    CreditNote,
    Event,
    HostedPage,
    Invoice,
    Item,
    ItemPrice,
    ItemFamily,
    Order,
    PaymentSource,
    Plan,
    PromotionalCredits,
    Quote,
    Transaction,
    VirtualBankAccount,
)
from source_chargebee_native.api import (
    snapshot_resource,
    fetch_resource_page,
    fetch_resource_changes,
    snapshot_associated_resource,
    fetch_associated_resource_page,
    fetch_associated_resource_changes,
)


RESTRICTED_RESOURCES = [
    "quotes",  # Requires Performance or Enterprise Chargebee subscription plan
    "quote_line_groups",  # Requires Performance or Enterprise Chargebee subscription plan
]

COMMON_STANDALONE_FULL_REFRESH_RESOURCES = [
    "site_migration_details",
    "quote_line_groups",
    "gifts",
    "unbilled_charges",
]

FULL_REFRESH_RESOURCES: dict[Literal["1.0", "2.0"], list[str]] = {
    "1.0": [
        *COMMON_STANDALONE_FULL_REFRESH_RESOURCES,
    ],
    "2.0": [
        *COMMON_STANDALONE_FULL_REFRESH_RESOURCES,
        "differential_prices",
    ],
}

COMMON_INCREMENTAL_RESOURCES: list[tuple[str, type[IncrementalChargebeeResource]]] = [
    ("comments", Comment),
    ("coupons", Coupon),
    ("credit_notes", CreditNote),
    ("customers", Customer),
    ("events", Event),
    ("hosted_pages", HostedPage),
    ("invoices", Invoice),
    ("orders", Order),
    ("payment_sources", PaymentSource),
    ("promotional_credits", PromotionalCredits),
    ("quotes", Quote),
    ("subscriptions", Subscription),
    ("transactions", Transaction),
    ("virtual_bank_accounts", VirtualBankAccount),
]

INCREMENTAL_RESOURCES: dict[
    Literal["1.0", "2.0"], list[tuple[str, type[IncrementalChargebeeResource]]]
] = {
    "1.0": [
        *COMMON_INCREMENTAL_RESOURCES,
        ("addons", Addon),
        ("plans", Plan),
    ],
    "2.0": [
        *COMMON_INCREMENTAL_RESOURCES,
        ("items", Item),
        ("item_prices", ItemPrice),
        ("item_families", ItemFamily),
    ],
}

COMMON_ASSOCIATED_FULL_REFRESH_RESOURCES: dict[str, AssociationConfig] = {
    "contacts": AssociationConfig(
        parent_resource="customers",
        parent_response_key="customer",
        parent_key_field="id",
        endpoint_pattern="{parent}/{id}/contacts",
        returns_list=True,
    ),
    "subscriptions_with_scheduled_changes": AssociationConfig(
        parent_resource="subscriptions",
        parent_response_key="subscription",
        parent_key_field="id",
        endpoint_pattern="{parent}/{id}/retrieve_with_scheduled_changes",
        parent_filter_params={
            "has_scheduled_changes[is]": "true"
        },  # Filter to prevent 400 error when no scheduled changes
        returns_list=False,
    ),
}

ASSOCIATED_FULL_REFRESH_RESOURCES: dict[
    Literal["1.0", "2.0"], dict[str, AssociationConfig]
] = {
    "1.0": {
        **COMMON_ASSOCIATED_FULL_REFRESH_RESOURCES,
    },
    "2.0": {
        **COMMON_ASSOCIATED_FULL_REFRESH_RESOURCES,
    },
}

COMMON_ASSOCIATED_INCREMENTAL_RESOURCES: dict[
    str, tuple[AssociationConfig, type[IncrementalChargebeeResource]]
] = {}

ASSOCIATED_INCREMENTAL_RESOURCES: dict[
    Literal["1.0", "2.0"],
    dict[str, tuple[AssociationConfig, type[IncrementalChargebeeResource]]],
] = {
    "1.0": {
        **COMMON_ASSOCIATED_INCREMENTAL_RESOURCES,
    },
    "2.0": {
        **COMMON_ASSOCIATED_INCREMENTAL_RESOURCES,
        "attached_items": (
            AssociationConfig(
                parent_resource="items",
                parent_response_key="item",
                parent_key_field="id",
                endpoint_pattern="{parent}/{id}/attached_items",
                returns_list=True,
            ),
            AttachedItem,
        ),
    },
}


async def validate_credentials_and_configuration(
    log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )
    url = f"https://{config.site}.chargebee.com/api/v2/configurations"

    try:
        configuration = APIResponse[ChargebeeConfiguration].model_validate_json(
            await http.request(log, url)
        )
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])

    if not configuration.configurations or len(configuration.configurations) != 1:
        raise ValidationError(
            [
                "Encountered error validating configuration.\n\n"
                "Please ensure that the provided configuration is correct."
            ]
        )

    if configuration.configurations[0].product_catalog != config.product_catalog:
        raise ValidationError(
            [
                "Encountered error validating configuration.\n\n"
                "Please ensure that the provided product catalog version is correct.\n\n"
                f"Provided: {config.product_catalog}, Actual: {configuration.configurations[0].product_catalog}."
            ]
        )


async def check_feature_availability(
    log: Logger,
    http: HTTPMixin,
    site: str,
    feature: str,
) -> bool:
    try:
        url = f"https://{site}.chargebee.com/api/v2/{feature}"
        await http.request(log, url, params={"limit": 1})
        return True
    except HTTPError as err:
        if err.code == 404:
            return False
        raise


async def full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open_standalone(
        resource_name: str,
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resource,
                http,
                config.site,
                resource_name,
                config.advanced.limit,
            ),
            tombstone=ChargebeeResource(_meta=ChargebeeResource.Meta(op="d")),
        )

    available_resources = []
    for name in FULL_REFRESH_RESOURCES[config.product_catalog]:
        if name in RESTRICTED_RESOURCES:
            if not await check_feature_availability(log, http, config.site, name):
                continue
        available_resources.append(name)

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=ChargebeeResource,
            open=functools.partial(open_standalone, name),
            initial_state=common.ResourceState(),
            initial_config=common.ResourceConfig(
                name=name, interval=timedelta(hours=1)
            ),
            schema_inference=True,
        )
        for name in available_resources
    ]


async def incremental_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        resource_name: str,
        resource_type: type[IncrementalChargebeeResource],
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_resource_page,
                http,
                config.site,
                resource_name,
                resource_type,
                config.start_date,
                config.advanced.limit,
            ),
            fetch_changes=functools.partial(
                fetch_resource_changes,
                http,
                config.site,
                resource_name,
                resource_type,
                config.advanced.limit,
            ),
        )

    cutoff = datetime.now(tz=UTC)

    available_resources = []
    for name, resource_type in INCREMENTAL_RESOURCES[config.product_catalog]:
        if name in RESTRICTED_RESOURCES:
            if not await check_feature_availability(log, http, config.site, name):
                continue
        available_resources.append((name, resource_type))

    return [
        common.Resource(
            name=name,
            key=[resource_type.get_resource_key_json_path()],
            model=resource_type,
            open=functools.partial(open, name, resource_type),
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=int(cutoff.timestamp())),
                backfill=common.ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=common.ResourceConfig(
                name=name, interval=timedelta(minutes=2)
            ),
            schema_inference=True,
        )
        for name, resource_type in available_resources
    ]


async def associated_full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open_child(
        association_config: AssociationConfig,
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_associated_resource,
                http,
                config.site,
                association_config,
                config.advanced.limit,
            ),
            tombstone=ChargebeeResource(_meta=ChargebeeResource.Meta(op="d")),
        )

    available_resources = []
    for name, association_config in ASSOCIATED_FULL_REFRESH_RESOURCES[
        config.product_catalog
    ].items():
        if name in RESTRICTED_RESOURCES:
            if not await check_feature_availability(log, http, config.site, name):
                continue
        available_resources.append((name, association_config))

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=ChargebeeResource,
            open=functools.partial(open_child, association_config),
            initial_state=common.ResourceState(),
            initial_config=common.ResourceConfig(
                name=name, interval=timedelta(minutes=2)
            ),
            schema_inference=True,
        )
        for name, association_config in available_resources
    ]


async def associated_incremental_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open_incremental_child(
        association_config: AssociationConfig,
        resource_type: type[IncrementalChargebeeResource],
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_associated_resource_page,
                http,
                config.site,
                association_config,
                resource_type,
                config.start_date,
                config.advanced.limit,
            ),
            fetch_changes=functools.partial(
                fetch_associated_resource_changes,
                http,
                config.site,
                association_config,
                resource_type,
                config.advanced.limit,
            ),
        )

    cutoff = datetime.now(tz=UTC)
    available_resources = []
    for name, (association_config, resource_type) in ASSOCIATED_INCREMENTAL_RESOURCES[
        config.product_catalog
    ].items():
        if name in RESTRICTED_RESOURCES:
            if not await check_feature_availability(log, http, config.site, name):
                continue
        available_resources.append((name, (association_config, resource_type)))

    return [
        common.Resource(
            name=name,
            key=[resource_type.get_resource_key_json_path()],
            model=resource_type,
            open=functools.partial(
                open_incremental_child,
                association_config,
                resource_type,
            ),
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=int(cutoff.timestamp())),
                backfill=common.ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=common.ResourceConfig(
                name=name, interval=timedelta(minutes=2)
            ),
            schema_inference=True,
        )
        for name, (
            association_config,
            resource_type,
        ) in available_resources
    ]


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )

    return [
        *await full_refresh_resources(log, http, config),
        *await incremental_resources(log, http, config),
        *await associated_full_refresh_resources(log, http, config),
        *await associated_incremental_resources(log, http, config),
    ]
