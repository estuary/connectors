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
    Customer,
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
    Quote,
    Transaction,
    VirtualBankAccount,
)
from source_chargebee_native.api import (
    snapshot_resource,
    fetch_resource_page,
    fetch_resource_changes,
)


# TODO(jsmith): revisit
# PARENT_CHILD_RESOURCES: dict[str, tuple[str, str]] = {
#     "contacts": ("customers", "id"),  # (parent_resource, parent_key_field)
#     "subscriptions_with_scheduled_changes": ("subscriptions", "id"),
# }

STANDALONE_FULL_REFRESH_RESOURCES: dict[Literal["1.0", "2.0"], list[str]] = {
    "1.0": [
        # "comments", # TODO(jsmith): revisit - potentially incremental
        # "gifts", # TODO(jsmith): revisit - potential client-side filtering
        # "promotional_credits", # TODO(jsmith): revisit - potentially incremental
        "site_migration_details",
        # "unbilled_charges", # TODO(jsmith): revisit - potential client-side filtering
        # "quote_line_groups", # TODO(jsmith): add this back in after figuring out the best way to handle 404 (quotes feature not enabled)
    ],
    "2.0": [
        # "comments", # TODO(jsmith): revisit - potentially incremental
        # "differential_prices", # TODO(jsmith): revisit
        # "gifts", # TODO(jsmith): revisit - potential client-side filtering
        # "promotional_credits", # TODO(jsmith): revisit - potentially incremental
        "site_migration_details",
        # "unbilled_charges", # TODO(jsmith): revisit - potential client-side filtering
        # "quote_line_groups", # TODO(jsmith): add this back in after figuring out the best way to handle 404 (quotes feature not enabled)
    ],
}

# TODO(jsmith): revisit
# CHILD_FULL_REFRESH_RESOURCES: list[str] = [
#     "contacts",
#     "subscriptions_with_scheduled_changes",
# ]

INCREMENTAL_RESOURCES: dict[
    Literal["1.0", "2.0"], list[tuple[str, type[IncrementalChargebeeResource]]]
] = {
    "1.0": [
        ("addons", Addon),
        ("coupons", Coupon),
        ("credit_notes", CreditNote),
        ("customers", Customer),
        ("events", Event),
        ("hosted_pages", HostedPage),
        ("invoices", Invoice),
        ("orders", Order),
        ("payment_sources", PaymentSource),
        ("plans", Plan),
        ("quotes", Quote),
        ("subscriptions", Subscription),
        ("transactions", Transaction),
        ("virtual_bank_accounts", VirtualBankAccount),
    ],
    "2.0": [
        ("addons", Addon),
        ("attached_items", AttachedItem),
        ("coupons", Coupon),
        ("credit_notes", CreditNote),
        ("customers", Customer),
        ("events", Event),
        ("hosted_pages", HostedPage),
        ("invoices", Invoice),
        ("items", Item),
        ("item_prices", ItemPrice),
        ("item_families", ItemFamily),
        ("orders", Order),
        ("payment_sources", PaymentSource),
        ("plans", Plan),
        ("quotes", Quote),
        ("subscriptions", Subscription),
        ("transactions", Transaction),
        ("virtual_bank_accounts", VirtualBankAccount),
    ],
}


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )
    url = f"https://{config.site}.chargebee.com/api/v2/customers"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


async def full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    resources = []

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
            ),
            tombstone=ChargebeeResource(_meta=ChargebeeResource.Meta(op="d")),
        )

    for name in STANDALONE_FULL_REFRESH_RESOURCES[config.product_catalog]:
        resources.append(
            common.Resource(
                name=name,
                key=["/_meta/row_id"],
                model=ChargebeeResource,
                open=functools.partial(open_standalone, name),
                initial_state=common.ResourceState(),
                initial_config=common.ResourceConfig(
                    name=name, interval=timedelta(minutes=2)
                ),
                schema_inference=True,
            )
        )

    return resources


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
            ),
            fetch_changes=functools.partial(
                fetch_resource_changes,
                http,
                config.site,
                resource_name,
                resource_type,
            ),
        )

    cutoff = datetime.now(tz=UTC)

    return [
        common.Resource(
            name=name,
            key=[resource_type.RESOURCE_KEY_JSON_PATH],
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
        for name, resource_type in INCREMENTAL_RESOURCES[config.product_catalog]
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
    )

    return [
        *await full_refresh_resources(log, http, config),
        *await incremental_resources(log, http, config),
    ]
