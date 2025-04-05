import braintree
from braintree import BraintreeGateway
from braintree.exceptions.authentication_error import AuthenticationError
from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource


from .models import (
    EndpointConfig,
    ResourceConfigWithSchedule,
    ResourceState,
    FullRefreshResource,
    IncrementalResource,
    SnapshotFn,
    NonPaginatedSnapshotBraintreeClass,
    IncrementalResourceBraintreeClass,
    AddOnsResponse,
    DiscountsResponse,
    PlansResponse,
    NonPaginatedSnapshotResponse,
    SearchResponse,
    CreditCardVerificationSearchResponse,
    CustomerSearchResponse,
    SubscriptionSearchResponse,
)

from .api import (
    dt_to_str,
    fetch_transactions,
    backfill_transactions,
    fetch_incremental_resources,
    backfill_incremental_resources,
    fetch_disputes,
    backfill_disputes,
)

from .api.merchant_accounts import snapshot_merchant_accounts
from .api.non_paginated_snapshot_resource import snapshot_non_paginated_resource

# Supported paginated full refresh resources and their fetch_snapshot function.
PAGINATED_FULL_REFRESH_RESOURCES: list[tuple[str, SnapshotFn]] = [
    ("merchant_accounts", snapshot_merchant_accounts),
]


# Supported non-paginated full refresh resources and their name, path component, response mode, and braintree class.
NON_PAGINATED_FULL_REFRESH_RESOURCES: list[tuple[str, str, type[NonPaginatedSnapshotResponse], NonPaginatedSnapshotBraintreeClass]] = [
    ("discounts", "discounts", DiscountsResponse, braintree.Discount),
    ("add_ons", "add_ons", AddOnsResponse, braintree.AddOn),
    ("plans", "plans", PlansResponse, braintree.Plan),
]


# Supported incremental resources and their corresponding name, path component, response model, and braintree class.
INCREMENTAL_RESOURCES: list[tuple[str, str, type[SearchResponse], IncrementalResourceBraintreeClass]] = [
    ("credit_card_verifications", "verifications", CreditCardVerificationSearchResponse, braintree.CreditCardVerification),
    ("customers", "customers", CustomerSearchResponse, braintree.Customer),
    ("subscriptions", "subscriptions", SubscriptionSearchResponse, braintree.Subscription),
]


def _create_gateway(config: EndpointConfig) -> BraintreeGateway:
    environment = braintree.Environment.Sandbox if config.advanced.is_sandbox else braintree.Environment.Production

    return braintree.BraintreeGateway(
        braintree.Configuration(
            environment=environment,
            merchant_id=config.merchant_id,
            public_key=config.credentials.username,
            private_key=config.credentials.password,
        )
    )


def _build_base_url(is_sandbox: bool, merchant_id: str) -> str:
    return f"https://api.{"sandbox." if is_sandbox else ""}braintreegateway.com/merchants/{merchant_id}"


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    base_url = _build_base_url(config.advanced.is_sandbox, config.merchant_id)
    gateway = _create_gateway(config)

    try:
        async for _ in snapshot_non_paginated_resource(
            http,
            base_url,
            "discounts",
            DiscountsResponse,
            gateway,
            braintree.Discount,
            log,
        ):
            break

    except AuthenticationError:
        msg = f"Encountered issue while validating credentials. Please confirm provided endpoint configuration and API key credentials are correct."
        raise ValidationError([msg])


def non_paginated_full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig, base_url: str,
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[NonPaginatedSnapshotResponse],
            gateway: BraintreeGateway,
            braintree_class: NonPaginatedSnapshotBraintreeClass,
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_non_paginated_resource,
                http,
                base_url,
                path,
                response_model,
                gateway,
                braintree_class,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, path, response_model, _create_gateway(config), braintree_class),
            initial_state=ResourceState(),
            initial_config=ResourceConfigWithSchedule(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, path, response_model, braintree_class) in NON_PAGINATED_FULL_REFRESH_RESOURCES
    ]


def paginated_full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig, base_url: str,
) -> list[common.Resource]:

    def open(
            snapshot_fn: SnapshotFn,
            gateway: BraintreeGateway,
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_fn,
                http,
                base_url,
                gateway,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, snapshot_fn, _create_gateway(config)),
            initial_state=ResourceState(),
            initial_config=ResourceConfigWithSchedule(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, snapshot_fn) in PAGINATED_FULL_REFRESH_RESOURCES
    ]


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig, base_url: str,
) -> list[common.Resource]:

    def open(
            path: str,
            response_model: type[SearchResponse],
            gateway: BraintreeGateway,
            braintree_class: IncrementalResourceBraintreeClass,
            window_size: int,
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_incremental_resources,
                http,
                base_url,
                path,
                response_model,
                gateway,
                braintree_class,
                window_size,
            ),
            fetch_page=functools.partial(
                backfill_incremental_resources,
                http,
                base_url,
                path,
                response_model,
                gateway,
                braintree_class,
                window_size,
            )
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return [
        common.Resource(
            name=name,
            key=["/id"],
            model=IncrementalResource,
            open=functools.partial(open, path, response_model, _create_gateway(config), braintree_class, config.advanced.window_size),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
            ),
            initial_config=ResourceConfigWithSchedule(
                name=name, interval=timedelta(minutes=5), schedule="0 20 * * 5",
            ),
            schema_inference=True,
        )
        for (name, path, response_model, braintree_class) in INCREMENTAL_RESOURCES
    ]


def transactions(
        log: Logger, http: HTTPMixin, config: EndpointConfig, base_url: str,
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
            window_size: int,
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_transactions,
                http,
                base_url,
                gateway,
                window_size,
            ),
            fetch_page=functools.partial(
                backfill_transactions,
                http,
                base_url,
                gateway,
                window_size,
            )
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return common.Resource(
        name='transactions',
        key=["/id"],
        model=IncrementalResource,
        open=functools.partial(open, _create_gateway(config), config.advanced.window_size),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
        ),
        initial_config=ResourceConfigWithSchedule(
            name='transactions', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )

def disputes(
        log: Logger, http: HTTPMixin, config: EndpointConfig, base_url: str,
) -> common.Resource:

    def open(
            window_size: int,
            binding: CaptureBinding[ResourceConfigWithSchedule],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_disputes,
                http,
                base_url,
                window_size,
            ),
            fetch_page=functools.partial(
                backfill_disputes,
                http,
                base_url,
                window_size,
            )
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return common.Resource(
        name='disputes',
        key=["/id"],
        model=IncrementalResource,
        open=functools.partial(open, config.advanced.window_size),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=dt_to_str(config.start_date), cutoff=cutoff)
        ),
        initial_config=ResourceConfigWithSchedule(
            name='disputes', interval=timedelta(minutes=5), schedule="0 20 * * 5"
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    base_url = _build_base_url(config.advanced.is_sandbox, config.merchant_id)

    return [
        *paginated_full_refresh_resources(log, http, config, base_url),
        *non_paginated_full_refresh_resources(log, http, config, base_url),
        *incremental_resources(log, http, config, base_url),
        transactions(log, http, config, base_url),
        disputes(log, http, config, base_url),
    ]
