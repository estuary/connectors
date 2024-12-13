import braintree
from braintree import BraintreeGateway
from braintree.exceptions.authentication_error import AuthenticationError
from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    FullRefreshResource,
    IncrementalResource,
    IncrementalResourceFetchChangesFn,
)

from .api import (
    _dt_to_str,
    snapshot_resources,
    fetch_transactions,
    backfill_transactions,
    fetch_credit_card_verifications,
    fetch_customers,
    fetch_disputes,
    fetch_subscriptions,
)


# Supported full refresh resources and their corresponding name, gateway property, and gateway response property.
FULL_REFRESH_RESOURCES: list[tuple[str, str, str | None]] = [
    ("merchant_accounts", "merchant_account", "merchant_accounts"),
    ("discounts", "discount", None),
    ("add_ons", "add_on", None),
    ("plans", "plan", None),
]

# Supported incremental resources and their corresponding name and fetch_changes function.
INCREMENTAL_RESOURCES: list[tuple[str, IncrementalResourceFetchChangesFn]] = [
    ("credit_card_verifications", fetch_credit_card_verifications),
    ("customers", fetch_customers),
    ("disputes", fetch_disputes),
    ("subscriptions", fetch_subscriptions),
]


def _create_gateway(config: EndpointConfig) -> BraintreeGateway:
    environment = braintree.Environment.Sandbox if config.advanced.is_sandbox else braintree.Environment.Production

    return braintree.BraintreeGateway(
        braintree.Configuration(
            environment=environment,
            merchant_id=config.merchant_id,
            public_key=config.credentials.public_key,
            private_key=config.credentials.private_key,
        )
    )

def validate_credentials(
        log: Logger, config: EndpointConfig
):
    gateway = _create_gateway(config)

    try:
        gateway.discount.all()
    except AuthenticationError:
        msg = f"Encountered issue while validating credentials. Please confirm provided endpoint configuration and API key credentials are correct."
        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, config: EndpointConfig,
) -> list[common.Resource]:

    def open(
            gateway: BraintreeGateway,
            gateway_property: str,
            gateway_response_field: str | None,
            binding: CaptureBinding[ResourceConfig],
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
                snapshot_resources,
                gateway,
                gateway_property,
                gateway_response_field,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, _create_gateway(config), gateway_property, gateway_response_field),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, gateway_property, gateway_response_field) in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(
        log: Logger, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            fetch_changes_fn: IncrementalResourceFetchChangesFn,
            gateway: BraintreeGateway,
            window_size: int,
            binding: CaptureBinding[ResourceConfig],
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
                fetch_changes_fn,
                gateway,
                window_size,
            ),
        )

    return [
        common.Resource(
            name=name,
            key=["/id"],
            model=IncrementalResource,
            open=functools.partial(open, fetch_changes_fn, _create_gateway(config), config.advanced.window_size),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, fetch_changes_fn) in INCREMENTAL_RESOURCES
    ]


def transactions(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
            window_size: int,
            binding: CaptureBinding[ResourceConfig],
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
                gateway,
                window_size,
            ),
            fetch_page=functools.partial(
                backfill_transactions,
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
            backfill=ResourceState.Backfill(next_page=_dt_to_str(config.start_date), cutoff=cutoff)
        ),
        initial_config=ResourceConfig(
            name='transactions', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        *full_refresh_resources(log, config),
        *incremental_resources(log, config),
        transactions(log, config),
    ]