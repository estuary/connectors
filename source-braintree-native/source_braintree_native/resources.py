import braintree
from braintree import BraintreeGateway
from braintree.exceptions.authentication_error import AuthenticationError
from datetime import timedelta
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
    FULL_REFRESH_RESOURCES,
    Transaction,
    Customer,
    Dispute,
    Subscription,
    CreditCardVerification,
)
from .api import (
    snapshot_resources,
    fetch_transactions,
    fetch_credit_card_verifications,
    fetch_customers,
    fetch_disputes,
    fetch_subscriptions,
)


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
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"), id=None)
        )

    return [
        common.Resource(
            name=name,
            key=["/id"],
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


def transactions(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
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
            ),
        )

    gateway = _create_gateway(config)

    return common.Resource(
        name='transactions',
        key=["/id"],
        model=Transaction,
        open=functools.partial(open, gateway),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='transactions', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )

def customers(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
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
                fetch_customers,
                gateway,
            ),
        )

    gateway = _create_gateway(config)

    return common.Resource(
        name='customers',
        key=["/id"],
        model=Customer,
        open=functools.partial(open, gateway),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='customers', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def disputes(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
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
                fetch_disputes,
                gateway,
            ),
        )

    gateway = _create_gateway(config)

    return common.Resource(
        name='disputes',
        key=["/id"],
        model=Dispute,
        open=functools.partial(open, gateway),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='disputes', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def subscriptions(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
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
                fetch_subscriptions,
                gateway,
            ),
        )

    gateway = _create_gateway(config)

    return common.Resource(
        name='subscriptions',
        key=["/id"],
        model=Subscription,
        open=functools.partial(open, gateway),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='subscriptions', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def credit_card_verifications(
        log: Logger, config: EndpointConfig
) -> common.Resource:

    def open(
            gateway: BraintreeGateway,
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
                fetch_credit_card_verifications,
                gateway,
            ),
        )

    gateway = _create_gateway(config)

    return common.Resource(
        name='credit_card_verifications',
        key=["/id"],
        model=CreditCardVerification,
        open=functools.partial(open, gateway),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='credit_card_verifications', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        *full_refresh_resources(log, config),
        transactions(log, config),
        disputes(log, config),
        subscriptions(log, config),
        customers(log, config),
        credit_card_verifications(log, config),
    ]