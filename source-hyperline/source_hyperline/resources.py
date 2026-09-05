import functools
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_audit_logs,
    backfill_customer_credits,
    backfill_customers,
    backfill_invoices,
    backfill_quotes,
    backfill_subscriptions,
    base_url,
    fetch_audit_logs,
    fetch_customer_credits,
    fetch_customers,
    fetch_invoices,
    fetch_quotes,
    fetch_subscriptions,
    snapshot_coupons,
    snapshot_customer_segments,
    snapshot_features,
    snapshot_invoicing_entities,
    snapshot_price_books,
    snapshot_price_configurations,
    snapshot_products,
    snapshot_promotion_codes,
    snapshot_subscription_transitions,
    snapshot_tax_rates,
    snapshot_transactions,
    snapshot_users,
    snapshot_wallets,
)
from .models import (
    INCREMENTAL_RESOURCES,
    SNAPSHOT_RESOURCES,
    AuditLog,
    BaseIncrementalResource,
    BaseSnapshotResource,
    Coupon,
    Customer,
    CustomerCredit,
    CustomerSegment,
    EndpointConfig,
    Feature,
    Invoice,
    InvoicingEntity,
    PriceBook,
    PriceConfiguration,
    Product,
    PromotionCode,
    Quote,
    ResourceConfig,
    ResourceState,
    Subscription,
    SubscriptionTransition,
    TaxRate,
    Transaction,
    User,
    Wallet,
)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{base_url(config.credentials.access_token)}/v1/users"

    try:
        await http.request(log, url, params={"take": 1})
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided API key is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"
        raise ValidationError([msg])


@dataclass(frozen=True)
class IncrementalStreamFns:
    fetch_changes: Callable[..., AsyncGenerator[Any, None]]
    fetch_page: Callable[..., AsyncGenerator[Any, None]]


INCREMENTAL_WIRING: dict[type[BaseIncrementalResource], IncrementalStreamFns] = {
    Customer: IncrementalStreamFns(fetch_customers, backfill_customers),
    Invoice: IncrementalStreamFns(fetch_invoices, backfill_invoices),
    Subscription: IncrementalStreamFns(fetch_subscriptions, backfill_subscriptions),
    Quote: IncrementalStreamFns(fetch_quotes, backfill_quotes),
    CustomerCredit: IncrementalStreamFns(
        fetch_customer_credits, backfill_customer_credits
    ),
}

SNAPSHOT_WIRING: dict[
    type[BaseSnapshotResource], Callable[..., AsyncGenerator[Any, None]]
] = {
    Product: snapshot_products,
    PriceBook: snapshot_price_books,
    PriceConfiguration: snapshot_price_configurations,
    Feature: snapshot_features,
    Coupon: snapshot_coupons,
    PromotionCode: snapshot_promotion_codes,
    TaxRate: snapshot_tax_rates,
    InvoicingEntity: snapshot_invoicing_entities,
    User: snapshot_users,
    CustomerSegment: snapshot_customer_segments,
    Transaction: snapshot_transactions,
    Wallet: snapshot_wallets,
    SubscriptionTransition: snapshot_subscription_transitions,
}


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    base = base_url(config.credentials.access_token)
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    def open_incremental(
        fns: IncrementalStreamFns,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fns.fetch_changes, http, base),
            fetch_page=functools.partial(fns.fetch_page, http, base, config.start_date),
        )

    def open_audit_logs(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_audit_logs, http, base),
            fetch_page=functools.partial(
                backfill_audit_logs, http, base, config.start_date
            ),
        )

    def open_snapshot(
        snapshot_fn: Callable[..., AsyncGenerator[Any, None]],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_fn, http, base),
        )

    incremental_resources = [
        common.Resource(
            name=model.NAME,
            key=model.KEY,
            model=model,
            open=functools.partial(open_incremental, INCREMENTAL_WIRING[model]),
            initial_state=ResourceState(
                # The first incremental window `(cutoff - 1ms, horizon]` emits
                # tick `cutoff` exactly; backfill ends at `updated_at__lt=cutoff`.
                inc=ResourceState.Incremental(
                    cursor=cutoff - timedelta(milliseconds=1)
                ),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(
                name=model.NAME, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for model in INCREMENTAL_RESOURCES
    ]

    audit_logs_resource = common.Resource(
        name=AuditLog.NAME,
        key=AuditLog.KEY,
        model=AuditLog,
        open=open_audit_logs,
        initial_state=ResourceState(
            # Incremental emits `happened_at >= cursor`, backfill suppresses
            # `happened_at >= cutoff` — gapless and non-overlapping without
            # any -1-tick machinery.
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(
            name=AuditLog.NAME, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )

    snapshot_resources = [
        common.SnapshotResource(
            name=model.NAME,
            open=functools.partial(open_snapshot, SNAPSHOT_WIRING[model]),
            initial_config=ResourceConfig(
                name=model.NAME, interval=timedelta(minutes=5)
            ),
        )
        for model in SNAPSHOT_RESOURCES
    ]

    return [*incremental_resources, audit_logs_resource, *snapshot_resources]
