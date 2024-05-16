from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding, ResourceConfig, ResourceState
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .api import (fetch_incremental,
                  fetch_backfill,
                  fetch_incremental_substreams,
                  fetch_backfill_substreams,
                  fetch_incremental_no_events,
                  fetch_backfill_substreams2,
                  fetch_incremental_substreams2
                  )

from .models import (
    Accounts,
    Authorizations,
    Bank_Accounts,
    BalanceTransactions,
    Cards,
    CardHolders,
    Coupons,
    Customers,
    CustomerBalanceTransaction,
    Charges,
    CheckoutSessions,
    CheckoutSessionsLine,
    CreditNotes,
    CreditNotesLines,
    Disputes,
    EarlyFraudWarning,
    Persons,
    Invoices,
    InvoiceItems,
    InvoiceLineItems,
    ExternalAccountCards,
    ExternalBankAccount,
    ApplicationFees,
    ApplicationFeesRefunds,
    Bank_Accounts,
    PaymentMethods,
    PaymentIntent,
    Payouts,
    Plans,
    Products,
    PromotionCode,
    Refunds,
    Reviews,
    SetupAttempts,
    SetupIntents,
    Subscriptions,
    SubscriptionsSchedule,
    SubscriptionItems,
    TopUps,
    Transactions,
    Transfers,
    TransferReversals,
    UsageRecords,
    Files,
    FilesLink,
    EndpointConfig,
    )
#TODO add config.stop_date after validating tests
async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    return [
        base_object(Accounts, http),
        base_object(ApplicationFees, http),
        base_object(Customers, http),
        base_object(Charges, http),
        base_object(CheckoutSessions, http),
        base_object(Coupons, http),
        base_object(CreditNotes, http),
        base_object(Disputes, http),
        base_object(EarlyFraudWarning, http),
        base_object(InvoiceItems, http),
        base_object(Invoices, http),
        base_object(PaymentIntent, http),
        base_object(Payouts, http),
        base_object(Plans, http),
        base_object(Products, http),
        base_object(PromotionCode, http),
        base_object(Refunds, http),
        base_object(Reviews, http),
        base_object(SetupIntents, http),
        base_object(Subscriptions, http),
        base_object(SubscriptionsSchedule, http),
        base_object(TopUps, http),
        base_object(Transfers, http),
        no_events_object(Files, http),
        no_events_object(FilesLink, http),
        no_events_object(BalanceTransactions, http),
        # issuing_object(Authorizations, http),
        # issuing_object(CardHolders, http),
        # issuing_object(Transactions, http),
        child_object(Accounts, Persons, http),
        child_object(Accounts, ExternalAccountCards, http),
        child_object(Accounts, ExternalBankAccount, http),
        child_object(ApplicationFees, ApplicationFeesRefunds, http),
        child_object(Customers, Cards, http),
        child_object(Customers, Bank_Accounts, http),
        child_object(Customers, PaymentMethods, http),
        child_object(Customers, CustomerBalanceTransaction, http),
        child_object(CheckoutSessions, CheckoutSessionsLine, http),
        child_object(CreditNotes, CreditNotesLines, http),
        child_object(Invoices, InvoiceLineItems, http),
        child_object(Transfers, TransferReversals, http),
        child_object(Subscriptions, SubscriptionItems, http),
        child_object2(Subscriptions, SubscriptionItems,UsageRecords, http),
        child_object(SetupIntents, SetupAttempts, http),



    ]




def base_object(
    cls, http: HTTPSession, stop_date =datetime.fromisoformat("2024-04-10T00:00:00Z".replace('Z', '+00:00'))
) -> Resource:
    """Base Object handles the default case from source-stripe-native
    It requires a single, parent stream with a valid Event API Type
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental, cls, http),
            fetch_page=functools.partial(fetch_backfill, cls, stop_date, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )

def child_object(
    cls, child_cls, http: HTTPSession, stop_date =datetime.fromisoformat("2024-04-10T00:00:00Z".replace('Z', '+00:00'))
) -> Resource:
    """Child Object handles the default child case from source-stripe-native
    It requires both the parent and child stream, with the parent stream having
    a valid Event API Type
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental_substreams, cls, child_cls, http),
            fetch_page=functools.partial(fetch_backfill_substreams, cls, child_cls, stop_date, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=child_cls.NAME,
        key=["/id"],
        model=child_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=child_cls.NAME),
        schema_inference=True,
    )

def child_object2(
    cls, child_cls, child_cls2, http: HTTPSession, stop_date =datetime.fromisoformat("2024-01-01T00:00:00Z".replace('Z', '+00:00'))
) -> Resource:
    """Child Object handles the default child case from source-stripe-native
    It requires both the parent and child stream, with the parent stream having
    a valid Event API Type
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental_substreams2, cls, child_cls, child_cls2, http),
            fetch_page=functools.partial(fetch_backfill_substreams2, cls, child_cls, child_cls2, stop_date, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=child_cls2.NAME,
        key=["/id"],
        model=child_cls2,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=child_cls2.NAME),
        schema_inference=True,
    )

def no_events_object(
    cls, http: HTTPSession, stop_date =datetime.fromisoformat("2024-04-10T00:00:00Z".replace('Z', '+00:00'))
) -> Resource:
    """No Events Object handles a edge-case from source-stripe-native,
    where the given parent stream does not contain a valid Events API type.
    It requires a single, parent stream with a valid list all API endpoint.
    It works very similar to the base object, but without the use of the Events APi.
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental_no_events, cls, http),
            fetch_page=functools.partial(fetch_backfill, cls, stop_date, http),
        )

    started_at = datetime.now(tz=UTC) - timedelta(days=1)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )

def issuing_object(
        cls, http: HTTPSession, stop_date =datetime.fromisoformat("2024-04-10T00:00:00Z".replace('Z', '+00:00'))
) -> Resource:
    #TODO add validation check to issuing streams

    """ Issuing Object works similar to Base Objects, but only handles "issuing" endpoint
    streams.
    """
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_incremental, cls, http),
            fetch_page=functools.partial(fetch_backfill, cls, stop_date, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME),
        schema_inference=True,
    )
