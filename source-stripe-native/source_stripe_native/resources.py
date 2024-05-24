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
                  fetch_backfill_usage_records,
                  fetch_incremental_usage_records,
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
    EventResult,
    SubscriptionItems,
    )
async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)


    all_streams = [
        base_object(Accounts, http, config.stop_date),
        base_object(ApplicationFees, http, config.stop_date),
        base_object(Customers, http, config.stop_date),
        base_object(Charges, http, config.stop_date),
        base_object(CheckoutSessions, http, config.stop_date),
        base_object(Coupons, http, config.stop_date),
        base_object(CreditNotes, http, config.stop_date),
        base_object(Disputes, http, config.stop_date),
        base_object(EarlyFraudWarning, http, config.stop_date),
        base_object(InvoiceItems, http, config.stop_date),
        base_object(Invoices, http, config.stop_date),
        base_object(PaymentIntent, http, config.stop_date),
        base_object(Payouts, http, config.stop_date),
        base_object(Plans, http, config.stop_date),
        base_object(Products, http, config.stop_date),
        base_object(PromotionCode, http, config.stop_date),
        base_object(Refunds, http, config.stop_date),
        base_object(Reviews, http, config.stop_date),
        base_object(SetupIntents, http, config.stop_date),
        base_object(Subscriptions, http, config.stop_date),
        base_object(SubscriptionsSchedule, http, config.stop_date),
        base_object(TopUps, http, config.stop_date),
        base_object(Transfers, http, config.stop_date),
        base_object(SubscriptionItems, http, config.stop_date),
        no_events_object(Files, http, config.stop_date),
        no_events_object(FilesLink, http, config.stop_date),
        no_events_object(BalanceTransactions, http, config.stop_date),
        child_object(Accounts, Persons, http, config.stop_date),
        child_object(Accounts, ExternalAccountCards, http, config.stop_date),
        child_object(Accounts, ExternalBankAccount, http, config.stop_date),
        child_object(ApplicationFees, ApplicationFeesRefunds, http, config.stop_date),
        child_object(Customers, Cards, http, config.stop_date),
        child_object(Customers, Bank_Accounts, http, config.stop_date),
        child_object(Customers, PaymentMethods, http, config.stop_date),
        child_object(Customers, CustomerBalanceTransaction, http, config.stop_date),
        child_object(CheckoutSessions, CheckoutSessionsLine, http, config.stop_date),
        child_object(CreditNotes, CreditNotesLines, http, config.stop_date),
        child_object(Invoices, InvoiceLineItems, http, config.stop_date),
        child_object(Transfers, TransferReversals, http, config.stop_date),
        child_object(SetupIntents, SetupAttempts, http, config.stop_date),
        usage_records(SubscriptionItems, UsageRecords, http, config.stop_date),


    ]

    # Checking if user has "Issuing" permissions
    try:
        #Using Authorizations stream for testing
        url = f"https://api.stripe.com/v1/{Authorizations.SEARCH_NAME}"
        result = EventResult.model_validate_json(
                await http.request(log, url, method="GET")
            )
        issuing_list = [
            issuing_object(Authorizations, http, config.stop_date),
            issuing_object(CardHolders, http, config.stop_date),
            issuing_object(Transactions, http, config.stop_date),
        ]
        all_streams += issuing_list
    except:
        # User does not have permission
        log.info("Permission Denied for Issuing Endpoints. "
                 "Removing 'Issuing' streams")

    return all_streams






def base_object(
    cls, http: HTTPSession, stop_date: datetime
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
    cls, child_cls, http: HTTPSession, stop_date: datetime
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

def usage_records(
    cls, child_cls, http: HTTPSession, stop_date: datetime
) -> Resource:
    """ Usage Records handles a specific stream (UsageRecords).
    This is required since Usage Records is a child stream from a child stream.
    It requires the parent stream, child stream and second child stream.
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
            fetch_changes=functools.partial(fetch_incremental_usage_records, cls, child_cls, http),
            fetch_page=functools.partial(fetch_backfill_usage_records, cls, child_cls, stop_date, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=child_cls.NAME,
        key=["/subscription_item"],
        model=child_cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=child_cls.NAME),
        schema_inference=True,
    )

def no_events_object(
    cls, http: HTTPSession, stop_date: datetime
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
        cls, http: HTTPSession, stop_date: datetime
) -> Resource:

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
