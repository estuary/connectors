from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding, ResourceConfig, ResourceState
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .api import fetch_incremental, fetch_backfill, fetch_incremental_substreams, fetch_backfill_substreams

from .models import (
    Accounts,
    Authorizations,
    Bank_Accounts,
    CardHolders,
    Customers,
    Cards,
    Persons,
    ExternalAccountCards,
    ExternalBankAccount,
    ApplicationFees,
    ApplicationFeesRefunds,
    Bank_Accounts,
    EndpointConfig,
    )

async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    return [
        base_object(Accounts, http),
        base_object(ApplicationFees, http),
        base_object(Customers, http),
        base_object(Authorizations, http),
        base_object(CardHolders, http),
        child_object(Accounts, Persons, http),
        #child_object(Accounts, ExternalAccountCards, http),
        #child_object(Accounts, ExternalBankAccount, http),
        child_object(ApplicationFees, ApplicationFeesRefunds, http),
        child_object(Customers, Cards, http),
        child_object(Customers, Bank_Accounts, http),
    ]




def base_object(
    cls, http: HTTPSession,
) -> Resource:

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
            fetch_page=functools.partial(fetch_backfill, cls, http),
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
    cls, child_cls, http: HTTPSession,
) -> Resource:

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
            fetch_page=functools.partial(fetch_backfill_substreams, cls, child_cls, http),
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
