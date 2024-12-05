from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, LogCursor, PageCursor, open_binding, ResourceConfig, ResourceState
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource

from .api import (
    fetch_incremental,
    fetch_backfill,
    fetch_incremental_actions,
    fetch_backfill_actions,
    fetch_snapshot,
    fetch_incremental_child,
    fetch_backfill_child,
    fetch_snapshot_child,
)

from .models import (
    Actions,
    ActionInquiries,
    Ads,
    Catalogs,
    Contracts,
    Campaigns,
    Deals,
    ExceptionLists,
    TrackingValueRequests,
    Tasks,
    BlockRedirectRules,
    MediaPartnerGroups,
    Notes,
    Invoices,
    Jobs,
    PhoneNumbers,
    PromoCodes,
    Reports,
    UniqueUrls,
    EndpointConfig,
)

async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    if config.api_catalog == "Brand":
        # We pass along the credentials.username value since its required by every endpoint
        all_streams = (
            action_object(Campaigns, Actions, http, config.stop_date, config.credentials.username),
            action_object(Campaigns, ActionInquiries, http, config.stop_date, config.credentials.username),
            base_object(Invoices, http, config.stop_date, config.credentials.username),
            base_object(Jobs, http, config.stop_date, config.credentials.username),
            base_object(PromoCodes, http, config.stop_date, config.credentials.username),
            base_object(Deals, http, config.stop_date, config.credentials.username),
            base_object(ExceptionLists, http, config.stop_date, config.credentials.username),
            base_object(UniqueUrls, http, config.stop_date, config.credentials.username),
            base_object(TrackingValueRequests, http, config.stop_date, config.credentials.username),
            snapshot_object(Campaigns, http, config.stop_date, config.credentials.username),
            snapshot_object(Ads, http, config.stop_date, config.credentials.username),
            snapshot_object(Catalogs, http, config.stop_date, config.credentials.username),
            snapshot_object(Reports, http, config.stop_date, config.credentials.username),
            snapshot_object(PhoneNumbers, http, config.stop_date, config.credentials.username),
            child_object(Campaigns, Contracts, http, config.stop_date, config.credentials.username),
            child_object(Campaigns, Tasks, http, config.stop_date, config.credentials.username),
            child_object(Campaigns, Notes, http, config.stop_date, config.credentials.username),
            child_object(Campaigns, BlockRedirectRules, http, config.stop_date, config.credentials.username),
            media_groups_object(Campaigns, MediaPartnerGroups, http, config.stop_date, config.credentials.username),

        )

    elif config.api_catalog == "Agency":
        pass

    elif config.api_catalog == "Partners":
        pass

    return all_streams


def base_object(
    cls, http: HTTPSession, stop_date: datetime, account_sid
) -> Resource:
    """Base Object is the standard object for source-impact-native
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
            fetch_changes=functools.partial(fetch_incremental, cls, account_sid, http),
            fetch_page=functools.partial(fetch_backfill, cls, stop_date,account_sid, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=[cls.PRIMARY_KEY],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def action_object(
    cls_parent, cls, http: HTTPSession, stop_date: datetime, account_sid
) -> Resource:
    """Action Object is the standard method for Actions and ActionsInquires streams.
    Actions endpoints require a really specific set of parameters to work, and also requires
    the Campaigns CampaignId values.
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
            fetch_changes=functools.partial(fetch_incremental_actions, cls_parent, cls, account_sid, http),
            fetch_page=functools.partial(fetch_backfill_actions, cls_parent, cls, stop_date, account_sid, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=["/Id"],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )

def snapshot_object(
    cls, http: HTTPSession, stop_date: datetime, account_sid
) -> Resource:
    """Snapshot Object handles the stream cases were we dont have any valid replication keys
    to use. Some streams have valid Ids being returned by the API, others depend on flow's _meta/id
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
            fetch_snapshot=functools.partial(fetch_snapshot, cls, account_sid, http),
            tombstone=cls(_meta=cls.Meta(op="d")),
        )

    return Resource(
        name=cls.NAME,
        key=[cls.PRIMARY_KEY],
        model=cls,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )

def child_object(
    cls_parent, cls, http: HTTPSession, stop_date: datetime, account_sid
) -> Resource:
    """Child Object handles the default parent/child streams. As of now, the only possible parent stream to exist is the
    Campaigns stream.
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
            fetch_changes=functools.partial(fetch_incremental_child, cls_parent, cls, account_sid, http),
            fetch_page=functools.partial(fetch_backfill_child, cls_parent, cls, stop_date, account_sid, http),
        )

    started_at = datetime.now(tz=UTC)

    return Resource(
        name=cls.NAME,
        key=[cls.PRIMARY_KEY],
        model=cls,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at)
        ),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )

def media_groups_object(
    cls_parent, cls, http: HTTPSession, stop_date: datetime, account_sid
) -> Resource:
    """Media Groups Object is a snapshot object.
    It exists because the MediaGroupsPartners does not have a valid primary key,
    but it requires the Campaigns Ids from the Campaign stream
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
            fetch_snapshot=functools.partial(fetch_snapshot_child, cls_parent, cls, account_sid, http),
            tombstone=cls(_meta=cls.Meta(op="d")),
        )

    return Resource(
        name=cls.NAME,
        key=[cls.PRIMARY_KEY],
        model=cls,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )