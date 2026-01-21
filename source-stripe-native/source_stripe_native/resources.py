from copy import deepcopy
from datetime import datetime, UTC, timedelta
from logging import Logger
from typing import cast
import functools
import re
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    FetchPageFn,
    Resource,
    open_binding,
    ResourceConfig,
    ResourceState,
)

from .priority_capture import (
    open_binding_with_priority_queue,
)
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource, HTTPError

from .api import (
    API,
    fetch_incremental,
    fetch_backfill,
    fetch_backfill_prices,
    fetch_incremental_substreams,
    fetch_backfill_substreams,
    fetch_incremental_no_events,
    fetch_backfill_usage_records,
    fetch_incremental_usage_records,
)

from .models import (
    Accounts,
    ConnectorState,
    EndpointConfig,
    ListResult,
    Prices,
    STREAMS,
    REGIONAL_STREAMS,
    SPLIT_CHILD_STREAM_NAMES,
)

DISABLED_MESSAGE_REGEX = r"Your account is not set up to use"

# Backfill suffixes for Prices stream - must separately backfill active and inactive prices
# due to Stripe API limitation (no way to query both in one request)
PRICES_BACKFILL_SUFFIXES = ["_active", "_inactive"]


async def check_accessibility(
    http: HTTPMixin,
    log: Logger,
    url: str,
) -> bool:
    """
    Returns a boolean representing whether the provided stream is accessible (True if accessible, False if inaccessible).

    If a stream is inaccessible due to API key permissions, the Stripe API response has a status code of:
    - 401 (live Stripe accounts)
    - 403 (test Stripe accounts)

    There are also some streams (like Authorizations, one of the Issuing streams) that can be enabled/disabled separately from API key permissions.
    These streams return a 400 status code & message that contains "Your account is not set up to use X."
    """
    is_accessible = True

    try:
        await http.request(log, url)
    except HTTPError as err:
        is_permission_blocked = err.code == 401 or err.code == 403
        is_disabled = err.code == 400 and bool(
            re.search(DISABLED_MESSAGE_REGEX, err.message)
        )
        is_accessible = not is_permission_blocked and not is_disabled

    return is_accessible


async def _fetch_connected_account_ids(
    http: HTTPSession,
    log: Logger,
) -> list[str]:
    account_ids: set[str] = set()

    url = f"{API}/accounts"
    params: dict[str, str | int] = {"limit": 100}

    while True:
        response = ListResult[Accounts].model_validate_json(
            await http.request(log, url, params=params)
        )

        for account in response.data:
            account_ids.add(account.id)

        if not response.has_more:
            break

        params["starting_after"] = response.data[-1].id

    return list(account_ids)


async def _fetch_platform_account_id(
    http: HTTPSession,
    log: Logger,
) -> str:
    platform_account = Accounts.model_validate_json(
        await http.request(log, f"{API}/account")
    )
    return platform_account.id


async def _reconcile_connector_state(
    account_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
):
    if (
        isinstance(state.inc, dict)
        and isinstance(state.backfill, dict)
        and isinstance(initial_state.inc, dict)
        and isinstance(initial_state.backfill, dict)
    ):
        should_checkpoint = False

        for account_id in account_ids:
            inc_state_exists = account_id in state.inc
            backfill_state_exists = account_id in state.backfill

            if not inc_state_exists and not backfill_state_exists:
                task.log.info(
                    f"Initializing new subtask state for account id {account_id}."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                state.backfill[account_id] = deepcopy(initial_state.backfill[account_id])
                should_checkpoint = True
            elif not inc_state_exists and backfill_state_exists:
                # Note: This case is to fix a legacy issue where the incremental state was not initialized
                # due to the connector restarting with the backfill for this subtask checkpointing some progress.
                # This is a temporary condition to ensure that we backfill these offending subtasks and reconcile their state.
                task.log.info(
                    f"Backfilling subtask for account id {account_id} due to missing incremental state."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                state.backfill[account_id] = deepcopy(initial_state.backfill[account_id])
                should_checkpoint = True

        if should_checkpoint:
            task.log.info(
                f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}."
            )
            await task.checkpoint(
                ConnectorState(
                    bindingStateV1={binding.stateKey: state},
                )
            )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig, should_fetch_connected_accounts: bool = True,
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    is_restricted_api_key = config.credentials.access_token.startswith("rk_")
    all_streams: list[Resource] = []

    # If a restricted API key was provided, we have to ensure the /events endpoint is accessible.
    # Almost all streams use this endpoint for incremental replication, so we must be able to access it.
    if is_restricted_api_key:
        events_url = f"{API}/events"
        if not await check_accessibility(http, log, events_url):
            raise RuntimeError(
                "/events endpoint is not accessible, preventing incremental replication.\nPlease update your restricted API key to have read permissions for Events."
            )

    platform_account_id = await _fetch_platform_account_id(http, log)
    connected_account_ids = []
    if config.capture_connected_accounts and should_fetch_connected_accounts:
        log.info("Fetching connected account IDs. This may take multiple minutes if there are many connected accounts.")
        connected_account_ids = await _fetch_connected_account_ids(http, log)
        log.info(
            f"Found {len(connected_account_ids)} connected account IDs.", {
                "connected_account_ids": connected_account_ids,
            }
        )

    all_account_ids = [platform_account_id, *connected_account_ids]
    initial_state = _create_initial_state(
        all_account_ids if connected_account_ids else platform_account_id
    )

    for element in STREAMS:
        is_accessible_stream = True
        base_stream = element.get("stream")

        # If we have a restricted API key, we have to check if we can access each stream.
        if is_restricted_api_key:
            url = f"{API}/{base_stream.SEARCH_NAME}"
            is_accessible_stream = await check_accessibility(http, log, url)

        if is_accessible_stream:
            # Prices requires special handling due to multi-backfill requirements
            if base_stream == Prices:
                resource = prices_object(
                    http,
                    config.start_date,
                    platform_account_id,
                    connected_account_ids,
                    all_account_ids,
                )
            # If the stream class does not have an EVENT_TYPES attribute, then it does not have any associated events.
            elif hasattr(base_stream, "EVENT_TYPES"):
                resource = base_object(
                    base_stream,
                    http,
                    config.start_date,
                    platform_account_id,
                    connected_account_ids,
                    all_account_ids,
                    initial_state,
                )
            else:
                resource = no_events_object(
                    base_stream,
                    http,
                    config.start_date,
                    platform_account_id,
                    connected_account_ids,
                    all_account_ids,
                    initial_state,
                )
            all_streams.append(resource)

            children = element.get("children", [])
            for child in children:
                is_accessible_child = True
                child_stream = child.get("stream")
                # If the child stream has a discoverPath specified, then it's accessibility can be different from its parent.
                if child.get("discoverPath", None):
                    url = f"{API}/{child.get('discoverPath')}"
                    is_accessible_child = await check_accessibility(http, log, url)

                if is_accessible_child:
                    match child_stream.NAME:
                        case "UsageRecords":
                            all_streams.append(
                                usage_records(
                                    base_stream,
                                    child_stream,
                                    http,
                                    config.start_date,
                                    platform_account_id,
                                    connected_account_ids,
                                    all_account_ids,
                                    initial_state,
                                )
                            )
                        case _ if child_stream.NAME in SPLIT_CHILD_STREAM_NAMES:
                            all_streams.append(
                                split_child_object(
                                    base_stream,
                                    child_stream,
                                    http,
                                    config.start_date,
                                    platform_account_id,
                                    connected_account_ids,
                                    all_account_ids,
                                    initial_state,
                                )
                            )
                        case _:
                            all_streams.append(
                                child_object(
                                    base_stream,
                                    child_stream,
                                    http,
                                    config.start_date,
                                    platform_account_id,
                                    connected_account_ids,
                                    all_account_ids,
                                    initial_state,
                                )
                            )

    # Regional streams are only available in certain countries, so we always have
    # check if they accessible regardless of the API key provided.
    for stream in REGIONAL_STREAMS:
        url = f"{API}/{stream.SEARCH_NAME}"
        if await check_accessibility(http, log, url):
            resource = base_object(
                stream,
                http,
                config.start_date,
                platform_account_id,
                connected_account_ids,
                all_account_ids,
                initial_state,
            )
            all_streams.append(resource)

    return all_streams


def _create_initial_state(account_ids: str | list[str]) -> ResourceState:
    cutoff = datetime.now(tz=UTC)

    if isinstance(account_ids, str):
        initial_state = ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        )
    else:
        initial_state = ResourceState(
            inc={
                account_id: ResourceState.Incremental(cursor=cutoff)
                for account_id in account_ids
            },
            backfill={
                account_id: ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for account_id in account_ids
            },
        )

    return initial_state


def _create_prices_initial_state(account_ids: str | list[str]) -> ResourceState:
    """
    Creates initial state for Prices stream with separate backfill subtasks for active/inactive.

    Single account mode:
    - Incremental: Incremental (standard, not a dict)
    - Backfill: {"_active": Backfill, "_inactive": Backfill} (subtask dict)

    Connected accounts mode:
    - Incremental: {account_id: Incremental} (dict keyed by account)
    - Backfill: {account_id_active: Backfill, account_id_inactive: Backfill} (composite keys)
    """
    cutoff = datetime.now(tz=UTC)

    if isinstance(account_ids, str):
        # Single account mode - use subtask dict for backfill (keyed by suffix only)
        initial_state = ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill={
                suffix: ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for suffix in PRICES_BACKFILL_SUFFIXES
            },
        )
    else:
        # Connected accounts mode - use composite keys for backfill
        initial_state = ResourceState(
            inc={
                account_id: ResourceState.Incremental(cursor=cutoff)
                for account_id in account_ids
            },
            backfill={
                f"{account_id}{suffix}": ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for account_id in account_ids
                for suffix in PRICES_BACKFILL_SUFFIXES
            },
        )

    return initial_state


async def _reconcile_prices_state(
    account_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
):
    """
    Reconciles state for newly discovered connected accounts for Prices stream.

    Similar to _reconcile_connector_state but handles composite backfill keys.
    """
    if (
        isinstance(state.inc, dict)
        and isinstance(state.backfill, dict)
        and isinstance(initial_state.inc, dict)
        and isinstance(initial_state.backfill, dict)
    ):
        should_checkpoint = False

        for account_id in account_ids:
            inc_state_exists = account_id in state.inc
            # Check if ANY backfill suffix exists for this account
            backfill_state_exists = any(
                f"{account_id}{suffix}" in state.backfill
                for suffix in PRICES_BACKFILL_SUFFIXES
            )

            if not inc_state_exists and not backfill_state_exists:
                task.log.info(
                    f"Initializing new subtask state for account id {account_id}."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                for suffix in PRICES_BACKFILL_SUFFIXES:
                    state_key = f"{account_id}{suffix}"
                    state.backfill[state_key] = deepcopy(initial_state.backfill[state_key])
                should_checkpoint = True
            elif not inc_state_exists and backfill_state_exists:
                task.log.info(
                    f"Backfilling subtask for account id {account_id} due to missing incremental state."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                for suffix in PRICES_BACKFILL_SUFFIXES:
                    state_key = f"{account_id}{suffix}"
                    state.backfill[state_key] = deepcopy(initial_state.backfill[state_key])
                should_checkpoint = True

        if should_checkpoint:
            task.log.info(
                f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}."
            )
            await task.checkpoint(
                ConnectorState(
                    bindingStateV1={binding.stateKey: state},
                )
            )


def base_object(
    cls,
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Base Object handles the default case from source-stripe-native
    It requires a single, parent stream with a valid Event API Type
    """

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            fetch_changes_fns = functools.partial(
                fetch_incremental,
                cls,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns = functools.partial(
                fetch_backfill,
                cls,
                start_date,
                platform_account_id,
                None,
                http,
            )
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fns,
                fetch_page=fetch_page_fns,
            )
        else:
            await _reconcile_connector_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental,
                    cls,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory(account_id: str):
                return functools.partial(
                    fetch_backfill,
                    cls,
                    start_date,
                    platform_account_id,
                    account_id,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory,
            )

    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def child_object(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Child Object handles the default child case from source-stripe-native
    It requires both the parent and child stream, with the parent stream having
    a valid Event API Type
    """

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            fetch_changes_fns = functools.partial(
                fetch_incremental_substreams,
                cls,
                child_cls,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns = functools.partial(
                fetch_backfill_substreams,
                cls,
                child_cls,
                start_date,
                platform_account_id,
                None,
                http,
            )
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fns,
                fetch_page=fetch_page_fns,
            )
        else:
            await _reconcile_connector_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental_substreams,
                    cls,
                    child_cls,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory(account_id: str):
                return functools.partial(
                    fetch_backfill_substreams,
                    cls,
                    child_cls,
                    start_date,
                    platform_account_id,
                    account_id,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory,
            )


    return Resource(
        name=child_cls.NAME,
        key=["/id"],
        model=child_cls,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(
            name=child_cls.NAME, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def split_child_object(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """
    split_child_object handles the case where a stream is a child stream when backfilling
    but incrementally replicates based off events that contain the child stream resource directly
    in the API response. Meaning, the stream behaves like a non-chid stream incrementally.
    """

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            fetch_changes_fns = functools.partial(
                fetch_incremental,
                child_cls,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns = functools.partial(
                fetch_backfill_substreams,
                cls,
                child_cls,
                start_date,
                platform_account_id,
                None,
                http,
            )
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fns,
                fetch_page=fetch_page_fns,
            )
        else:
            await _reconcile_connector_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental,
                    child_cls,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory(account_id: str):
                return functools.partial(
                    fetch_backfill_substreams,
                    cls,
                    child_cls,
                    start_date,
                    platform_account_id,
                    account_id,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory,
            )


    return Resource(
        name=child_cls.NAME,
        key=["/id"],
        model=child_cls,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(
            name=child_cls.NAME, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def usage_records(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Usage Records handles a specific stream (UsageRecords).
    This is required since Usage Records is a child stream from SubscriptionItem
    and requires special processing.
    """

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            fetch_changes_fns = functools.partial(
                fetch_incremental_usage_records,
                cls,
                child_cls,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns = functools.partial(
                fetch_backfill_usage_records,
                cls,
                child_cls,
                start_date,
                platform_account_id,
                None,
                http,
            )
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fns,
                fetch_page=fetch_page_fns,
            )
        else:
            await _reconcile_connector_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental_usage_records,
                    cls,
                    child_cls,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory(account_id: str):
                return functools.partial(
                    fetch_backfill_usage_records,
                    cls,
                    child_cls,
                    start_date,
                    platform_account_id,
                    account_id,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory,
            )

    return Resource(
        name=child_cls.NAME,
        key=["/subscription_item"],  # Note: This is different from other resources
        model=child_cls,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(
            name=child_cls.NAME, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def no_events_object(
    cls,
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """No Events Object handles a edge-case from source-stripe-native,
    where the given parent stream does not contain a valid Events API type.
    It requires a single, parent stream with a valid list all API endpoint.
    It works very similar to the base object, but without the use of the Events APi.
    """

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            fetch_changes_fns = functools.partial(
                fetch_incremental_no_events,
                cls,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns = functools.partial(
                fetch_backfill,
                cls,
                start_date,
                platform_account_id,
                None,
                http,
            )
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fns,
                fetch_page=fetch_page_fns,
            )
        else:
            await _reconcile_connector_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental_no_events,
                    cls,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory(account_id: str):
                return functools.partial(
                    fetch_backfill,
                    cls,
                    start_date,
                    platform_account_id,
                    account_id,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory,
            )


    return Resource(
        name=cls.NAME,
        key=["/id"],
        model=cls,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name=cls.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def prices_object(
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
) -> Resource:
    """
    Prices stream with multi-backfill support for active and inactive prices.

    Due to Stripe API limitations, the /v1/prices endpoint requires the `active` parameter
    to filter by status - there's no way to fetch both active and inactive prices in a
    single request. This factory creates a Resource that uses:
    - 1 incremental task per account (events contain both active and inactive prices)
    - 2 backfill tasks per account (one for active, one for inactive prices)
    """
    initial_state = _create_prices_initial_state(
        all_account_ids if connected_account_ids else platform_account_id
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids:
            # Single account mode - use standard open_binding with subtask dict for fetch_page
            fetch_changes_fn = functools.partial(
                fetch_incremental,
                Prices,
                platform_account_id,
                None,
                http,
            )
            fetch_page_fns: dict[str, FetchPageFn[Prices]] = {
                suffix: cast(
                    FetchPageFn[Prices],
                    functools.partial(
                        fetch_backfill_prices,
                        Prices,
                        start_date,
                        platform_account_id,
                        None,
                        suffix == "_active",  # active=True for "_active", False for "_inactive"
                        http,
                    ),
                )
                for suffix in PRICES_BACKFILL_SUFFIXES
            }
            open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_changes=fetch_changes_fn,
                fetch_page=fetch_page_fns,
            )
        else:
            # Connected accounts mode - use priority queue with multi-backfill support
            await _reconcile_prices_state(
                all_account_ids, binding, state, initial_state, task
            )

            def fetch_changes_factory(account_id: str):
                return functools.partial(
                    fetch_incremental,
                    Prices,
                    platform_account_id,
                    account_id,
                    http,
                )

            def fetch_page_factory_with_suffix(account_id: str, suffix: str):
                active = suffix == "_active"
                return functools.partial(
                    fetch_backfill_prices,
                    Prices,
                    start_date,
                    platform_account_id,
                    account_id,
                    active,
                    http,
                )

            open_binding_with_priority_queue(
                binding,
                binding_index,
                state,
                task,
                all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory_with_suffix,
                backfill_subtask_suffixes=PRICES_BACKFILL_SUFFIXES,
            )

    return Resource(
        name=Prices.NAME,
        key=["/id"],
        model=Prices,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name=Prices.NAME, interval=timedelta(minutes=5)),
        schema_inference=True,
    )