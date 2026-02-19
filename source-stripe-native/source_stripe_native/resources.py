import functools
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    ConnectorState,
    Resource,
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding

# Default backfill schedule for exempt streams - runs daily at midnight UTC
DEFAULT_SCHEDULE = "0 0 * * *"

from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    API,
    fetch_backfill,
    fetch_backfill_substreams,
    fetch_backfill_usage_records,
    fetch_incremental,
    fetch_incremental_no_events,
    fetch_incremental_substreams,
    fetch_incremental_usage_records,
)
from .models import (
    CONNECTED_ACCOUNT_EXEMPT_STREAMS,
    REGIONAL_STREAMS,
    SCHEDULED_BACKFILL_STREAMS,
    SPLIT_CHILD_STREAM_NAMES,
    STREAMS,
    Accounts,
    ConnectorState,
    EndpointConfig,
    ListResult,
)
from .priority_capture import (
    open_binding_with_priority_queue,
)

DISABLED_MESSAGE_REGEX = r"Your account is not set up to use"


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
                state.backfill[account_id] = deepcopy(
                    initial_state.backfill[account_id]
                )
                should_checkpoint = True
            elif not inc_state_exists and backfill_state_exists:
                # Note: This case is to fix a legacy issue where the incremental state was not initialized
                # due to the connector restarting with the backfill for this subtask checkpointing some progress.
                # This is a temporary condition to ensure that we backfill these offending subtasks and reconcile their state.
                task.log.info(
                    f"Backfilling subtask for account id {account_id} due to missing incremental state."
                )
                state.inc[account_id] = deepcopy(initial_state.inc[account_id])
                state.backfill[account_id] = deepcopy(
                    initial_state.backfill[account_id]
                )
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
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    should_fetch_connected_accounts: bool = True,
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
        log.info(
            "Fetching connected account IDs. This may take multiple minutes if there are many connected accounts."
        )
        connected_account_ids = await _fetch_connected_account_ids(http, log)
        log.info(
            f"Found {len(connected_account_ids)} connected account IDs.",
            {
                "connected_account_ids": connected_account_ids,
            },
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
            # If the stream class does not have an EVENT_TYPES attribute, then it does not have any associated events.
            resource = (
                base_object(
                    base_stream,
                    http,
                    config.start_date,
                    config.advanced.incremental_window_size,
                    platform_account_id,
                    connected_account_ids,
                    all_account_ids,
                    initial_state,
                )
                if hasattr(base_stream, "EVENT_TYPES")
                else no_events_object(
                    base_stream,
                    http,
                    config.start_date,
                    platform_account_id,
                    connected_account_ids,
                    all_account_ids,
                    initial_state,
                )
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
                                    config.advanced.incremental_window_size,
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
                                    config.advanced.incremental_window_size,
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
                                    config.advanced.incremental_window_size,
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
                config.advanced.incremental_window_size,
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


def _create_initial_state_for_exempt_stream() -> ResourceState:
    """
    Create initial state for subtask-exempt streams.
    These streams use non-dict state (single cursor) since they always query
    the platform account, regardless of connected accounts.
    """
    cutoff = datetime.now(tz=UTC)
    return ResourceState(
        inc=ResourceState.Incremental(cursor=cutoff),
        backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
    )


def build_subtask_to_flat_migration_patch(
    old_keys: set[str],
    cutoff: datetime,
) -> dict:
    """
    Build an RFC 7396 merge patch for migrating from subtask-based (dict) state
    to flat (single-cursor) state.

    The returned dict contains:
    - Deletion markers (None) for all old account keys
    - New flat state fields (cursor, cutoff, next_page)

    This hybrid structure is valid as a merge patch but won't pass Pydantic validation,
    so it must be used with model_construct().
    """
    deletion_markers = dict.fromkeys(old_keys, None)

    return {
        "inc": {**deletion_markers, "cursor": cutoff},
        "backfill": {**deletion_markers, "cutoff": cutoff, "next_page": None},
    }


async def _maybe_migrate_exempt_state(
    stream_name: str,
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    platform_account_id: str,
    task: Task,
) -> ResourceState:
    """
    Check if an exempt stream needs state migration and perform it if necessary.

    Migrates from subtask-based state (dict) to single-cursor state using
    model_construct to build an RFC 7396 merge patch for atomic, crash-safe migration.
    """
    if stream_name not in CONNECTED_ACCOUNT_EXEMPT_STREAMS or not isinstance(
        state.inc, dict
    ):
        return state

    task.log.info(
        f"Migrating {stream_name} from dict state to non-dict state. "
        "This will trigger a re-backfill."
    )

    # Determine the cursor to preserve (platform account, or earliest fallback)
    inc_state = state.inc.get(platform_account_id)
    if inc_state is None and state.inc:
        valid_cursors = [v for v in state.inc.values() if v is not None]
        if valid_cursors:
            inc_state = min(valid_cursors, key=lambda x: x.cursor)

    cutoff = inc_state.cursor if inc_state else datetime.now(tz=UTC)
    assert isinstance(cutoff, datetime)

    # Collect all old keys to delete from inc and backfill dicts
    old_keys = set(state.inc.keys())
    if isinstance(state.backfill, dict):
        old_keys.update(state.backfill.keys())

    migration_patch = build_subtask_to_flat_migration_patch(old_keys, cutoff)
    # Use model_construct to bypass Pydantic validation since the hybrid structure
    # (deletion markers + new values) wouldn't pass normal validation
    migration_state = ResourceState.model_construct(**migration_patch)
    await task.checkpoint(
        ConnectorState(bindingStateV1={binding.stateKey: migration_state})
    )

    # Return clean single-cursor state for the connector to use
    return ResourceState(
        inc=ResourceState.Incremental(cursor=cutoff),
        backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        last_initialized=state.last_initialized,
    )


def base_object(
    cls,
    http: HTTPSession,
    start_date: datetime,
    incremental_window_size: timedelta,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Base Object handles the default case from source-stripe-native
    It requires a single, parent stream with a valid Event API Type
    """
    is_exempt = cls.NAME in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    needs_schedule = cls.NAME in SCHEDULED_BACKFILL_STREAMS

    # Exempt streams should use non-dict state since they always query
    # the platform account regardless of connected accounts
    effective_initial_state = (
        _create_initial_state_for_exempt_stream()
        if is_exempt and connected_account_ids
        else initial_state
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Exempt streams should not use per-account subtasks even when
        # connected accounts exist. They always query the platform account.
        if not connected_account_ids or is_exempt:
            # Migrate state if needed (for existing captures with dict state).
            state = await _maybe_migrate_exempt_state(
                cls.NAME, binding, state, platform_account_id, task
            )

            fetch_changes_fns = functools.partial(
                fetch_incremental,
                cls,
                platform_account_id,
                None,
                http,
                incremental_window_size,
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
                    incremental_window_size,
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
        initial_state=effective_initial_state,
        initial_config=ResourceConfigWithSchedule(
            name=cls.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE if needs_schedule else "",
        ),
        schema_inference=True,
    )


def child_object(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    incremental_window_size: timedelta,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Child Object handles the default child case from source-stripe-native
    It requires both the parent and child stream, with the parent stream having
    a valid Event API Type
    """
    is_exempt = child_cls.NAME in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    needs_schedule = child_cls.NAME in SCHEDULED_BACKFILL_STREAMS

    # Exempt streams should use non-dict state since they always query
    # the platform account regardless of connected accounts
    effective_initial_state = (
        _create_initial_state_for_exempt_stream()
        if is_exempt and connected_account_ids
        else initial_state
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids or is_exempt:
            # Migrate state if needed (for existing captures with dict state).
            state = await _maybe_migrate_exempt_state(
                child_cls.NAME, binding, state, platform_account_id, task
            )
            fetch_changes_fns = functools.partial(
                fetch_incremental_substreams,
                cls,
                child_cls,
                platform_account_id,
                None,
                http,
                incremental_window_size,
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
                    incremental_window_size,
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
        initial_state=effective_initial_state,
        initial_config=ResourceConfigWithSchedule(
            name=child_cls.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE if needs_schedule else "",
        ),
        schema_inference=True,
    )


def split_child_object(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    incremental_window_size: timedelta,
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
    is_exempt = child_cls.NAME in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    needs_schedule = child_cls.NAME in SCHEDULED_BACKFILL_STREAMS

    # Exempt streams should use non-dict state since they always query
    # the platform account regardless of connected accounts
    effective_initial_state = (
        _create_initial_state_for_exempt_stream()
        if is_exempt and connected_account_ids
        else initial_state
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids or is_exempt:
            # Migrate state if needed (for existing captures with dict state).
            state = await _maybe_migrate_exempt_state(
                child_cls.NAME, binding, state, platform_account_id, task
            )
            fetch_changes_fns = functools.partial(
                fetch_incremental,
                child_cls,
                platform_account_id,
                None,
                http,
                incremental_window_size,
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
                    incremental_window_size,
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
        initial_state=effective_initial_state,
        initial_config=ResourceConfigWithSchedule(
            name=child_cls.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE if needs_schedule else "",
        ),
        schema_inference=True,
    )


def usage_records(
    cls,
    child_cls,
    http: HTTPSession,
    start_date: datetime,
    incremental_window_size: timedelta,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
    initial_state: ResourceState,
) -> Resource:
    """Usage Records handles a specific stream (UsageRecords).
    This is required since Usage Records is a child stream from SubscriptionItem
    and requires special processing.
    """
    is_exempt = child_cls.NAME in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    needs_schedule = child_cls.NAME in SCHEDULED_BACKFILL_STREAMS

    # Exempt streams should use non-dict state since they always query
    # the platform account regardless of connected accounts
    effective_initial_state = (
        _create_initial_state_for_exempt_stream()
        if is_exempt and connected_account_ids
        else initial_state
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if not connected_account_ids or is_exempt:
            # Migrate state if needed (for existing captures with dict state).
            state = await _maybe_migrate_exempt_state(
                child_cls.NAME, binding, state, platform_account_id, task
            )
            fetch_changes_fns = functools.partial(
                fetch_incremental_usage_records,
                cls,
                child_cls,
                platform_account_id,
                None,
                http,
                incremental_window_size,
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
                    incremental_window_size,
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
        initial_state=effective_initial_state,
        initial_config=ResourceConfigWithSchedule(
            name=child_cls.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE if needs_schedule else "",
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
    is_exempt = cls.NAME in CONNECTED_ACCOUNT_EXEMPT_STREAMS
    needs_schedule = cls.NAME in SCHEDULED_BACKFILL_STREAMS

    # Exempt streams should use non-dict state since they always query
    # the platform account regardless of connected accounts
    effective_initial_state = (
        _create_initial_state_for_exempt_stream()
        if is_exempt and connected_account_ids
        else initial_state
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Exempt streams should not use per-account subtasks even when
        # connected accounts exist. They always query the platform account.
        if not connected_account_ids or is_exempt:
            # Migrate state if needed (for existing captures with dict state).
            state = await _maybe_migrate_exempt_state(
                cls.NAME, binding, state, platform_account_id, task
            )

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
        initial_state=effective_initial_state,
        initial_config=ResourceConfigWithSchedule(
            name=cls.NAME,
            interval=timedelta(minutes=5),
            schedule=DEFAULT_SCHEDULE if needs_schedule else "",
        ),
        schema_inference=True,
    )
