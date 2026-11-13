import asyncio
import functools
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    SlackAPIError,
    auth_test,
    backfill_messages,
    backfill_threads,
    fetch_messages,
    fetch_messages_lookback,
    fetch_threads,
    fetch_threads_lookback,
    join_channel,
    list_channels,
    snapshot_channel_members,
    snapshot_channels,
    snapshot_users,
)
from .models import (
    SCHEDULED_BACKFILL_STREAMS,
    Channel,
    ChannelMember,
    ConnectorState,
    EndpointConfig,
    Message,
    ThreadReply,
    User,
)

# users.list and conversations.list are Tier 2 methods, so the snapshot
# streams refresh less often than the default incremental interval.
SNAPSHOT_INTERVAL = timedelta(hours=1)

# Weekly re-backfill of the message and thread streams. Thread replies to a
# parent older than the lookback window are only captured by a re-backfill.
DEFAULT_SCHEDULE = "0 0 * * 0"

LOOKBACK = "lookback"

USER_TOKEN_PREFIX = "xoxp-"
APP_LEVEL_TOKEN_PREFIX = "xapp-"
BOT_TOKEN_PREFIX = "xoxb-"


def _check_token_kind(token: str) -> None:
    if token.startswith(USER_TOKEN_PREFIX):
        raise ValidationError(
            [
                "The provided token is a user token (xoxp-). Provide the Bot User OAuth Token (xoxb-) from your Slack app's OAuth & Permissions page instead."
            ]
        )
    if token.startswith(APP_LEVEL_TOKEN_PREFIX):
        raise ValidationError(
            [
                "The provided token is an app-level token (xapp-), which cannot read workspace data. Provide the Bot User OAuth Token (xoxb-) from your Slack app's OAuth & Permissions page instead."
            ]
        )
    if not token.startswith(BOT_TOKEN_PREFIX):
        raise ValidationError(
            [
                "The provided token is not a Slack bot token. Provide the Bot User OAuth Token (xoxb-) from your Slack app's OAuth & Permissions page."
            ]
        )


async def resolve_channels(
    log: Logger,
    http: HTTPSession,
    config: EndpointConfig,
) -> tuple[list[Channel], list[str]]:
    """Return the channels in scope for this capture, and a description of
    each configured channel that cannot be captured.

    A channel is in scope if the bot can read its history: it is a member, or
    the channel is public, unarchived, and the bot is configured to join it.
    """

    def readable(channel: Channel) -> bool:
        if channel.is_member:
            return True
        return (
            config.join_public_channels
            and not channel.is_private
            and not channel.is_archived
        )

    all_channels = [
        c async for c in list_channels(http, config.include_private_channels, log)
    ]

    if not config.channels:
        return [c for c in all_channels if readable(c)], []

    by_id = {c.id: c for c in all_channels}
    by_name = {c.name: c for c in all_channels}

    targets: dict[str, Channel] = {}
    problems: list[str] = []
    for entry in config.channels:
        ref = entry.strip().removeprefix("#")
        channel = by_id.get(ref) or by_name.get(ref)

        if channel is None:
            problems.append(
                f"Channel '{entry}' was not found. Private channels are only visible if the bot has been invited and private channels are enabled."
            )
        elif not readable(channel):
            if channel.is_archived:
                reason = "it is archived and the bot was not a member"
            else:
                reason = "the bot is not a member; invite the bot to the channel or enable joining public channels"
            problems.append(f"Channel '{entry}' cannot be read: {reason}.")
        else:
            targets[channel.id] = channel

    return list(targets.values()), problems


async def join_missing_channels(
    log: Logger, http: HTTPSession, channels: list[Channel]
) -> None:
    for channel in channels:
        if channel.is_member:
            continue
        log.info(
            "joining channel", {"channel_id": channel.id, "channel_name": channel.name}
        )
        await join_channel(http, channel.id, log)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the token is a working bot token and that every configured
    channel can be captured."""
    _check_token_kind(config.credentials.access_token)
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        await auth_test(http, log)
        _, problems = await resolve_channels(log, http, config)
    except SlackAPIError as err:
        if err.error in (
            "invalid_auth",
            "not_authed",
            "token_revoked",
            "account_inactive",
        ):
            msg = f"Invalid bot token. Please confirm the provided token is correct and the app is still installed in the workspace. Slack error: {err.error}"
        elif err.error == "missing_scope":
            msg = f"The bot token is missing a required scope for {err.method}. Add the scopes listed in the connector documentation to your Slack app and reinstall it."
        else:
            msg = (
                f"Encountered error validating the bot token. Slack error: {err.error}"
            )
        raise ValidationError([msg])
    except HTTPError as err:
        raise ValidationError(
            [f"Encountered error validating the bot token.\n\n{err.message}"]
        )

    if problems:
        raise ValidationError(problems)


def _snapshot_resource(name: str, open_fn) -> common.Resource:
    return common.SnapshotResource(
        name=name,
        open=open_fn,
        initial_config=ResourceConfig(name=name, interval=SNAPSHOT_INTERVAL),
        schema_inference=True,
    )


def users(http: HTTPMixin) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_users, http),
        )

    return _snapshot_resource(User.resource_name, open)


def channels(http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_channels, http, config.include_private_channels
            ),
        )

    return _snapshot_resource(Channel.resource_name, open)


class ChannelScope:
    """The channels in scope for this capture, resolved once per process and
    shared by every channel-scoped binding. Joins public channels first when
    the capture is configured to."""

    def __init__(self, http: HTTPMixin, config: EndpointConfig):
        self._http = http
        self._config = config
        self._lock = asyncio.Lock()
        self._channels: list[Channel] | None = None

    async def get(self, log: Logger) -> list[Channel]:
        async with self._lock:
            if self._channels is None:
                channels, problems = await resolve_channels(
                    log, self._http, self._config
                )
                for problem in problems:
                    log.warning(problem)
                if self._config.join_public_channels:
                    await join_missing_channels(log, self._http, channels)
                self._channels = channels
            return self._channels


def channel_members(http: HTTPMixin, scope: ChannelScope) -> common.Resource:
    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        channel_ids = [c.id for c in await scope.get(task.log)]
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_channel_members, http, channel_ids
            ),
        )

    return _snapshot_resource(ChannelMember.resource_name, open)


def _lookback_key(channel_id: str) -> str:
    return f"{channel_id}_{LOOKBACK}"


def _channel_states(channel_ids: list[str], cutoff: datetime) -> ResourceState:
    """Seed one backfill, one incremental, and one lookback subtask per
    channel. The incremental cursor starts one tick before the cutoff, which
    the backfill excludes."""
    return ResourceState(
        backfill={
            channel_id: ResourceState.Backfill(cutoff=cutoff, next_page=None)
            for channel_id in channel_ids
        },
        inc={
            **{
                channel_id: ResourceState.Incremental(
                    cursor=cutoff - timedelta(microseconds=1)
                )
                for channel_id in channel_ids
            },
            **{
                _lookback_key(channel_id): ResourceState.Incremental(cursor=cutoff)
                for channel_id in channel_ids
            },
        },
    )


async def _patch_missing_channel_states(
    binding: CaptureBinding[ResourceConfigWithSchedule],
    state: ResourceState,
    task: Task,
    channel_ids: list[str],
):
    """Add subtask state for channels that came into scope after the binding
    was initialized, so they are backfilled from the start date."""
    assert isinstance(state.inc, dict) and isinstance(state.backfill, dict)

    missing = [cid for cid in channel_ids if cid not in state.inc]
    if not missing:
        return

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)
    new_states = _channel_states(missing, cutoff)
    assert isinstance(new_states.inc, dict)
    assert isinstance(new_states.backfill, dict)

    old_state = deepcopy(state)
    state.inc.update(new_states.inc)
    state.backfill.update(new_states.backfill)

    task.log.info(
        f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}.",
        {"prev_state": old_state, "new_state": state},
    )
    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


def _channel_incremental_resource(
    model: type[Message] | type[ThreadReply],
    fetch_changes,
    fetch_lookback,
    fetch_page,
    scope: ChannelScope,
    config: EndpointConfig,
) -> common.Resource:
    """A stream read per channel, with a realtime, a lookback, and a backfill
    subtask for each channel in scope. Patterned on source-posthog's `events`
    (source-posthog/source_posthog/resources.py:318)."""

    async def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        channel_ids = [c.id for c in await scope.get(task.log)]
        if not channel_ids:
            task.log.warning(
                "no channels are in scope; nothing to capture",
                {"stream": model.resource_name},
            )
            return

        await _patch_missing_channel_states(binding, state, task, channel_ids)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                **{cid: functools.partial(fetch_changes, cid) for cid in channel_ids},
                **{
                    _lookback_key(cid): functools.partial(
                        fetch_lookback, cid, config.advanced.lookback_window
                    )
                    for cid in channel_ids
                },
            },
            fetch_page={
                cid: functools.partial(fetch_page, cid, config.start_date)
                for cid in channel_ids
            },
        )

    return common.Resource(
        name=model.resource_name,
        key=["/_meta/channel_id", "/ts"],
        model=model,
        open=open,
        # Channels are only known once the capture opens, which fills this in.
        initial_state=ResourceState(inc={}, backfill={}),
        initial_config=ResourceConfigWithSchedule(
            name=model.resource_name,
            interval=timedelta(minutes=5),
            schedule=(
                DEFAULT_SCHEDULE
                if model.resource_name in SCHEDULED_BACKFILL_STREAMS
                else ""
            ),
        ),
        schema_inference=True,
    )


def messages(
    http: HTTPMixin, scope: ChannelScope, config: EndpointConfig
) -> common.Resource:
    return _channel_incremental_resource(
        Message,
        functools.partial(fetch_messages, http),
        functools.partial(fetch_messages_lookback, http),
        functools.partial(backfill_messages, http),
        scope,
        config,
    )


def threads(
    http: HTTPMixin, scope: ChannelScope, config: EndpointConfig
) -> common.Resource:
    return _channel_incremental_resource(
        ThreadReply,
        functools.partial(fetch_threads, http),
        functools.partial(fetch_threads_lookback, http),
        functools.partial(backfill_threads, http),
        scope,
        config,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes. Discovery is static: no
    provider calls are made here."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    scope = ChannelScope(http, config)
    return [
        users(http),
        channels(http, config),
        channel_members(http, scope),
        messages(http, scope, config),
        threads(http, scope, config),
    ]
