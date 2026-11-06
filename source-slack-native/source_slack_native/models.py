from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Literal

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    model_validator,
)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class BotToken(AccessToken):
    """A bot token issued to a Slack app the customer created in their own
    workspace. Declared as one arm of a `credentials_title` union so a managed
    OAuth arm can be added later without breaking existing captures."""

    credentials_title: Literal["Bot Token"] = Field(  # type: ignore[assignment]
        default="Bot Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="Bot User OAuth Token",
        description="The Bot User OAuth Token (starts with xoxb-) of a Slack app installed in your workspace.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    credentials: BotToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 0},
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        json_schema_extra={"order": 1},
    )
    channels: list[str] = Field(
        description="Names or IDs of the channels to capture. If left empty, every public channel the bot is a member of is captured (plus private channels it is a member of, if enabled below).",
        title="Channels",
        default_factory=list,
        json_schema_extra={"order": 2},
    )
    join_public_channels: bool = Field(
        description="If selected, the bot joins every public channel in scope that it is not already a member of. Slack posts a visible message in each channel the bot joins.",
        title="Join Public Channels",
        default=False,
        json_schema_extra={"order": 3},
    )
    include_private_channels: bool = Field(
        description="If selected, private channels the bot has been invited to are captured as well. Requires the groups:read and groups:history scopes.",
        title="Include Private Channels",
        default=False,
        json_schema_extra={"order": 4},
    )

    class Advanced(BaseModel):
        lookback_window: timedelta = Field(
            description="How far back the connector periodically re-reads messages and threads to pick up edits, reactions, and new thread replies. Defaults to 7 days.",
            title="Lookback Window",
            default=timedelta(days=7),
            gt=timedelta(0),
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True, "order": 5},
    )


ConnectorState = GenericConnectorState[ResourceState]


# Every Slack Web API response is an envelope with an `ok` flag. Failures are
# usually returned with HTTP 200 and `ok: false`, so the envelope must be
# checked on every response.
class ResponseMetadata(BaseModel, extra="allow"):
    next_cursor: str = ""


class SlackResponse(BaseModel, extra="allow"):
    ok: bool
    error: str | None = None
    response_metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)


class AuthTestResponse(SlackResponse):
    user_id: str | None = None
    team_id: str | None = None
    bot_id: str | None = None


class HistoryRemainder(SlackResponse):
    has_more: bool = False
    is_limited: bool = False


class SlackDocument(BaseDocument, extra="allow"):
    pass


class User(SlackDocument):
    resource_name: ClassVar[str] = "users"
    path: ClassVar[str] = "users.list"
    items_key: ClassVar[str] = "members"
    page_size: ClassVar[int] = 200

    id: str


class Channel(SlackDocument):
    resource_name: ClassVar[str] = "channels"
    path: ClassVar[str] = "conversations.list"
    items_key: ClassVar[str] = "channels"
    page_size: ClassVar[int] = 999

    id: str
    name: str
    is_member: bool
    is_archived: bool
    is_private: bool


class ChannelMember(SlackDocument):
    resource_name: ClassVar[str] = "channel_members"
    path: ClassVar[str] = "conversations.members"
    items_key: ClassVar[str] = "members"
    page_size: ClassVar[int] = 999

    channel_id: str
    user_id: str


@dataclass(frozen=True, slots=True)
class ChannelIdValidationContext:
    """Validation context carrying the channel_id for document construction."""

    channel_id: str


class ChannelScopedMixin(SlackDocument):
    """Mixin for documents that belong to a channel whose ID is absent from
    the response body. Extends _meta with channel_id and injects it from the
    validation context."""

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(  # pyright: ignore[reportUnannotatedClassAttribute]
            validate_assignment=True
        )
        channel_id: str = Field(
            default="",
            description="The Slack channel this document belongs to",
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=Meta,
        alias="_meta",
        description="Document metadata",
    )

    @model_validator(mode="before")
    @classmethod
    def _inject_channel_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, ChannelIdValidationContext):
            meta = data.get("_meta") or {}
            meta["channel_id"] = info.context.channel_id
            data["_meta"] = meta
        return data


class Message(ChannelScopedMixin):
    resource_name: ClassVar[str] = "messages"
    path: ClassVar[str] = "conversations.history"
    items_key: ClassVar[str] = "messages"
    page_size: ClassVar[int] = 999

    # Slack message timestamps are unique within a channel and double as the
    # message's ID. They are strings of the form "1700000000.123456".
    ts: str
    thread_ts: str | None = None
    reply_count: int | None = None
    latest_reply: str | None = None


class ThreadReply(ChannelScopedMixin):
    resource_name: ClassVar[str] = "threads"
    path: ClassVar[str] = "conversations.replies"
    items_key: ClassVar[str] = "messages"
    page_size: ClassVar[int] = 999

    ts: str
    thread_ts: str


SNAPSHOT_RESOURCES: list[type[User] | type[Channel]] = [User, Channel]

# Streams whose bindings default to a periodic re-backfill. Thread replies to a
# parent older than the lookback window are only picked up this way.
SCHEDULED_BACKFILL_STREAMS = [Message.resource_name, ThreadReply.resource_name]

__all__ = [
    "ResourceConfig",
    "ResourceConfigWithSchedule",
]
