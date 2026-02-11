import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, ClassVar, TypeVar

import xxhash
from pydantic import AwareDatetime, BaseModel, Field, ValidationInfo, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.http import AccessToken
from estuary_cdk.incremental_csv_processor import BaseCSVRow


SMALLEST_EXPORT_DATE_WINDOW_SIZE = timedelta(seconds=1)


def default_start_date():
    return datetime.now(UTC) - timedelta(days=30)


class ProjectType(StrEnum):
    EMAIL_BASED = "Email-based"
    USER_ID_BASED = "UserID-based"
    HYBRID = "Hybrid"


class ApiKey(AccessToken):
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    project_type: ProjectType = Field(
        description="The type of Iterable project, which determines how users are uniquely identified. See https://support.iterable.com/hc/en-us/articles/9216719179796-Project-Types-and-Unique-Identifiers for more information.",
        title="Project Type",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date.",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    class Advanced(BaseModel):
        window_size: Annotated[timedelta, Field(
            description="Date window size for export jobs in ISO 8601 format. E.g. P5D means 5 days, PT6H means 6 hours.",
            title="Window Size",
            default_factory=lambda: timedelta(hours=12),
            ge=timedelta(seconds=5),
            le=timedelta(days=365),
        )]
        list_users_timeout: Annotated[timedelta, Field(
            description="HTTP request timeout for fetching users of a list in ISO 8601 format. E.g. PT30M means 30 minutes, PT1H means 1 hour. Default is 30 minutes if unset.",
            title="list_users Request Timeout",
            default_factory=lambda: timedelta(minutes=30),
            ge=timedelta(minutes=5),
            le=timedelta(hours=4),
        )]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class TemplateTypes(StrEnum):
    BASE = "Base"
    BLAST = "Blast"
    TRIGGERED = "Triggered"
    WORKFLOW = "Workflow"


class MessageMediums(StrEnum):
    EMAIL = "Email"
    PUSH = "Push"
    IN_APP = "InApp"
    SMS = "SMS"


class EventTypes(StrEnum):
    EMAIL_SEND = "emailSend"
    EMAIL_OPEN = "emailOpen"
    EMAIL_CLICK = "emailClick"
    HOSTED_UNSUBSCRIBE_CLICK = "hostedUnsubscribeClick"
    EMAIL_COMPLAINT = "emailComplaint"
    EMAIL_BOUNCE = "emailBounce"
    EMAIL_SEND_SKIP = "emailSendSkip"
    PUSH_SEND = "pushSend"
    PUSH_OPEN = "pushOpen"
    PUSH_UNINSTALL = "pushUninstall"
    PUSH_BOUNCE = "pushBounce"
    PUSH_SEND_SKIP = "pushSendSkip"
    IN_APP_SEND = "inAppSend"
    IN_APP_OPEN = "inAppOpen"
    IN_APP_CLICK = "inAppClick"
    IN_APP_CLOSE = "inAppClose"
    IN_APP_DELETE = "inAppDelete"
    IN_APP_DELIVERY = "inAppDelivery"
    IN_APP_SEND_SKIP = "inAppSendSkip"
    IN_APP_RECALL = "inAppRecall"
    INBOX_SESSION = "inboxSession"
    INBOX_MESSAGE_IMPRESSION = "inboxMessageImpression"
    SMS_SEND = "smsSend"
    SMS_BOUNCE = "smsBounce"
    SMS_CLICK = "smsClick"
    SMS_RECEIVED = "smsReceived"
    SMS_SEND_SKIP = "smsSendSkip"
    WEB_PUSH_SEND = "webPushSend"
    WEB_PUSH_CLICK = "webPushClick"
    WEB_PUSH_SEND_SKIP = "webPushSendSkip"
    EMAIL_SUBSCRIBE = "emailSubscribe"
    EMAIL_UNSUBSCRIBE = "emailUnsubscribe"
    PURCHASE = "purchase"
    CUSTOM_EVENT = "customEvent"
    SMS_USAGE_INFO = "smsUsageInfo"
    EMBEDDED_SEND = "embeddedSend"
    EMBEDDED_SEND_SKIP = "embeddedSendSkip"
    EMBEDDED_CLICK = "embeddedClick"
    EMBEDDED_RECEIVED = "embeddedReceived"
    EMBEDDED_IMPRESSION = "embeddedImpression"
    EMBEDDED_SESSION = "embeddedSession"
    UNKNOWN_SESSION = "unknownSession"
    JOURNEY_EXIT = "journeyExit"
    WHATSAPP_BOUNCE = "whatsAppBounce"
    WHATSAPP_CLICK = "whatsAppClick"
    WHATSAPP_RECEIVED = "whatsAppReceived"
    WHATSAPP_SEEN = "whatsAppSeen"
    WHATSAPP_SEND = "whatsAppSend"
    WHATSAPP_SEND_SKIP = "whatsAppSendSkip"
    WHATSAPP_USAGE_INFO = "whatsAppUsageInfo"


class IterableResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]
    interval: ClassVar[timedelta] = timedelta(minutes=15)
    disable: ClassVar[bool] = False


TIterableResource = TypeVar(name="TIterableResource", bound=IterableResource)


class ResourceWithId(IterableResource, extra="allow"):
    id: int


# Full refresh streams
class Channels(ResourceWithId):
    name: ClassVar[str] = "channels"
    path: ClassVar[str] = "channels"


class MessageTypes(ResourceWithId):
    name: ClassVar[str] = "message_types"
    path: ClassVar[str] = "messageTypes"


class Lists(ResourceWithId):
    name: ClassVar[str] = "lists"
    path: ClassVar[str] = "lists"


class MetadataTables(IterableResource):
    name: ClassVar[str] = "metadata_tables"
    path: ClassVar[str] = "metadata"


class Templates(IterableResource):
    name: ClassVar[str] = "templates"
    path: ClassVar[str] = "templates"


class ListUsers(IterableResource):
    name: ClassVar[str] = "list_users"
    path: ClassVar[str] = "lists/getUsers"
    # Since list_users has a backfill task but not an incremental task,
    # the interval has no effect. Instead, the schedule
    # dictates how frequently the stream re-backfills all records.
    interval: ClassVar[timedelta] = timedelta(hours=24)
    schedule: ClassVar[str] = "55 23 * * *"
    disable: ClassVar[bool] = True

    list_id: int
    user_id: str


# Incremental streams
class CampaignPreLaunchStates(StrEnum):
    DRAFT = "Draft"
    RECURRING = "Recurring"
    SCHEDULED = "Scheduled"
    STARTING = "Starting"


class CampaignInProgressStates(StrEnum):
    READY = "Ready"
    RUNNING = "Running"
    RECALLING = "Recalling"


class CampaignFinalStates(StrEnum):
    ABORTED = "Aborted"
    ARCHIVED = "Archived"
    FINISHED = "Finished"
    RECALLED = "Recalled"


CampaignState = CampaignPreLaunchStates | CampaignInProgressStates | CampaignFinalStates


class Campaigns(ResourceWithId):
    name: ClassVar[str] = "campaigns"
    path: ClassVar[str] = "campaigns"
    interval: ClassVar[timedelta] = timedelta(minutes=5)

    createdAt: int
    updatedAt: int
    endedAt: int = Field(
        default=None,
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop('default') # type: ignore
    )
    campaignState: CampaignState

    @property
    def most_recent_ts(self) -> int:
        if self.endedAt is not None:
            return max(self.updatedAt, self.endedAt)
        return self.updatedAt


class CampaignsResponse(BaseModel):
    campaigns: list[Campaigns]
    nextPageUrl: str | None = None


class CampaignMetrics(BaseCSVRow):
    name: ClassVar[str] = "campaign_metrics"
    path: ClassVar[str] = "campaigns/metrics"
    interval: ClassVar[timedelta] = timedelta(minutes=15)
    disable: ClassVar[bool] = False

    id: int


# Iterable returns strings that are almost ISO 8601 compliant datetimes,
# but aren't quite. In order for Flow's schema inference to infer
# a datetime format, the connector transforms these non-ISO 8601 compliant
# strings into compliant datetimes. Examples of non-compliant strings
# that are transformed include:
#
# 1. Space-separated datetime with timezone offset:
#    Input:  '2026-01-14 06:20:22 +00:00' or '2026-01-14 06:20:22 +0000'
#    Output: '2026-01-14T06:20:22+00:00'
#    Fix: Replace spaces with 'T', ensure colon in timezone offset.
#
# 2. Malformed datetime with double fractional seconds:
#    Input:  '2025-12-19T22:43:34.134105.000Z'
#    Output: '2025-12-19T22:43:34.134105Z'
#    Fix: Strip the extraneous '.000' before the 'Z'.

# Matches: 'YYYY-MM-DD HH:MM:SS +HH:MM' or 'YYYY-MM-DD HH:MM:SS +HHMM'
_SPACE_SEPARATED_DATETIME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-])(\d{2}):?(\d{2})$")

# Matches: 'YYYY-MM-DDTHH:MM:SS.<digits>.<3 digits>Z' (malformed double fractional seconds)
_MANGLED_FRACTIONAL_DATETIME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)\.\d{3}Z$")


def _fix_iterable_datetime(value: str) -> str:
    """Fix a single Iterable datetime string to ISO 8601 format.

    Returns the original value unchanged if it doesn't match any known Iterable datetime format.
    """
    if match := _SPACE_SEPARATED_DATETIME_RE.match(value):
        date, time, sign, tz_hours, tz_mins = match.groups()
        return f"{date}T{time}{sign}{tz_hours}:{tz_mins}"
    if match := _MANGLED_FRACTIONAL_DATETIME_RE.match(value):
        return f"{match.group(1)}Z"
    return value


def _fix_iterable_datetimes(data: dict | list):
    """Recursively fix Iterable datetime strings in nested data structures.

    Mutates `data` in place, applying _fix_iterable_datetime to all string values.
    """
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, str):
                data[k] = _fix_iterable_datetime(v)
            elif isinstance(v, (dict, list)):
                _fix_iterable_datetimes(v)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, str):
                data[i] = _fix_iterable_datetime(item)
            elif isinstance(item, (dict, list)):
                _fix_iterable_datetimes(item)


class ExportResource(IterableResource):
    path: ClassVar[str] = "export"
    interval: ClassVar[timedelta] = timedelta(minutes=5)

    @model_validator(mode="before")
    @classmethod
    def fix_datetime_formats(cls, data: dict) -> dict:
        _fix_iterable_datetimes(data)
        return data

    @property
    def cursor_value(self) -> AwareDatetime:
        raise NotImplementedError

    @property
    def identifier(self) -> str:
        raise NotImplementedError


TExportResource = TypeVar(name="TExportResource", bound=ExportResource)


@dataclass
class EventValidationContext:
    event_type: str


class Events(ExportResource):
    name: ClassVar[str] = "events"

    email: str
    createdAt: AwareDatetime
    eventType: str

    # Not all events have a natural unique identifier, and what fields could
    # help uniquely identify an event are different across event types. This means
    # that some of these potential key fields are nullable, which is not allowed
    # for Flow collection keys. So we insert a synthetic id computed from a hash of
    # identifying fields.
    estuary_id: str = Field(alias="_estuary_id")

    # Optional fields used in synthetic ID computation.
    # These may be absent from the source data.
    itblUserId: str = Field(
        default=None,
        json_schema_extra=lambda x: x.pop('default')  # type: ignore
    )
    campaignId: int = Field(
        default=None,
        json_schema_extra=lambda x: x.pop('default')  # type: ignore
    )
    eventName: str = Field(
        default=None,
        json_schema_extra=lambda x: x.pop('default')  # type: ignore
    )

    @staticmethod
    def _get_id_part(data: dict, key: str) -> str:
        value = data.get(key)
        return "" if value is None else str(value)

    @model_validator(mode="before")
    @classmethod
    def _add_event_type_and_synthetic_id(cls, data: dict, info: ValidationInfo) -> dict:
        if not info.context or not isinstance(info.context, EventValidationContext):
            raise RuntimeError(f"Validation context must be of type EventValidationContext: {info.context}")

        event_type = info.context.event_type
        data["eventType"] = event_type

        parts = [
            cls._get_id_part(data, "createdAt"),
            cls._get_id_part(data, "email"),
            cls._get_id_part(data, "itblUserId"),
            cls._get_id_part(data, "campaignId"),
            cls._get_id_part(data, "eventName"),
            event_type,
        ]
        data["_estuary_id"] = xxhash.xxh128("|".join(parts).encode()).hexdigest()

        return data

    @property
    def cursor_value(self) -> AwareDatetime:
        return self.createdAt

    @property
    def identifier(self) -> str:
        return self.estuary_id


class BaseUsers(ExportResource):
    primary_key: ClassVar[str]
    name: ClassVar[str] = "users"
    data_type: ClassVar[str] = "user"

    profileUpdatedAt: AwareDatetime

    @property
    def cursor_value(self) -> AwareDatetime:
        return self.profileUpdatedAt


class UsersWithIds(BaseUsers):
    primary_key: ClassVar[str] = "itblUserId"

    itblUserId: str

    @property
    def identifier(self) -> str:
        return self.itblUserId


class UsersWithEmails(BaseUsers):
    primary_key: ClassVar[str] = "email"

    email: str

    @property
    def identifier(self) -> str:
        return self.email
