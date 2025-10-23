from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Generic, Literal, TypeVar

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from pydantic import AwareDatetime, BaseModel, Field, model_validator


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0}
    )
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True, "order": 1},
    )



class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    class Advanced(BaseModel):
        window_size: Annotated[timedelta, Field(
            description="Date window size for the events backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours.",
            title="Window size",
            default=timedelta(hours=1),
            ge=timedelta(seconds=30),
            le=timedelta(days=365),
        )]

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

ConnectorState = GenericConnectorState[ResourceState]


class CursorField(StrEnum):
    UPDATED_AT = "updated_at"
    UPDATED = "updated"
    DATETIME = "datetime"


class LowerBoundOperator(StrEnum):
    GREATER_OR_EQUAL = "greater-or-equal"
    GREATER_THAN = "greater-than"


class UpperBoundOperator(StrEnum):
    LESS_OR_EQUAL = "less-or-equal"
    LESS_THAN = "less-than"


class CampaignType(StrEnum):
    EMAIL = "email"
    SMS = "sms"
    MOBILE_PUSH = "mobile_push"


class BaseStream(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]
    extra_params: ClassVar[dict[str, str | int] | None] = None

    id: str


class IncrementalStream(BaseStream):
    supports_less_than_filter: ClassVar[bool] = True
    cursor_field: ClassVar[CursorField]
    additional_filters: ClassVar[list[str] | None] = None

    class Attributes(BaseModel):
        model_config = {"extra": "allow"}

    attributes: Attributes

    def get_cursor_value(self) -> datetime:
        cursor_str: str = getattr(self.attributes, self.cursor_field.value)
        return datetime.fromisoformat(cursor_str.replace('Z', '+00:00'))


# Stream for validating credentials
class Accounts(BaseStream):
    name: ClassVar[str] = "accounts"
    path: ClassVar[str] = "accounts"


# Full refresh streams
class Metrics(BaseStream):
    name: ClassVar[str] = "metrics"
    path: ClassVar[str] = "metrics"

class Tags(BaseStream):
    name: ClassVar[str] = "tags"
    path: ClassVar[str] = "tags"
    extra_params: ClassVar[dict[str, str]] = {
        "include": "tag-group",
    }

class TagGroups(BaseStream):
    name: ClassVar[str] = "tag_groups"
    path: ClassVar[str] = "tag-groups"


class PushTokens(BaseStream):
    name: ClassVar[str] = "push_tokens"
    path: ClassVar[str] = "push-tokens"
    extra_params: ClassVar[dict[str, str]] = {
        "include": "profile",
    }

class Coupons(BaseStream):
    name: ClassVar[str] = "coupons"
    path: ClassVar[str] = "coupons"

class CouponCodes(BaseStream):
    name: ClassVar[str] = "coupon_codes"
    path: ClassVar[str] = "coupon-codes"
    extra_params: ClassVar[dict[str, str]] = {
        "include": "coupon",
    }

# Incremental streams
class Campaigns(IncrementalStream):
    name: ClassVar[str] = "campaigns"
    path: ClassVar[str] = "campaigns"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED_AT


class EmailCampaigns(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.EMAIL}')",
    ]

class EmailCampaignsArchived(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.EMAIL}')",
        "equals(archived,true)"
    ]


class SmsCampaigns(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.SMS}')",
    ]


class SmsCampaignsArchived(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.SMS}')",
        "equals(archived,true)"
    ]


class MobilePushCampaigns(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.MOBILE_PUSH}')",
    ]


class MobilePushCampaignsArchived(Campaigns):
    additional_filters: ClassVar[list[str] | None] = [
        f"equals(messages.channel,'{CampaignType.MOBILE_PUSH}')",
        "equals(archived,true)"
    ]


class Flows(IncrementalStream):
    name: ClassVar[str] = "flows"
    path: ClassVar[str] = "flows"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED


class FlowsArchived(Flows):
    additional_filters: ClassVar[list[str] | None] = [
        "equals(archived,true)"
    ]


class Templates(IncrementalStream):
    name: ClassVar[str] = "templates"
    path: ClassVar[str] = "templates"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED


class Lists(IncrementalStream):
    name: ClassVar[str] = "lists"
    path: ClassVar[str] = "lists"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED
    supports_less_than_filter: ClassVar[bool] = False


class Profiles(IncrementalStream):
    name: ClassVar[str] = "profiles"
    path: ClassVar[str] = "profiles"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED
    extra_params: ClassVar[dict[str, str | int]] = {
        "additional-fields[profile]": "subscriptions",
        "page[size]": 100,
    }


class Segments(IncrementalStream):
    name: ClassVar[str] = "segments"
    path: ClassVar[str] = "segments"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED
    supports_less_than_filter: ClassVar[bool] = False


class Forms(IncrementalStream):
    name: ClassVar[str] = "forms"
    path: ClassVar[str] = "forms"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED_AT


class Images(IncrementalStream):
    name: ClassVar[str] = "images"
    path: ClassVar[str] = "images"
    cursor_field: ClassVar[CursorField] = CursorField.UPDATED_AT
    extra_params: ClassVar[dict[str, str | int]] = {
        "page[size]": 100,
    }


class Events(IncrementalStream):
    name: ClassVar[str] = "events"
    path: ClassVar[str] = "events"
    cursor_field: ClassVar[CursorField] = CursorField.DATETIME

    @model_validator(mode="before")
    @classmethod
    def _transform(cls, values: dict[str, Any]) -> dict[str, Any]:
        event_properties = values["attributes"]["event_properties"]

        # The presence of the "$flow" field means certain fields are present
        # or contain different data. If "$flow" is present, it contains the
        # flow_id associated with the event. If it's not present but a "Campaign Name"
        # field is present, then the "$message" field contains the campaign_id associated
        # with the event.
        if flow_id := event_properties.get("$flow", None):
            values["flow_id"] = flow_id
        elif event_properties.get("Campaign Name", None):
            # Events associated with deleted campaigns do not have a
            # $message field present with the deleted campaign's id.
            campaign_id = event_properties.get("$message", None)
            if campaign_id:
                values["campaign_id"] = campaign_id

        return values


class BaseResponse(BaseModel):
    class Links(BaseModel):
        next: str | None = None

    links: Links


_SnapshotRecord = TypeVar("_SnapshotRecord", bound=BaseStream)


class SnapshotResponse(BaseResponse, Generic[_SnapshotRecord]):
    data: list[_SnapshotRecord]


_IncrementalRecord = TypeVar("_IncrementalRecord", bound=IncrementalStream)


class IncrementalResponse(BaseResponse, Generic[_IncrementalRecord]):
    data: list[_IncrementalRecord]


FULL_REFRESH_STREAMS: list[type[BaseStream]] = [
    Coupons,
    CouponCodes,
    Metrics,
    Tags,
    TagGroups,
    PushTokens,
]


INCREMENTAL_STREAMS: list[type[IncrementalStream]] = [
    Templates,
    Lists,
    Profiles,
    Segments,
    Forms,
    Images,
]
