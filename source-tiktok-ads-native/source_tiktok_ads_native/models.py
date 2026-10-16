import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar
from zoneinfo import ZoneInfo

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    create_model,
    field_validator,
    model_validator,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    AccessToken,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
)


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"


# TikTok's token exchange takes a JSON body and names the app credentials
# `app_id` / `secret` and the authorization code `auth_code`.
accessTokenBody = {
    "app_id": "{{{ client_id }}}",
    "secret": "{{{ client_secret }}}",
    "auth_code": "{{{ code }}}",
}

OAUTH2_SPEC = OAuth2Spec(
    provider="tiktok",
    # The authorize URL the existing source-tiktok-marketing connector uses with
    # Estuary's registered "tiktok" OAuth app.
    authUrlTemplate=(
        "https://ads.tiktok.com/marketing_api/auth?"
        f"app_id={urlencode_field('client_id')}&"
        f"redirect_uri={urlencode_field('redirect_uri')}&"
        "response_type=code&"
        f"state={urlencode_field('state')}"
    ),
    accessTokenUrlTemplate="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/",
    accessTokenHeaders={"content-type": "application/json"},
    accessTokenBody=json.dumps(accessTokenBody),
    # TikTok wraps every response in {"code", "message", "data"}; the token
    # never expires and there is no refresh token.
    accessTokenResponseMap={"access_token": "/data/access_token"},
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


# --- Reports --------------------------------------------------------------------------


class ReportType(StrEnum):
    BASIC = "BASIC"
    AUDIENCE = "AUDIENCE"


class DataLevel(StrEnum):
    ADVERTISER = "AUCTION_ADVERTISER"
    CAMPAIGN = "AUCTION_CAMPAIGN"
    AD_GROUP = "AUCTION_ADGROUP"
    AD = "AUCTION_AD"


class Granularity(StrEnum):
    DAILY = "daily"
    HOURLY = "hourly"
    LIFETIME = "lifetime"


class Breakdown(StrEnum):
    AGE = "age"
    GENDER = "gender"
    COUNTRY_CODE = "country_code"
    LANGUAGE = "language"
    PLATFORM = "platform"
    PLACEMENT = "placement"
    NETWORK = "ac"


# A report's level fixes its one ID dimension and its granularity fixes its one
# time dimension, so neither is configurable and TikTok's one-of-each rule holds.
ID_DIMENSION: dict[DataLevel, str] = {
    DataLevel.ADVERTISER: "advertiser_id",
    DataLevel.CAMPAIGN: "campaign_id",
    DataLevel.AD_GROUP: "adgroup_id",
    DataLevel.AD: "ad_id",
}
TIME_DIMENSION: dict[Granularity, str | None] = {
    Granularity.DAILY: "stat_time_day",
    Granularity.HOURLY: "stat_time_hour",
    Granularity.LIFETIME: None,
}

MAX_METRICS = 100
CUSTOM_REPORT_NAME_PREFIX = "custom_report_"
NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class ReportConfig(BaseModel):
    name: str = Field(
        title="Report Name",
        description="Lowercase letters, digits and underscores. The report is captured into a collection named custom_report_<name>.",
        json_schema_extra={"order": 0},
    )
    report_type: ReportType = Field(
        default=ReportType.BASIC,
        title="Report Type",
        description="BASIC reports break performance down by time. AUDIENCE reports also break it down by audience attributes such as age or country, daily only.",
        json_schema_extra={"order": 1},
    )
    data_level: DataLevel = Field(
        default=DataLevel.AD,
        title="Level",
        description="The level rows are reported at. Each row carries that level's ID.",
        json_schema_extra={"order": 2},
    )
    granularity: Granularity = Field(
        default=Granularity.DAILY,
        title="Granularity",
        description="Daily or hourly rows, in each advertiser's time zone, or one lifetime row per entity.",
        json_schema_extra={"order": 3},
    )
    breakdowns: list[Breakdown] = Field(
        default=[],
        title="Breakdowns",
        description="AUDIENCE reports need exactly one breakdown, or age and gender together. BASIC reports may break down by country_code, daily only.",
        json_schema_extra={"order": 4},
    )
    metrics: str = Field(
        title="Metrics",
        description="A comma-separated list of TikTok report metrics, up to 100.",
        examples=["spend,impressions,clicks,web_event_add_to_cart,on_web_subscribe"],
        json_schema_extra={"order": 5},
    )

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not NAME_PATTERN.fullmatch(value):
            raise ValueError(
                f"Invalid report name {value!r}: use lowercase letters, digits and underscores, starting with a letter."
            )
        return value

    @field_validator("metrics")
    @classmethod
    def _check_metrics(cls, value: str) -> str:
        metrics = _split_metrics(value)
        if not metrics:
            raise ValueError("A report needs at least one metric.")
        if len(metrics) > MAX_METRICS:
            raise ValueError(
                f"A report can request at most {MAX_METRICS} metrics, not {len(metrics)}."
            )
        invalid = [m for m in metrics if not NAME_PATTERN.fullmatch(m)]
        if invalid:
            raise ValueError(f"Invalid metric names: {', '.join(invalid)}.")
        return value

    @model_validator(mode="after")
    def _check_dimensions(self) -> "ReportConfig":
        breakdowns = set(self.breakdowns)
        if len(breakdowns) != len(self.breakdowns):
            raise ValueError("A breakdown is listed more than once.")

        if self.report_type == ReportType.AUDIENCE:
            if not breakdowns:
                raise ValueError("An AUDIENCE report needs a breakdown.")
            if len(breakdowns) > 1 and breakdowns != {Breakdown.AGE, Breakdown.GENDER}:
                raise ValueError(
                    "An AUDIENCE report takes one breakdown, or age and gender together."
                )
            if self.granularity != Granularity.DAILY:
                raise ValueError(
                    "TikTok serves AUDIENCE reports at daily granularity only."
                )
        elif breakdowns:
            if breakdowns != {Breakdown.COUNTRY_CODE}:
                raise ValueError("A BASIC report can only break down by country_code.")
            if self.granularity != Granularity.DAILY:
                raise ValueError(
                    "TikTok serves country_code breakdowns at daily granularity only."
                )

        return self

    @property
    def metric_list(self) -> list[str]:
        return _split_metrics(self.metrics)

    @property
    def id_dimension(self) -> str:
        return ID_DIMENSION[self.data_level]

    @property
    def time_dimension(self) -> str | None:
        return TIME_DIMENSION[self.granularity]

    def dimensions(self) -> list[str]:
        """The request's dimensions: the level's ID, the time dimension, the breakdowns."""
        time = [self.time_dimension] if self.time_dimension else []
        return [self.id_dimension, *time, *(str(b) for b in self.breakdowns)]

    def key(self) -> list[str]:
        """The collection key: the advertiser plus every dimension."""
        fields = ["advertiser_id"] + [
            d for d in self.dimensions() if d != "advertiser_id"
        ]
        return [f"/{f}" for f in fields]


def _split_metrics(value: str) -> list[str]:
    return list(dict.fromkeys(m.strip() for m in value.split(",") if m.strip()))


# The one report captured without configuration: ad level rolls up to every other
# level, and these long-standing core metrics are the ones TikTok is least likely
# to retire. Anything else is a custom report.
DEFAULT_REPORTS: tuple[ReportConfig, ...] = (
    ReportConfig(
        name="ad_report_daily",
        report_type=ReportType.BASIC,
        data_level=DataLevel.AD,
        granularity=Granularity.DAILY,
        metrics=(
            "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,conversion,"
            "cost_per_conversion,conversion_rate,video_play_actions,video_watched_2s,"
            "video_watched_6s,likes,comments,shares,follows,profile_visits"
        ),
    ),
)


class ReportDocument(BaseDocument, extra="allow"):
    advertiser_id: str


def report_document_model(report: ReportConfig) -> type[ReportDocument]:
    """The document model for a report. Only the key fields are declared; metrics
    vary by report and are left to schema inference."""
    fields: dict[str, Any] = {
        d: (str, ...) for d in report.dimensions() if d != report.time_dimension
    }
    if report.granularity == Granularity.DAILY:
        fields["stat_time_day"] = (date, ...)
    elif report.granularity == Granularity.HOURLY:
        fields["stat_time_hour"] = (AwareDatetime, ...)
    return create_model(report.name, __base__=ReportDocument, **fields)


# A pasted access token, from a sandbox ad account or the customer's own TikTok
# developer app. The CDK's class has no title, so the web app would show its
# class name.
class AccessTokenCredentials(AccessToken):
    model_config = ConfigDict(title="Access Token")


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    # A value typed into the wrong field may be a secret, so pydantic's errors
    # never echo what was entered. Report names and metrics, which aren't
    # secrets, are named in their own errors to help fix typos.
    model_config = ConfigDict(hide_input_in_errors=True)

    credentials: OAuth2Credentials | AccessTokenCredentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Report data before this date will not be replicated; campaigns, ad groups and ads are captured in full. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    advertiser_ids: list[str] = Field(
        default=[],
        title="Advertiser IDs",
        description="Advertiser IDs to capture. If left empty, every advertiser the authorized TikTok account can access is captured. Required with an access token or a sandbox account.",
    )
    custom_reports: list[ReportConfig] = Field(
        default=[],
        title="Custom Reports",
        description="Reports to capture in addition to the default ad_report_daily. Each becomes its own collection.",
    )

    @field_validator("advertiser_ids")
    @classmethod
    def _clean_advertiser_ids(cls, value: list[str]) -> list[str]:
        """Drop blank entries, which the web app can submit, so that a list with
        none left counts as empty, and drop repeated ones. TikTok advertiser IDs
        are digits; an invalid entry is named by position, since a value pasted
        into the wrong field may be a secret."""
        stripped = [i.strip() for i in value]
        invalid = [
            str(n)
            for n, i in enumerate(stripped, start=1)
            if i and not (i.isascii() and i.isdigit())
        ]
        if invalid:
            raise ValueError(
                f"Advertiser IDs contain only digits, such as 1234567890123456789. Check entry {', '.join(invalid)}."
            )
        return list(dict.fromkeys(i for i in stripped if i))

    @model_validator(mode="after")
    def _check_report_names(self) -> "EndpointConfig":
        names = [r.name for r in self.custom_reports]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(
                f"Custom report names must be unique: {', '.join(duplicates)}."
            )
        return self

    def reports(self) -> list[ReportConfig]:
        """Every report to capture: the defaults, then the custom reports under
        their namespaced collection names."""
        return [*DEFAULT_REPORTS] + [
            r.model_copy(update={"name": CUSTOM_REPORT_NAME_PREFIX + r.name})
            for r in self.custom_reports
        ]

    class Advanced(BaseModel):
        is_sandbox: bool = Field(
            description="Check if you are using a TikTok sandbox ad account. Sandbox access tokens and advertiser IDs come from the Sandbox Ad Account page of a TikTok API for Business developer app.",
            title="Is a Sandbox Environment",
            default=False,
        )
        lookback_window_days: int = Field(
            description="Days of already-captured report data to re-read on each sync. TikTok keeps attributing conversions to past days for up to the ad group's click attribution window, at most 28 days. A lower value re-reads less data, but misses late conversions from ad groups with a longer window.",
            title="Report Lookback (Days)",
            default=28,
            ge=0,
            le=28,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class TikTokEnvelope(BaseModel):
    """The fields every TikTok response carries, success or failure."""

    code: int
    message: str


class TikTokResponse[DataT](TikTokEnvelope):
    data: DataT


class AdvertiserListItem(BaseModel):
    advertiser_id: str


class AdvertiserList(BaseModel):
    """`data` of both `oauth2/advertiser/get` and `advertiser/info`."""

    advertisers: list[AdvertiserListItem] = Field(alias="list")


# TikTok entity timestamps are UTC with one-second resolution and no offset.
TIKTOK_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_tiktok_datetime(value: Any) -> Any:
    if isinstance(value, str):
        return datetime.strptime(value, TIKTOK_DATETIME_FORMAT).replace(tzinfo=UTC)
    return value


TikTokDatetime = Annotated[AwareDatetime, BeforeValidator(_parse_tiktok_datetime)]


def _parse_unix_seconds(value: Any) -> Any:
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=UTC)
    return value


# Advertiser timestamps are Unix seconds instead.
TikTokUnixTimestamp = Annotated[AwareDatetime, BeforeValidator(_parse_unix_seconds)]


class Advertiser(BaseDocument, extra="allow"):
    advertiser_id: str
    create_time: TikTokUnixTimestamp
    display_timezone: str

    def zone(self) -> ZoneInfo:
        """The time zone the account's report dates and hours are in: the named
        zone, which follows daylight saving, not the fixed-offset `timezone`."""
        return ZoneInfo(self.display_timezone)


class AdvertiserBatch(BaseModel):
    """`data` of `advertiser/info`."""

    advertisers: list[Advertiser] = Field(alias="list")


class PageInfo(BaseModel):
    page: int
    total_page: int


class TikTokPageRemainder(TikTokEnvelope):
    """Everything in a list response except the streamed `data.list` items."""

    class Data(BaseModel):
        page_info: PageInfo

    # An error response carries `data: {}`, so `data` is only validated as a
    # page once `code` shows success.
    data: Any = None

    def page_info(self) -> PageInfo:
        return self.Data.model_validate(self.data).page_info


@dataclass(frozen=True)
class Cascade:
    """A child status meaning the child's parent was deleted. TikTok sets it
    without changing the child's `modify_time`, so the parent's deletion time
    stands in for it."""

    status: str
    parent: type["TikTokEntity"]
    # The child's field holding the parent's id; a required field on the child.
    parent_id_field: str


class TikTokEntity(BaseDocument, extra="allow"):
    """An advertiser-scoped object TikTok lists page by page and stamps with a
    `modify_time` on every change, including a direct deletion."""

    PATH: ClassVar[str]
    # The field holding the object's ID, which is also its collection key.
    ID_FIELD: ClassVar[str]
    # `secondary_status` filter values listing every object of this type, and
    # only the directly deleted ones.
    ALL_STATUS: ClassVar[str]
    DELETED_STATUS: ClassVar[str]
    CASCADES: ClassVar[tuple[Cascade, ...]] = ()
    # The server-side "modified since" filter, where the endpoint has one.
    MODIFIED_AFTER_FILTER: ClassVar[str | None] = None

    create_time: TikTokDatetime
    modify_time: TikTokDatetime

    def get_cursor(self) -> datetime:
        return self.modify_time

    def get_id(self) -> str:
        return getattr(self, self.ID_FIELD)


class Campaign(TikTokEntity):
    PATH: ClassVar[str] = "campaign/get/"
    ID_FIELD: ClassVar[str] = "campaign_id"
    ALL_STATUS: ClassVar[str] = "CAMPAIGN_STATUS_ALL"
    DELETED_STATUS: ClassVar[str] = "CAMPAIGN_STATUS_DELETE"

    campaign_id: str


class AdGroup(TikTokEntity):
    PATH: ClassVar[str] = "adgroup/get/"
    ID_FIELD: ClassVar[str] = "adgroup_id"
    ALL_STATUS: ClassVar[str] = "ADGROUP_STATUS_ALL"
    DELETED_STATUS: ClassVar[str] = "ADGROUP_STATUS_DELETE"
    CASCADES: ClassVar[tuple[Cascade, ...]] = (
        Cascade("ADGROUP_STATUS_CAMPAIGN_DELETE", Campaign, "campaign_id"),
    )

    adgroup_id: str
    campaign_id: str


class Ad(TikTokEntity):
    PATH: ClassVar[str] = "ad/get/"
    ID_FIELD: ClassVar[str] = "ad_id"
    ALL_STATUS: ClassVar[str] = "AD_STATUS_ALL"
    DELETED_STATUS: ClassVar[str] = "AD_STATUS_DELETE"
    CASCADES: ClassVar[tuple[Cascade, ...]] = (
        Cascade("AD_STATUS_ADGROUP_DELETE", AdGroup, "adgroup_id"),
        Cascade("AD_STATUS_CAMPAIGN_DELETE", Campaign, "campaign_id"),
    )
    MODIFIED_AFTER_FILTER: ClassVar[str | None] = "modified_after"

    ad_id: str
    adgroup_id: str
    campaign_id: str
