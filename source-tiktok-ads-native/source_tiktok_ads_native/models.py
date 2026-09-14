from datetime import datetime, timedelta, UTC
from enum import StrEnum
import json
from typing import Annotated, Any, ClassVar, TYPE_CHECKING

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)

from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    ValidationInfo,
    create_model,
    field_validator,
    model_validator,
)


# TikTok names its OAuth parameters `app_id` / `secret` / `auth_code` rather than the
# usual `client_id` / `client_secret` / `code`. The control plane only substitutes its
# own variable names, so each one is mapped onto TikTok's spelling here.
OAUTH2_SPEC = OAuth2Spec(
    provider="tiktok",
    authUrlTemplate=(
        "https://ads.tiktok.com/marketing_api/auth?"
        r"app_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&response_type=code"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/",
    accessTokenHeaders={"Content-Type": "application/json"},
    accessTokenBody=json.dumps(
        {
            "app_id": "{{{ client_id }}}",
            "secret": "{{{ client_secret }}}",
            "auth_code": "{{{ code }}}",
        }
    ),
    # TikTok wraps every response in a {code, message, request_id, data} envelope, so the
    # token lives one level down rather than at the document root.
    accessTokenResponseMap={
        "access_token": "/data/access_token",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    advertiser_ids: list[str] = Field(
        description="Advertiser accounts to capture. Leave empty to capture every account the credentials can access. Required when authenticating with an access token.",
        title="Advertiser IDs",
        default_factory=list,
    )
    custom_reports: str = Field(
        description="A JSON array describing additional reports to sync beyond the prebuilt ones. Reference this connector's documentation for the format: https://go.estuary.dev/source-tiktok-ads-native",
        title="Custom Reports",
        default="",
    )

    @field_validator("advertiser_ids")
    @classmethod
    def dedupe_advertiser_ids(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))

    class Advanced(BaseModel):
        is_sandbox: Annotated[
            bool,
            Field(
                description="Read from a TikTok sandbox ad account instead of production. Sandbox accounts are served from a separate host, hold only mock data, and their access tokens are not valid against production.",
                title="Use Sandbox",
                default=False,
            ),
        ]
        lookback_window_days: Annotated[
            int,
            Field(
                description="Number of days of already-captured report data to re-read on each sync. Raise this to match your longest click attribution window. TikTok allows up to 28 days, and the default of 7 only covers accounts left on TikTok's own 7-day default.",
                title="Lookback Window (days)",
                default=7,
                ge=0,
                le=30,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


# TikTok stamps entity timestamps as "YYYY-MM-DD HH:MM:SS" in UTC, which is neither RFC3339
# nor timezone-aware. Report timestamps use the same shape but are advertiser-local, so they
# are parsed separately against the account's zone.
ENTITY_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_entity_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    return datetime.strptime(value, ENTITY_DATETIME_FORMAT).replace(tzinfo=UTC)


class TikTokResource(BaseDocument, extra="allow"):
    """An entity returned by one of TikTok's paginated `get`/`search` endpoints."""

    # Endpoint contract. `PATH` is relative to the API root; `ITEMS_KEY` names the array
    # inside `data`; `MAX_PAGE_SIZE` is the per-endpoint ceiling, which varies widely.
    NAME: ClassVar[str]
    PATH: ClassVar[str]
    ITEMS_KEY: ClassVar[str] = "list"
    MAX_PAGE_SIZE: ClassVar[int] = 1000
    PRIMARY_KEY: ClassVar[str]

    # Every entity is read per-advertiser, and TikTok does not echo the advertiser back on
    # most of these responses, so it is stamped on from the request scope.
    advertiser_id: str

    @model_validator(mode="before")
    @classmethod
    def stamp_advertiser_id(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data

        context = info.context
        if context and "advertiser_id" in context:
            data.setdefault("advertiser_id", context["advertiser_id"])

        return data

    def get_cursor(self) -> datetime:
        raise NotImplementedError


class TimestampedResource(TikTokResource):
    """An entity carrying TikTok's standard create/modify timestamps."""

    create_time: AwareDatetime
    modify_time: AwareDatetime

    _parse_timestamps = field_validator("create_time", "modify_time", mode="before")(
        parse_entity_datetime
    )

    def get_cursor(self) -> datetime:
        return self.modify_time


class Campaign(TimestampedResource):
    NAME: ClassVar[str] = "campaigns"
    PATH: ClassVar[str] = "campaign/get/"
    PRIMARY_KEY: ClassVar[str] = "/campaign_id"

    campaign_id: str


class AdGroup(TimestampedResource):
    NAME: ClassVar[str] = "ad_groups"
    PATH: ClassVar[str] = "adgroup/get/"
    PRIMARY_KEY: ClassVar[str] = "/adgroup_id"

    adgroup_id: str


class Ad(TimestampedResource):
    NAME: ClassVar[str] = "ads"
    PATH: ClassVar[str] = "ad/get/"
    PRIMARY_KEY: ClassVar[str] = "/ad_id"

    ad_id: str


class Video(TikTokResource):
    NAME: ClassVar[str] = "videos"
    PATH: ClassVar[str] = "file/video/ad/search/"
    MAX_PAGE_SIZE: ClassVar[int] = 100
    PRIMARY_KEY: ClassVar[str] = "/video_id"

    video_id: str
    # Videos expose only a modify time, and TikTok returns at most the 10,000 most recently
    # modified per advertiser, so older assets are unreachable once that many exist.
    modify_time: AwareDatetime

    _parse_timestamps = field_validator("modify_time", mode="before")(
        parse_entity_datetime
    )

    def get_cursor(self) -> datetime:
        return self.modify_time


class Image(TikTokResource):
    NAME: ClassVar[str] = "images"
    PATH: ClassVar[str] = "file/image/ad/search/"
    MAX_PAGE_SIZE: ClassVar[int] = 100
    PRIMARY_KEY: ClassVar[str] = "/image_id"

    image_id: str
    # Unlike the ad-object endpoints, image timestamps already arrive as ISO-8601 with a
    # trailing Z, so they need no reformatting.
    modify_time: AwareDatetime

    def get_cursor(self) -> datetime:
        return self.modify_time


class Location(TikTokResource):
    NAME: ClassVar[str] = "locations"
    PATH: ClassVar[str] = "search/region/"
    ITEMS_KEY: ClassVar[str] = "region_list"
    PRIMARY_KEY: ClassVar[str] = "/region_id"

    region_id: str


class AdvertiserAccount(TikTokResource):
    NAME: ClassVar[str] = "advertisers"
    PATH: ClassVar[str] = "advertiser/info/"
    PRIMARY_KEY: ClassVar[str] = "/advertiser_id"


# --- Reports ---------------------------------------------------------------------------


class ReportType(StrEnum):
    BASIC = "BASIC"
    AUDIENCE = "AUDIENCE"
    # Deprecated upstream in favour of `/gmv_max/report/get/`, but still the only way to
    # reach the GMV spend figures Fivetran exposes.
    TT_SHOP = "TT_SHOP"


class DataLevel(StrEnum):
    AUCTION_AD = "AUCTION_AD"
    AUCTION_ADGROUP = "AUCTION_ADGROUP"
    AUCTION_CAMPAIGN = "AUCTION_CAMPAIGN"
    AUCTION_ADVERTISER = "AUCTION_ADVERTISER"


class Granularity(StrEnum):
    HOURLY = "hourly"
    DAILY = "daily"
    LIFETIME = "lifetime"


# The report level fixes which id dimension the request carries. Deriving it rather than
# accepting it keeps TikTok's "only one ID dimension per request" rule unbreakable.
ID_DIMENSION: dict[DataLevel, str] = {
    DataLevel.AUCTION_AD: "ad_id",
    DataLevel.AUCTION_ADGROUP: "adgroup_id",
    DataLevel.AUCTION_CAMPAIGN: "campaign_id",
    DataLevel.AUCTION_ADVERTISER: "advertiser_id",
}

# Likewise for the single permitted time dimension.
TIME_DIMENSION: dict[Granularity, str | None] = {
    Granularity.HOURLY: "stat_time_hour",
    Granularity.DAILY: "stat_time_day",
    Granularity.LIFETIME: None,
}

# Exactly one of these is required by an audience report, except that `age` and `gender` may
# be requested together.
AUDIENCE_DIMENSIONS = frozenset(
    {
        "ac",
        "additional_interests",
        "ad_type",
        "age",
        "audience_tags",
        "behavior_id",
        "contextual_tag",
        "country_code",
        "device_brand_id",
        "dma_id",
        "gender",
        "hashtag",
        "interest_category",
        "interest_category_tier2",
        "interest_category_tier3",
        "interest_category_tier4",
        "language",
        "placement",
        "platform",
        "province_id",
    }
)

# TikTok serves no time series for these, so they are only valid at lifetime granularity.
TIME_INCOMPATIBLE_DIMENSIONS = frozenset(
    {
        "device_brand_id",
        "interest_category",
        "interest_category_tier2",
        "interest_category_tier3",
        "interest_category_tier4",
    }
)

MAX_METRICS_PER_REQUEST = 100


class Report(BaseModel, extra="forbid"):
    """One configured report stream.

    `dimensions` carries only breakdown dimensions: the id dimension follows from
    `data_level` and the time dimension from `granularity`.
    """

    name: str
    data_level: DataLevel
    granularity: Granularity
    metrics: list[str]
    report_type: ReportType = ReportType.BASIC
    dimensions: list[str] = Field(default_factory=list)

    @field_validator("dimensions", "metrics")
    @classmethod
    def dedupe(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))

    @property
    def time_dimension(self) -> str | None:
        return TIME_DIMENSION[self.granularity]

    @property
    def id_dimension(self) -> str:
        return ID_DIMENSION[self.data_level]

    def all_dimensions(self) -> list[str]:
        dimensions = [self.id_dimension]

        if self.time_dimension is not None:
            dimensions.append(self.time_dimension)

        return dimensions + self.dimensions


class ReportDocument(BaseDocument, extra="allow"):
    pass


def create_report_doc_model(report: Report) -> type[ReportDocument]:
    """Build the document model for a report.

    Every dimension is required, because together with the advertiser they form the key;
    metrics are left to schema inference so a report never has to enumerate them.
    """
    fields: dict[str, Any] = {
        dimension: (str, ...)
        for dimension in report.all_dimensions()
        if dimension != report.time_dimension
    }

    # The time dimension is reformatted from TikTok's zone-less "YYYY-MM-DD HH:MM:SS" into
    # an offset-bearing instant, so it is typed as one.
    if report.time_dimension is not None:
        fields[report.time_dimension] = (AwareDatetime, ...)
    # TikTok omits the advertiser from single-account report rows, so it is stamped on and
    # declared here to keep it part of the key.
    fields["advertiser_id"] = (str, ...)

    return create_model(report.name, __base__=ReportDocument, **fields)
