from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING, Annotated, Any
from urllib import parse

from pydantic import AwareDatetime, BaseModel, Field, ValidationInfo, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    BaseOAuth2Credentials,
    ResourceState,
    OAuth2Spec,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Always the multi-tenant `common` endpoint: on a multi-tenant app it accepts every
# work, school and personal Microsoft account that a specific tenant would.
# https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-identity-platform?view=bingads-13
MICROSOFT_IDENTITY_URL = "https://login.microsoftonline.com/common/oauth2/v2.0"

# `offline_access` is what makes Microsoft return a refresh token.
# https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-consent?view=bingads-13
SCOPE = "https://ads.microsoft.com/msads.manage offline_access"

# The sandbox rejects tokens for SCOPE with code 105 and only accepts tokens for
# its own resource. Microsoft's sandbox guide doesn't say so; Microsoft's Python
# SDK requests this scope for the sandbox (`MSA_PROD`).
# https://github.com/BingAds/BingAds-Python-SDK/blob/main/bingads/authorization.py
SANDBOX_SCOPE = "https://si.ads.microsoft.com/msads.manage offline_access"


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"


OAUTH2_SPEC = OAuth2Spec(
    provider="microsoft",
    authUrlTemplate=(
        f"{MICROSOFT_IDENTITY_URL}/authorize?"
        f"client_id={urlencode_field('client_id')}&"
        f"response_type=code&"
        f"response_mode=query&"
        f"redirect_uri={urlencode_field('redirect_uri')}&"
        f"scope={parse.quote(SCOPE)}&"
        f"state={urlencode_field('state')}"
    ),
    accessTokenUrlTemplate=f"{MICROSOFT_IDENTITY_URL}/token",
    accessTokenHeaders={
        "content-type": "application/x-www-form-urlencoded",
    },
    accessTokenBody=(
        "grant_type=authorization_code"
        f"&client_id={urlencode_field('client_id')}"
        f"&client_secret={urlencode_field('client_secret')}"
        f"&redirect_uri={urlencode_field('redirect_uri')}"
        f"&code={urlencode_field('code')}"
        f"&scope={parse.quote(SCOPE)}"
    ),
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
    },
    # Sent on the connector's own refresh-token exchanges, so a refreshed
    # access token keeps the Microsoft Advertising scope.
    additionalTokenExchangeBody={"scope": SCOPE},
)


# Access tokens last about an hour, so they're refreshed mid-run from the
# configured refresh token, which Microsoft keeps valid after issuing a new
# one: Airbyte's connector and source-onedrive refresh this way too.
# RotatingOAuth2Credentials would refresh only at startup, crashing the task
# each time an access token expired.
if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


DEFAULT_START_DATE_DAYS_AGO = 30
DEFAULT_LOOKBACK_WINDOW_DAYS = 30
MAX_LOOKBACK_WINDOW_DAYS = 90


def utc_now() -> datetime:
    """The current time. Tests patch it to pin "now"."""
    return datetime.now(tz=UTC)


def default_start_date():
    return utc_now() - timedelta(days=DEFAULT_START_DATE_DAYS_AGO)


# The numeric Account ID the API takes, not the 8-character account number the UI
# shows most prominently. The API types AccountId as a long integer.
AccountId = Annotated[str, Field(pattern=r"^[0-9]+$")]


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    developer_token: str = Field(
        min_length=1,
        title="Developer Token",
        description="The Microsoft Advertising developer token sent with every API request.",
        json_schema_extra={"secret": True},
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    account_ids: list[AccountId] = Field(
        default=[],
        title="Account IDs",
        description="The numeric Account IDs to capture, not the 8-character account numbers. If left blank, every account the authenticated user can access is captured.",
    )

    class Advanced(BaseModel):
        lookback_window_days: int = Field(
            default=DEFAULT_LOOKBACK_WINDOW_DAYS,
            title="Lookback Window Days",
            description="Number of days of report data to re-fetch on each sync, to pick up late conversions and invalid-traffic credits.",
            ge=0,
            le=MAX_LOOKBACK_WINDOW_DAYS,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


@dataclass(frozen=True)
class ParentIds:
    """IDs of a document's parent entities, which Microsoft's response omits.

    For example, `Ads/QueryByAdGroupId` returns ads without their account,
    campaign or ad group IDs. Passing `ParentIds({"AdGroupId": ...})` as the
    pydantic validation context adds those IDs to each ad as top-level
    fields, so every ad records which account, campaign and ad group it
    belongs to."""

    ids: dict[str, str]


class StampedDocument(BaseDocument, extra="allow"):
    @model_validator(mode="before")
    @classmethod
    def _stamp_parent_ids(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(info.context, ParentIds) and isinstance(data, dict):
            return {**data, **info.context.ids}
        return data


# Entity IDs are longs, which the REST API sends as JSON strings.
class Account(BaseDocument, extra="allow"):
    Id: str
    Name: str
    ParentCustomerId: str


class Campaign(StampedDocument):
    Id: str
    AccountId: str
    CampaignType: str


class AdGroup(StampedDocument):
    Id: str
    AccountId: str
    CampaignId: str


def _drop_asset_performance_labels(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: _drop_asset_performance_labels(v)
            for k, v in value.items()
            if k != "AssetPerformanceLabel"
        }
    if isinstance(value, list):
        return [_drop_asset_performance_labels(v) for v in value]
    return value


class Ad(StampedDocument):
    Id: str
    AccountId: str
    CampaignId: str
    AdGroupId: str

    # An asset's performance label is recomputed by Microsoft without anyone
    # editing the ad, so keeping it would rewrite every ad on most snapshots.
    @model_validator(mode="before")
    @classmethod
    def _drop_volatile_fields(cls, data: Any) -> Any:
        return _drop_asset_performance_labels(data)


# Performance Max campaigns have asset groups instead of ad groups and ads.
class AssetGroup(StampedDocument):
    Id: str
    AccountId: str
    CampaignId: str

    # Same reason as Ad: asset links carry performance labels.
    @model_validator(mode="before")
    @classmethod
    def _drop_volatile_fields(cls, data: Any) -> Any:
        return _drop_asset_performance_labels(data)
