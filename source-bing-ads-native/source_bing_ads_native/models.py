from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING, Annotated, Any
from urllib import parse

from pydantic import AwareDatetime, BaseModel, Field, ValidationInfo, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    RotatingOAuth2Credentials,
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
        "access_token": "/access_token",
        "access_token_expires_at": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
    # Sent on the connector's own refresh-token exchanges, so a refreshed
    # access token keeps the Microsoft Advertising scope.
    additionalTokenExchangeBody={"scope": SCOPE},
)


# Microsoft returns a new refresh token with every access token, and users of
# the Airbyte-based connector have had to sign in again about every 90 days.
# Rotating credentials save each new refresh token back to the config, so
# the stored one never gets that old. The cost: the CDK only rotates at startup, so the task
# restarts each time its hour-long access token expires.
class BingAdsOAuth2Credentials(RotatingOAuth2Credentials):
    # Estuary's developer token, like the OAuth app's client ID and secret,
    # is Estuary's rather than the user's, so Flow adds it to the credentials
    # from the connector's `oauth2_injected_values` instead of asking for it.
    developer_token: str = Field(
        min_length=1,
        title="Developer Token",
        description="Injected by Flow.",
        json_schema_extra={"secret": True},
    )


if TYPE_CHECKING:
    OAuth2Credentials = BingAdsOAuth2Credentials
else:
    OAuth2Credentials = BingAdsOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


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


class Ad(StampedDocument):
    Id: str
    AccountId: str
    CampaignId: str
    AdGroupId: str


# Performance Max campaigns have asset groups instead of ad groups and ads.
class AssetGroup(StampedDocument):
    Id: str
    AccountId: str
    CampaignId: str
