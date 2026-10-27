from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING, Annotated
from urllib import parse

from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
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


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


# The numeric Account ID the API takes, not the 8-character account number the UI
# shows most prominently. The API types AccountId as a long integer.
AccountId = Annotated[str, Field(pattern=r"^[0-9]+$")]


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    developer_token: str = Field(
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
        lookback_days: int = Field(
            default=30,
            title="Lookback Days",
            description="Number of days of report data to re-fetch on each sync, to pick up late conversions and invalid-traffic credits.",
            ge=0,
            le=90,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]

# Stream document models and resource registries are added by `add-stream`.
