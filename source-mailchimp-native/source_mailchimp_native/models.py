import abc
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, Literal

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceState,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import (
    BasicAuth,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
)
from pydantic import AwareDatetime, BaseModel, Field
from pydantic.json_schema import SkipJsonSchema

# Mailchimp's OAuth flow returns a single non-expiring access token and no
# refresh token, so credentials take the long-lived client-credentials shape.
# The spec mirrors the legacy connector's Estuary-registered OAuth app for
# provider "mailchimp" — keep the provider name in sync with that registration.
OAUTH2_SPEC = OAuth2Spec(
    provider="mailchimp",
    authUrlTemplate=(
        "https://login.mailchimp.com/oauth2/authorize?response_type=code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://login.mailchimp.com/oauth2/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={"access_token": "/access_token"},
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


class ApiKey(BasicAuth):
    """Mailchimp API keys authenticate over HTTP Basic with the key as the
    password; the username is ignored by the API."""

    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string"},
    )
    # Mailchimp ignores the Basic-auth username, so it stays out of the config
    # schema entirely; TokenSource still reads it when building the header.
    username: SkipJsonSchema[str] = ""
    password: str = Field(
        title="API Key",
        description=(
            "Your Mailchimp API key, including the data center suffix "
            "(e.g. ends in -us21)."
        ),
        json_schema_extra={"secret": True},
        alias="api_key",
    )


class OAuthMetadata(BaseModel):
    """Response of Mailchimp's OAuth metadata endpoint, which reports the
    account's data-center-specific API endpoint."""

    api_endpoint: str


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: OAuth2Credentials | ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class MailchimpEntity(BaseDocument, abc.ABC, extra="allow"):
    """Base for documents fetched from Mailchimp collection endpoints.

    Concrete entities pin their stream identity: `NAME` is the stream name,
    `PATH` is the URL path under the account's base URL, and `ITEMS_KEY` is
    the envelope key the items array lives under (usually but not always the
    path's last segment)."""

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    ITEMS_KEY: ClassVar[str]


class MailchimpList(MailchimpEntity):
    NAME: ClassVar[str] = "lists"
    PATH: ClassVar[str] = "lists"
    ITEMS_KEY: ClassVar[str] = "lists"

    id: str


class Campaign(MailchimpEntity):
    NAME: ClassVar[str] = "campaigns"
    PATH: ClassVar[str] = "campaigns"
    ITEMS_KEY: ClassVar[str] = "campaigns"

    id: str
    create_time: AwareDatetime


class Automation(MailchimpEntity):
    """Classic automation workflow. The endpoint is feature-gated and empty on
    the test account, so this shape is docs-based; the required `id` fails
    loudly if a real payload disagrees."""

    NAME: ClassVar[str] = "automations"
    PATH: ClassVar[str] = "automations"
    ITEMS_KEY: ClassVar[str] = "automations"

    id: str


class CollectionMeta(BaseModel, extra="allow"):
    """Envelope remainder shared by Mailchimp's collection endpoints.

    `total_items` trails the items array in live responses and drives
    pagination completion, so it must stay required — schema drift should fail
    loudly rather than end pagination early."""

    total_items: int


# Streams whose records mutate after creation but expose only created-time
# filters: a daily scheduled backfill is the only mechanism that recovers
# updates (deletions are still not captured).
SCHEDULED_BACKFILL_STREAMS = [
    "campaigns",
]
