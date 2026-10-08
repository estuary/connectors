import re
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, field_validator


class ApiToken(AccessToken):
    """An Okta API token, sent as `Authorization: SSWS <token>`.

    Created in the Admin Console under Security > API > Tokens. The token
    inherits the admin role of the account that created it and is revoked by
    Okta after 30 days without use.
    """

    credentials_title: Literal["API Token"] = Field(
        default="API Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Token",
        json_schema_extra={"secret": True, "order": 1},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    domain: str = Field(
        title="Okta Domain",
        description=(
            "The Okta org domain the Management API is served from, without a scheme. "
            "Examples: dev-123456.okta.com, acme.okta-emea.com, acme.oktapreview.com, "
            "or a custom domain such as login.acme.com."
        ),
        pattern=r"^[A-Za-z0-9.-]+$",
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. users and groups are "
            "backfilled from the objects last updated after this instant, and system_log "
            "from the events published at or after it; applications and "
            "application_users are always captured in full. If left blank, the start "
            "date will be set to 30 days before the present."
        ),
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    @field_validator("domain", mode="before")
    @classmethod
    def normalize_domain(cls, value: object) -> object:
        """Reduce a pasted domain to the bare host every request is built on.

        A scheme, trailing slash, surrounding whitespace and upper case are
        tolerated and stripped; a path or an empty value is rejected here with
        a readable message rather than by the field pattern.
        """
        if not isinstance(value, str):
            return value
        host = (
            value.strip()
            .lower()
            .removeprefix("https://")
            .removeprefix("http://")
            .rstrip("/")
        )
        if not host or "/" in host or any(c.isspace() for c in host):
            raise ValueError(
                f"Okta domain must be a bare host such as dev-123456.okta.com, got {value!r}"
            )
        return host


ConnectorState = GenericConnectorState[ResourceState]


class OktaApplication(BaseDocument, extra="allow"):
    """One app integration, ACTIVE or INACTIVE; deleted apps are never listed."""

    PATH: ClassVar[str] = "apps"
    # Documented maximum; larger values are silently clamped to 200.
    PAGE_SIZE: ClassVar[int] = 200

    id: str


class SystemLogEvent(BaseDocument, extra="allow"):
    """One System Log event, the atomic audit fact Okta records.

    Only the collection key and the backfill watermark are modelled; every
    other field rides schema inference. Events are immutable once published.
    """

    PATH: ClassVar[str] = "logs"
    SINCE_PARAM: ClassVar[str] = "since"
    UNTIL_PARAM: ClassVar[str] = "until"
    AFTER_PARAM: ClassVar[str] = "after"
    # The `after` token is a position, `<persistence epoch-ms>_<n>`, never a
    # session handle (`System Log / polling resume after`).
    AFTER_TOKEN: ClassVar[re.Pattern[str]] = re.compile(r"\d+_\d+")
    # Okta rejects anything above 1000 with a 400 (System Log / limit over max).
    PAGE_SIZE: ClassVar[int] = 1000

    uuid: str
    published: AwareDatetime

    def get_cursor(self) -> AwareDatetime:
        return self.published
