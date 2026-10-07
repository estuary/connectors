from datetime import datetime, timedelta, UTC
from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field, field_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


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

# Stream document models and resource registries are added by `add-stream`.
