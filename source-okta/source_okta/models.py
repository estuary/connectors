import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal, Self, override
from urllib.parse import parse_qs, urlparse

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import LogCursor, ResourceState
from estuary_cdk.capture.common import ResourceConfig as GenericResourceConfig
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, field_validator

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# A bare, lowercased host name.
HOST_PATTERN = re.compile(r"[a-z0-9.-]+")


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


class EndpointConfig(BaseModel):
    domain: str = Field(
        title="Okta Domain",
        description=(
            "The Okta org domain the Management API is served from, without a scheme. "
            "Examples: dev-123456.okta.com, acme.okta-emea.com, acme.oktapreview.com, "
            "or a custom domain such as login.acme.com."
        ),
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Users, groups, and "
            "system logs updated before this date will not be captured. If left blank, "
            "all available data will be captured."
        ),
        title="Start Date",
        default=_EPOCH,
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    @field_validator("domain", mode="before")
    @classmethod
    def normalize_domain(cls, value: object) -> object:
        """Reduce a pasted domain to the bare host every request is built on."""
        if not isinstance(value, str):
            return value
        host = (
            value.strip()
            .lower()
            .removeprefix("https://")
            .removeprefix("http://")
            .rstrip("/")
        )
        if not HOST_PATTERN.fullmatch(host):
            raise ValueError(
                f"Okta domain must be a bare host such as dev-123456.okta.com, got {value!r}"
            )
        return host


class ResourceConfig(GenericResourceConfig):
    # The CDK's `PT0S` default is below the minimum interval.
    interval: timedelta = Field(
        default=timedelta(minutes=5),
        description="Interval between updates for this resource",
        json_schema_extra={"nonsensitive": True},
    )


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
    # Okta rejects anything above 1000 with a 400 (System Log / limit over max).
    PAGE_SIZE: ClassVar[int] = 1000

    uuid: str
    published: AwareDatetime


@dataclass(frozen=True, order=True)
class AfterToken:
    """A System Log `after` token, ordered by the log position it resumes from.

    Okta documents the token as opaque; its observed form is
    `<persistence epoch-ms>_<n>`, resuming past the first `n` events persisted
    from that ms on. Field order is log order only while those `n` events all
    sit at `epoch_ms`: true of every token held here, since Okta issues its own
    at the last returned event's ms and `from_timestamp` mints `n = 0`.
    """

    epoch_ms: int
    n: int

    @classmethod
    def from_str(cls, token: str) -> Self:
        epoch_ms, n = token.split("_")
        parsed = cls(int(epoch_ms), int(n))
        if str(parsed) != token:
            raise ValueError(f"unrecognized System Log `after` token: {token!r}")
        return parsed

    @classmethod
    def from_url(cls, url: str) -> Self:
        """The `after` token of a Link URL, such as a polling next link."""
        values = parse_qs(urlparse(url).query).get(SystemLogEvent.AFTER_PARAM)
        if not values:
            raise RuntimeError(f"Okta next link carries no `after` parameter: {url}")
        return cls.from_str(values[0])

    @classmethod
    def from_timestamp(cls, at: datetime) -> Self:
        """The token for the first event persisted at or after `at`.

        `n = 0` selects the same events as an inclusive `since`.
        """
        return cls((at - _EPOCH) // timedelta(milliseconds=1), 0)

    @classmethod
    def from_cursor(cls, log_cursor: LogCursor) -> Self:
        match log_cursor:
            case (str() as token,):
                return cls.from_str(token)
            case _:
                raise RuntimeError(
                    f"expected a one-element System Log `after` cursor, got {log_cursor!r}"
                )

    def to_cursor(self) -> LogCursor:
        return (str(self),)

    @property
    def timestamp(self) -> datetime:
        """The persistence instant the token resumes from."""
        return _EPOCH + timedelta(milliseconds=self.epoch_ms)

    @override
    def __str__(self) -> str:
        return f"{self.epoch_ms}_{self.n}"
