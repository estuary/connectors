from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Generic, TypeVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, PrivateAttr

from .constants import CursorField, RateGroup, Scope


# See README.md for more information about Authentication
class JWTCredentials(AccessToken):
    access_token: str = Field(
        default="",  # Dynamically set by token source
        json_schema_extra={"secret": True, "x-hidden-field": True},
    )
    client_id: str = Field(
        title="Client ID",
        description="Client ID from your RingCentral JWT Auth app.",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        description="Client Secret from your RingCentral JWT Auth app.",
        json_schema_extra={"secret": True},
    )
    jwt: str = Field(
        title="JWT Credential",
        description=(
            "JWT credential from your RingCentral Developer Console. "
            "Create one at https://developers.ringcentral.com/console/my-credentials "
            "and authorize it for your JWT Auth app."
        ),
        json_schema_extra={"secret": True},
    )
    # This is used to validate Scopes that are granted match what the connector expects
    # for enaabled streams
    _scopes: set[str] = PrivateAttr(default=set())

    def set_scopes(self, scopes: set[str]) -> None:
        self._scopes = scopes

    def get_scopes(self) -> set[str]:
        return self._scopes


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=7)


class EndpointConfig(BaseModel):
    credentials: JWTCredentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. Defaults to 7 days ago.",
        title="Start Date",
        default_factory=default_start_date,
    )

    class Advanced(BaseModel):
        # We set the max value <= 7-days since most call logs would typically be under 24-hours.
        # If we find later that this needs to be configurable past the 7-day limit, we can change this.
        lookback_window: int = Field(
            description=(
                "Number of hours to look back when fetching call logs during incremental sync. "
                "Call log records can be updated after creation, so the connector re-fetches "
                "records within this window once per day to capture updates. Default is 24 hours."
            ),
            title="Call Log Lookback Window (Hours)",
            default=24,
            ge=1,
            le=168,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class RingCentralResource(BaseDocument, extra="allow"):
    NAME: ClassVar[str]
    KEY: ClassVar[str]
    ENDPOINT: ClassVar[str]
    RATE_GROUP: ClassVar[RateGroup]
    REQUIRED_SCOPES: ClassVar[set[Scope]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Skip validation for abstract intermediate classes
        if cls.__name__ in ("FullRefreshResource", "IncrementalResource"):
            return
        for attr in ("NAME", "KEY", "RATE_GROUP", "REQUIRED_SCOPES"):
            if not hasattr(cls, attr):
                raise TypeError(f"{cls.__name__} must define {attr}")


class FullRefreshResource(RingCentralResource):
    KEY: ClassVar[str] = "/_meta/row_id"


class IncrementalResource(RingCentralResource):
    KEY: ClassVar[str] = "/id"
    CURSOR_FIELD: ClassVar[CursorField]
    REQUIRES_LOOKBACK: ClassVar[bool] = False

    id: str
    lastModifiedTime: AwareDatetime | None = None

    @property
    def cursor_value(self) -> AwareDatetime:
        raise NotImplementedError


class Extension(FullRefreshResource):
    """Extension (user, department, etc.) from the Extensions API."""

    NAME: ClassVar[str] = "extensions"
    ENDPOINT: ClassVar[str] = "/account/~/extension"
    RATE_GROUP: ClassVar[RateGroup] = RateGroup.MEDIUM
    REQUIRED_SCOPES: ClassVar[set[Scope]] = {Scope.READ_ACCOUNTS}


class InternalContact(FullRefreshResource):
    """Company directory entry from the Directory Entries API (internal contacts).

    Represents users, departments, announcements, and other extension types
    in the corporate directory.
    """

    NAME: ClassVar[str] = "internal_contacts"
    ENDPOINT: ClassVar[str] = "/account/~/directory/entries"
    RATE_GROUP: ClassVar[RateGroup] = RateGroup.MEDIUM
    REQUIRED_SCOPES: ClassVar[set[Scope]] = {Scope.READ_ACCOUNTS}


class ExternalContact(FullRefreshResource):
    NAME: ClassVar[str] = "external_contacts"
    ENDPOINT: ClassVar[str] = "/account/~/extension/~/address-book/contact"
    RATE_GROUP: ClassVar[RateGroup] = RateGroup.LIGHT
    REQUIRED_SCOPES: ClassVar[set[Scope]] = {Scope.READ_CONTACTS}


class UserCallLog(IncrementalResource):
    NAME: ClassVar[str] = "user_call_log"
    ENDPOINT: ClassVar[str] = "/account/~/extension/~/call-log"
    CURSOR_FIELD: ClassVar[CursorField] = CursorField.START_TIME
    REQUIRES_LOOKBACK: ClassVar[bool] = True
    RATE_GROUP: ClassVar[RateGroup] = RateGroup.HEAVY
    REQUIRED_SCOPES: ClassVar[set[Scope]] = {Scope.READ_CALL_LOG}

    startTime: AwareDatetime

    @property
    def cursor_value(self) -> AwareDatetime:
        return self.startTime


class Message(IncrementalResource):
    NAME: ClassVar[str] = "messages"
    ENDPOINT: ClassVar[str] = "/account/~/extension/~/message-store"
    CURSOR_FIELD: ClassVar[CursorField] = CursorField.CREATION_TIME
    RATE_GROUP: ClassVar[RateGroup] = RateGroup.LIGHT
    REQUIRED_SCOPES: ClassVar[set[Scope]] = {Scope.READ_MESSAGES}

    creationTime: AwareDatetime

    @property
    def cursor_value(self) -> AwareDatetime:
        return self.creationTime


TResource = TypeVar("TResource", bound=RingCentralResource)


class Page(BaseModel, extra="allow"):
    uri: str


class Navigation(BaseModel, extra="allow"):
    nextPage: Page | None = None


class PaginatedResponse(BaseModel, Generic[TResource], extra="allow"):
    records: list[TResource] = Field(default_factory=list)
    navigation: Navigation | None = None
