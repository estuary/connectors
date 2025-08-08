from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import (
    Annotated,
    AsyncGenerator,
    Callable,
    ClassVar,
    Generic,
    Literal,
    TypeVar,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    LogCursor,
    PageCursor,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel, Field

ConnectorState = GenericConnectorState[ResourceState]

API_KEY_HEADER = "DD-API-KEY"
APP_KEY_HEADER = "DD-APPLICATION-KEY"


class Auth(AccessToken):
    credentials_title: Literal["Private App Credentials"] = Field(
        default="Private App Credentials", json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True},
    )
    application_key: str = Field(
        title="Application Key",
        json_schema_extra={"secret": True},
    )


def default_start_date() -> datetime:
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    credentials: Auth = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    site: Literal[
        "datadoghq.com",
        "us3.datadoghq.com",
        "us5.datadoghq.com",
        "datadoghq.eu",
        "ddog-gov.com",
        "ap1.datadoghq.com",
    ] = Field(
        title="Site",
        description="The main Datadog site, designating a region or organization.",
        default="datadoghq.com",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any Datadog data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date.",
        title="Start Date",
        default_factory=default_start_date,
        le=datetime.now(tz=UTC),
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            int,
            Field(
                description="Window size in days for incremental streams. The default of 30 days is recommended for most use cases.",
                title="Window Size",
                default=30,
                gt=0,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @property
    def common_headers(self) -> dict[str, str]:
        return {
            API_KEY_HEADER: self.credentials.access_token,
            APP_KEY_HEADER: self.credentials.application_key,
            "Content-Type": "application/json",
        }

    @property
    def base_url(self) -> str:
        return f"https://api.{self.site}/api/v2"


TApiResponse = TypeVar("TApiResponse")


class Meta(BaseModel, extra="allow"):
    class Page(BaseModel, extra="allow"):
        after: str

    class Warning(BaseModel, extra="allow"):
        code: str
        detail: str
        title: str

    page: Page | None = None
    status: Literal["done", "timeout"]
    warnings: list[Warning] | None = None


class ApiResponse(BaseModel, Generic[TApiResponse]):
    data: TApiResponse | None = None
    meta: Meta

    @property
    def cursor(self) -> str | None:
        if self.meta.page:
            return self.meta.page.after

        return None


class DatadogResource(BaseDocument, extra="allow"):
    RESOURCE_NAME: ClassVar[str]
    PRIMARY_KEYS: ClassVar[list[str]]

    id: str


class IncrementalResource(DatadogResource):
    class Attributes(BaseModel, extra="allow"):
        timestamp: AwareDatetime

    attributes: Attributes


class RealUserMonitoringResource(IncrementalResource):
    RESOURCE_NAME = "real_user_monitoring"
    PRIMARY_KEYS = ["/id"]


class RealUserMonitoringEventSearchRequest(BaseModel, extra="allow"):
    class Filter(BaseModel):
        query: str = Field(default="*")
        start: AwareDatetime = Field(serialization_alias="from")
        end: AwareDatetime | str = Field(default="now", serialization_alias="to")

    class Page(BaseModel, extra="allow"):
        limit: int = Field(default=1000)
        cursor: str | None = Field(default=None)

    page: Page = Field(default_factory=Page)
    filter: Filter


IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, str, dict[str, str], int, Logger, LogCursor],
    AsyncGenerator[IncrementalResource | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [
        HTTPSession,
        str,
        dict[str, str],
        datetime,
        Logger,
        PageCursor,
        LogCursor,
    ],
    AsyncGenerator[IncrementalResource | PageCursor, None],
]
