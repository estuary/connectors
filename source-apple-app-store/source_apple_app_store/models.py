from datetime import date, timedelta
from enum import StrEnum
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
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
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic.types import AwareDatetime

# Forward reference to avoid circular imports
if TYPE_CHECKING:
    from .client import AppleAppStoreClient

ConnectorState = GenericConnectorState[ResourceState]


class AppleCredentials(AccessToken):
    credentials_title: Literal["Private App Credentials"] = Field(
        default="Private App Credentials",
        json_schema_extra={"type": "string"},
    )
    access_token: str = Field(
        default="",  # Set to empty string by default because it will be generated dynamically by the patched TokenSource
        json_schema_extra={"secret": True, "hidden": True},
    )
    private_key: str = Field(
        title="Private Key",
        description="The contents of the private key file (.p8) downloaded from App Store Connect. Include the entire contents including BEGIN and END markers.",
        json_schema_extra={"secret": True, "multiline": True},
    )
    key_id: str = Field(
        title="Key ID",
        description="The Key ID shown in App Store Connect when you created the API key.",
        json_schema_extra={"secret": True},
    )
    issuer_id: str = Field(
        title="Issuer ID",
        description="The Issuer ID shown in App Store Connect (found in Users and Access > Keys).",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    """Main endpoint configuration for the Apple App Store Analytics connector."""

    model_config = ConfigDict(extra="forbid")

    credentials: AppleCredentials = Field(
        ...,
        title="Apple App Store Connect Credentials",
        description="JWT authentication credentials for Apple's API",
    )

    app_ids: List[str] = Field(
        default_factory=list,
        title="App IDs",
        description="Specific App IDs to sync. If empty, discovers all accessible apps",
    )


TData = TypeVar("TData")


class ApiResponse(BaseModel, Generic[TData], extra="allow"):
    class Links(BaseModel, extra="allow"):
        self: str = Field(description="URL of the current page")
        next: Optional[str] = Field(
            description="URL of the next page, which includes the cursor"
        )

    class Meta(BaseModel, extra="allow"):
        class Pagination(BaseModel, extra="allow"):
            total: int
            limit: int

        paging: Pagination

    data: TData
    links: Links | None
    meta: Meta | None

    @property
    def cursor(self) -> Optional[str]:
        if self.links is None:
            return None
        elif self.links.next is None:
            return None
        return self.links.next.split("?cursor=")[-1]


class AppleResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    primary_keys: ClassVar[list[str]]


class AppReview(AppleResource):
    name: ClassVar[str] = "app_reviews"
    primary_keys: ClassVar[list[str]] = ["/app_id", "/id"]

    # Ensures that the model validates assignment of the app_id
    # field which is not returned in the API response, but is required
    # for the resource key and set manually in the connector
    model_config = ConfigDict(validate_assignment=True)

    class Attributes(BaseModel, extra="allow"):
        createdDate: AwareDatetime

    id: str
    app_id: str = Field(
        default="DEFAULT",
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    attributes: Attributes


class BaseValidationContext:
    def __init__(self, app_id: str, **kwargs):
        self.app_id = app_id
        self.count = 0

    def increment(self):
        self.count += 1


class AppleAnalyticsRow(BaseDocument):
    model_config = ConfigDict(
        serialize_by_alias=True,
        extra="allow",
        validate_assignment=True,  # necessary since we set app_id after model creation
    )

    report_name: ClassVar[str]
    resource_name: ClassVar[str]
    completeness_lag: ClassVar[timedelta] = timedelta(days=2)
    primary_keys: ClassVar[list[str]]
    validation_context_model: ClassVar[type[BaseValidationContext]] = (
        BaseValidationContext
    )

    app_id: str
    record_date: date = Field(..., alias="Date")
    row_number: int = Field(description="Row sequence number for deduplication")

    @model_validator(mode="before")
    @classmethod
    def _add_row_metadata(
        cls, data: Dict[str, Any], info: ValidationInfo
    ) -> Dict[str, Any]:
        if not info.context or not isinstance(info.context, BaseValidationContext):
            raise RuntimeError(
                f"Validation context is not set or is not of type BaseValidationContext: {info.context}"
            )

        assert "row_number" not in data, (
            "Row number should not be set before validation."
        )
        data["row_number"] = info.context.count
        info.context.increment()

        return data

    @field_validator("*", mode="before")
    @classmethod
    def parse_by_field_type(cls, value: Any, info):
        field_types = getattr(cls, "_FIELD_TYPE_MAP", {})
        field_alias = info.alias or info.field_name
        field_type = field_types.get(field_alias)

        if value == "":
            return None
        if field_type is int:
            try:
                return int(value)
            except Exception as e:
                raise ValueError(f"Invalid int value for {field_alias}: {value}") from e
        if field_type is date:
            try:
                if isinstance(value, str) and value.isdigit():
                    return date.fromtimestamp(int(value))
                return date.fromisoformat(value)
            except Exception as e:
                raise ValueError(
                    f"Invalid date value for {field_alias}: {value}"
                ) from e
        return value


class AnalyticsReportAccessType(StrEnum):
    ONGOING = "ONGOING"
    ONE_TIME_SNAPSHOT = "ONE_TIME_SNAPSHOT"


class AnalyticsReportGranularity(StrEnum):
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class AnalyticsReportRequest(BaseModel, extra="allow"):
    class Attributes(BaseModel, extra="allow"):
        accessType: AnalyticsReportAccessType
        stoppingDueToInactivity: Optional[bool] = False

    id: str
    attributes: Attributes


class AnalyticsReport(BaseModel, extra="allow"):
    class AnalyticsReportAttributes(BaseModel, extra="allow"):
        name: str

    id: str
    attributes: AnalyticsReportAttributes


class AnalyticsReportInstance(BaseModel, extra="allow"):
    class Attributes(BaseModel, extra="allow"):
        granularity: AnalyticsReportGranularity
        processingDate: date

    id: str
    attributes: Attributes


class AnalyticsReportSegment(BaseModel, extra="allow"):
    class Attributes(BaseModel, extra="allow"):
        url: str

    id: str
    attributes: Attributes


class AppSessionsReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-sessions
    """

    report_name: ClassVar[str] = "App Sessions"
    resource_name: ClassVar[str] = "app_sessions"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)
    primary_keys: ClassVar[list[str]] = [
        "/app_id",
        "/App Apple Identifier",
        "/Date",
        "/App Version",
        "/Device",
        "/row_number",
    ]

    app_apple_identifier: str = Field(..., alias="App Apple Identifier")
    app_version: str = Field(..., alias="App Version")
    device: str = Field(..., alias="Device")
    total_session_duration: int = Field(..., alias="Total Session Duration", ge=0)


class AppCrashesReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-crashes
    """

    report_name: ClassVar[str] = "App Crashes"
    resource_name: ClassVar[str] = "app_crashes"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)
    primary_keys: ClassVar[list[str]] = [
        "/app_id",
        "/App Apple Identifier",
        "/Date",
        "/App Version",
        "/Device",
        "/Platform Version",
        "/row_number",
    ]

    app_apple_identifier: str = Field(..., alias="App Apple Identifier")
    app_version: str = Field(..., alias="App Version")
    device: str = Field(..., alias="Device")
    platform_version: str = Field(..., alias="Platform Version")
    total_session_duration: int = Field(..., alias="Total Session Duration", ge=0)


class AppDownloadsReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-download
    """

    report_name: ClassVar[str] = "App Store Downloads"
    resource_name: ClassVar[str] = "app_downloads"
    completeness_lag: ClassVar[timedelta] = timedelta(days=2)
    primary_keys: ClassVar[list[str]] = [
        "/app_id",
        "/App Apple Identifier",
        "/Date",
        "/App Version",
        "/Device",
        "/Platform Version",
        "/row_number",
    ]

    app_apple_identifier: str = Field(..., alias="App Apple Identifier")
    app_version: str = Field(..., alias="App Version")
    device: str = Field(..., alias="Device")
    platform_version: str = Field(..., alias="Platform Version")
    total_session_duration: int = Field(..., alias="Total Session Duration", ge=0)


class AppStoreInstallsReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-installs
    """

    report_name: ClassVar[str] = "App Store Installations and Deletions"
    resource_name: ClassVar[str] = "app_store_installs_and_deletions"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)
    primary_keys: ClassVar[list[str]] = [
        "/app_id",
        "/App Apple Identifier",
        "/Date",
        "/App Version",
        "/Platform Version",
        "/Event Type",
        "/row_number",
    ]

    app_apple_identifier: str = Field(..., alias="App Apple Identifier")
    app_version: str = Field(..., alias="App Version")
    device: str = Field(..., alias="Device")
    platform_version: str = Field(..., alias="Platform Version")
    event_type: str = Field(..., alias="Event Type")
    total_session_duration: int = Field(..., alias="Total Session Duration", ge=0)


class AppStoreDiscoveryAndEngagementReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-store-discovery-and-engagement
    """

    report_name: ClassVar[str] = "App Store Discovery and Engagement"
    resource_name: ClassVar[str] = "app_store_discovery_and_engagement"
    completeness_lag: ClassVar[timedelta] = timedelta(days=3)
    primary_keys: ClassVar[list[str]] = [
        "/app_id",
        "/App Apple Identifier",
        "/Date",
        "/Platform Version",
        "/Event Type",
        "/row_number",
    ]

    app_apple_identifier: str = Field(..., alias="App Apple Identifier")
    device: str = Field(..., alias="Device")
    platform_version: str = Field(..., alias="Platform Version")
    event_type: str = Field(..., alias="Event Type")
    total_session_duration: int = Field(..., alias="Total Session Duration", ge=0)


# Type definitions for API resource functions
ApiFetchChangesFn = Callable[
    ["AppleAppStoreClient", str, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]

ApiFetchPageFn = Callable[
    ["AppleAppStoreClient", str, Logger, Optional[PageCursor], LogCursor],
    AsyncGenerator[BaseDocument | PageCursor, None],
]
