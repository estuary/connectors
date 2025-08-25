import csv
import os
from datetime import datetime, date, timedelta
from enum import StrEnum
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
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
        json_schema_extra={"secret": True, "x-hidden-field": True},
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

    app_ids: list[str] = Field(
        default_factory=list,
        title="App IDs",
        description="Specific App IDs to sync. If empty, discovers all accessible apps",
    )


TData = TypeVar("TData")


class ApiResponse(BaseModel, Generic[TData], extra="allow"):
    class Links(BaseModel, extra="allow"):
        self: str
        next: str | None = None

    class Meta(BaseModel, extra="allow"):
        class Pagination(BaseModel, extra="allow"):
            total: int
            limit: int

        paging: Pagination

    data: TData
    links: Links | None = None
    meta: Meta | None = None

    @property
    def cursor(self) -> str | None:
        if self.links is None:
            return None
        elif self.links.next is None:
            return None
        return self.links.next.split("?cursor=")[-1]


class AppleResourceValidationContext:
    def __init__(self, app_id: str, **kwargs):
        self.app_id = app_id


class AppleResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    primary_keys: ClassVar[list[str]]
    app_id: str
    validation_context_model: ClassVar[type[AppleResourceValidationContext]] = (
        AppleResourceValidationContext
    )

    @model_validator(mode="before")
    @classmethod
    def _add_app_id(cls, data: dict[str, Any], info: ValidationInfo) -> Any:
        if not info.context or not isinstance(
            info.context, AppleResourceValidationContext
        ):
            raise RuntimeError(
                f"Validation context is not set or is not of type AppleResourceValidationContext: {info.context}"
            )

        assert "app_id" not in data, "App id should not be set before validation."
        data["app_id"] = info.context.app_id
        return data


class AppReview(AppleResource):
    name: ClassVar[str] = "app_reviews"
    primary_keys: ClassVar[list[str]] = ["/app_id", "/id"]

    class Attributes(BaseModel, extra="allow"):
        createdDate: AwareDatetime

    id: str
    attributes: Attributes


class BaseValidationContext:
    def __init__(self, filename: str, **kwargs):
        self.filename = filename
        self.count = 0

    def increment(self):
        self.count += 1


class AppleAnalyticsRow(BaseDocument):
    model_config = ConfigDict(
        serialize_by_alias=True,
        extra="allow",
        validate_assignment=True,  # necessary since we set app_id after model creation
    )

    _field_types: ClassVar[dict[str, type]] = {
        "record_date": datetime,
    }
    report_name: ClassVar[str]
    resource_name: ClassVar[str]
    completeness_lag: ClassVar[timedelta] = timedelta(days=2)
    primary_keys: ClassVar[list[str]] = [
        "/filename",
        "/row_number",
    ]
    validation_context_model: ClassVar[type[BaseValidationContext]] = (
        BaseValidationContext
    )

    record_date: date = Field(..., alias="Date")
    filename: str = Field(default="")
    row_number: int = Field(description="Row sequence number for deduplication")

    @model_validator(mode="before")
    @classmethod
    def _add_row_metadata(
        cls, data: dict[str, Any], info: ValidationInfo
    ) -> dict[str, Any]:
        if not info.context or not isinstance(info.context, BaseValidationContext):
            raise RuntimeError(
                f"Validation context is not set or is not of type BaseValidationContext: {info.context}"
            )

        assert "row_number" not in data, (
            "Row number should not be set before validation."
        )
        assert "filename" not in data, "Filename should not be set before validation."

        data["filename"] = info.context.filename
        data["row_number"] = info.context.count
        info.context.increment()

        return data

    @field_validator("*", mode="before")
    @classmethod
    def parse_by_field_type(cls, value: Any, info):
        field_name = info.field_name
        field_type = cls._field_types.get(field_name)

        if value == "":
            return None

        if field_type is int:
            try:
                return int(value)
            except Exception as e:
                raise ValueError(f"Invalid int value for {field_name}: {value}") from e
        elif field_type is date:
            try:
                if isinstance(value, str) and value.isdigit():
                    return date.fromtimestamp(int(value))
                return date.fromisoformat(value)
            except Exception as e:
                raise ValueError(f"Invalid date value for {field_name}: {value}") from e
        elif field_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                if value.lower() in ("true", "1"):
                    return True
                elif value.lower() in ("false", "0"):
                    return False
            raise ValueError(f"Invalid bool value for {field_name}: {value}")
        else:
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
        stoppedDueToInactivity: bool | None = False

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
    filename: str = Field(default="")

    @model_validator(mode="after")
    def extract_filename_from_url(self):
        from urllib.parse import urlparse

        url = self.attributes.url
        parsed_url = urlparse(url)

        # Extract filename from the path, removing query parameters
        path = parsed_url.path
        filename = path.lstrip("/") if path else ""

        self.filename = filename
        return self


class AppSessionsDetailedReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-sessions
    """

    report_name: ClassVar[str] = "App Sessions Detailed"
    resource_name: ClassVar[str] = "app_sessions_detailed"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)


class AppCrashesReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-crashes
    """

    report_name: ClassVar[str] = "App Crashes"
    resource_name: ClassVar[str] = "app_crashes"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)


class AppDownloadsDetailedReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-download
    """

    report_name: ClassVar[str] = "App Downloads Detailed"
    resource_name: ClassVar[str] = "app_downloads_detailed"
    completeness_lag: ClassVar[timedelta] = timedelta(days=2)


class AppStoreInstallsDetailedReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-installs
    """

    report_name: ClassVar[str] = "App Store Installation and Deletion Detailed"
    resource_name: ClassVar[str] = "app_store_installs_and_deletions_detailed"
    completeness_lag: ClassVar[timedelta] = timedelta(days=5)


class AppStoreDiscoveryAndEngagementDetailedReportRow(AppleAnalyticsRow):
    """
    References:
        - https://developer.apple.com/documentation/analytics-reports/app-store-discovery-and-engagement
    """

    report_name: ClassVar[str] = "App Store Discovery and Engagement Detailed"
    resource_name: ClassVar[str] = "app_store_discovery_and_engagement_detailed"
    completeness_lag: ClassVar[timedelta] = timedelta(days=3)


# Type definitions for API resource functions
ApiFetchChangesFn = Callable[
    ["AppleAppStoreClient", str, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]

ApiFetchPageFn = Callable[
    ["AppleAppStoreClient", str, Logger, PageCursor | None, LogCursor],
    AsyncGenerator[BaseDocument | PageCursor, None],
]
