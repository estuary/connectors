from datetime import UTC, datetime
from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import ClientCredentialsOAuth2Credentials
from estuary_cdk.incremental_csv_processor import BaseCSVRow


class BaseURL(StrEnum):
    """Zuora REST API base URLs, one per data center and environment.
    https://developer.zuora.com/api-references/api/overview/#section/Introduction/Access-to-the-API
    """
    US_PRODUCTION = "https://rest.zuora.com"
    US_API_SANDBOX = "https://rest.apisandbox.zuora.com"
    US_CENTRAL_SANDBOX = "https://rest.test.zuora.com"
    US_PERFORMANCE_TEST = "https://rest.pt1.zuora.com"
    US_CLOUD_PRODUCTION = "https://rest.na.zuora.com"
    US_CLOUD_API_SANDBOX = "https://rest.sandbox.na.zuora.com"
    EU_PRODUCTION = "https://rest.eu.zuora.com"
    EU_API_SANDBOX = "https://rest.sandbox.eu.zuora.com"
    EU_CENTRAL_SANDBOX = "https://rest.test.eu.zuora.com"
    APAC_PRODUCTION = "https://rest.ap.zuora.com"
    APAC_CENTRAL_SANDBOX = "https://rest.test.ap.zuora.com"


class EndpointConfig(BaseModel):
    credentials: ClientCredentialsOAuth2Credentials
    base_url: BaseURL = Field(
        title="API Base URL",
        description="Zuora REST API base URL for your tenant's data center and environment.",
        default=BaseURL.US_PRODUCTION,
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description=(
            "UTC date and time from which to start replicating data. "
            "Defaults to January 1, 2000, which captures all available history."
        ),
        default_factory=lambda: datetime(2000, 1, 1, tzinfo=UTC),
    )


ConnectorState = GenericConnectorState[ResourceState]


class ZuoraDocument(BaseCSVRow):
    Id: str
    UpdatedDate: AwareDatetime


class ExportStatus(StrEnum):
    # https://developer.zuora.com/v1-api-reference/older-api/operation/Object_POSTExport/
    PENDING = "Pending"
    PROCESSING = "Processing"
    COMPLETED = "Completed"
    CANCELED = "Canceled"
    FAILED = "Failed"


class ExportError(BaseModel, extra="allow"):
    Code: str | None = None
    Message: str | None = None


class ExportSubmitResponse(BaseModel, extra="allow"):
    """Response from POST /v1/object/export."""
    Success: bool = False
    Id: str | None = None
    Errors: list[ExportError] = Field(default_factory=list)


class ExportStatusResponse(BaseModel, extra="allow"):
    """Response from GET /v1/object/export/{id}."""
    Status: ExportStatus
    FileId: str | None = None
    StatusReason: str | None = None


class DescribeField(BaseModel, extra="allow"):
    """A <field> entry within a GET /v1/describe/{object} response."""
    name: str
    selectable: bool = False
    contexts: list[str] = Field(default_factory=list)

    @property
    def is_exportable(self) -> bool:
        # A selectable field can still be unavailable in the export ZOQL context.
        # Only fields whose contexts include "export" work in an export query,
        # otherwise the export job fails with "There is no field named X".
        # https://docs.zuora.com/en/zuora-platform/data/legacy-query-methods/export-zoql/changes-to-the-describe-api
        return self.selectable and "export" in self.contexts


class DescribeObject(BaseModel, extra="allow"):
    """A GET /v1/describe/{object} response."""
    name: str = ""
    fields: list[DescribeField] = Field(default_factory=list)

    @property
    def exportable_field_names(self) -> list[str]:
        return [f.name for f in self.fields if f.is_exportable]


class CatalogObject(BaseModel, extra="allow"):
    name: str


class DescribeCatalog(BaseModel, extra="allow"):
    """A GET /v1/describe response."""
    objects: list[CatalogObject] = Field(default_factory=list)
