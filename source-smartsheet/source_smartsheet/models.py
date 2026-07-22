from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal, override

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationInfo,
    field_validator,
    model_validator,
)


class ApiAccessToken(AccessToken):
    credentials_title: Literal["API Access Token"] = Field(
        default="API Access Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Access Token",
        description="Smartsheet API Access Token, generated under Account > Apps & Integrations > API Access.",
        json_schema_extra={"secret": True, "order": 1},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    region: Literal["us", "eu", "gov", "au"] = Field(
        title="Region",
        description="The Smartsheet data center your account is hosted in. Determines the API base URL used to reach your account.",
        default="us",
    )
    credentials: ApiAccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]

# Keys mirror EndpointConfig.region's Literal values.
REGION_BASE_URLS = {
    "us": "https://api.smartsheet.com/2.0",
    "eu": "https://api.smartsheet.eu/2.0",
    "gov": "https://api.smartsheetgov.com/2.0",
    "au": "https://api.smartsheet.au/2.0",
}


def base_url(region: str) -> str:
    return REGION_BASE_URLS[region]


# =============================================================================
# Shared API envelopes -- the sheets and reports endpoint families return one
# list-page and one detail-page shape.
# =============================================================================


class ListPageMeta(BaseModel, extra="allow"):
    """Remainder of a `GET /sheets` / `GET /reports` list page once the
    `data[]` items have been streamed out.

    `pageNumber >= totalPages` is a safe completion signal for both list
    endpoints (unlike the detail endpoints, whose pagination clamps silently
    with no such fields): the sheets list echoes the page actually served
    even past the last one, and the reports list reports `pageNumber` `0`
    when there are zero results and `1` once there's at least one."""

    pageNumber: int
    totalPages: int


class DetailPageMeta(BaseModel, extra="allow"):
    """Remainder of a `GET /sheets/{id}` / `GET /reports/{id}` detail page
    once the `rows[]` items have been streamed out: the entity's full
    metadata envelope, plus `totalRowCount` -- the row walks' client-side
    pagination bound. `Sheet.from_detail` dumps this wholesale into the sheets
    metadata stream's output."""

    totalRowCount: int


class RowValidationContext(ABC):
    __slots__: tuple[str, ...] = ()

    @abstractmethod
    def inject(self, meta: dict[str, JsonValue]) -> None:
        """Write this context's parent id into a row's `_meta` mapping."""
        ...


class RowScopedMixin(BaseDocument):
    """Base for detail-row documents scoped to a parent entity (a sheet or a
    report). Applies whatever `RowValidationContext` is supplied at validation
    time; concrete subclasses declare the specific `_meta` id field the context
    writes into.
    """

    @model_validator(mode="before")
    @classmethod
    def _inject_id_from_context(
        cls, data: JsonValue, info: ValidationInfo
    ) -> JsonValue:
        if not isinstance(data, dict):
            return data
        if isinstance(info.context, RowValidationContext):
            meta = data.get("_meta") or {}
            info.context.inject(meta)
            data["_meta"] = meta
        return data


# =============================================================================
# Sheets cluster: `sheets` (catalog metadata) and `sheet_rows` (row data)
# =============================================================================


class SheetSummary(BaseModel, extra="allow"):
    id: int
    modifiedAt: AwareDatetime

    @staticmethod
    def build_url(region: str) -> str:
        return f"{base_url(region)}/sheets"


class Sheet(BaseDocument):
    """One document per sheet: catalog/metadata only, never row data."""

    resource_name: ClassVar[str] = "sheets"

    id: int
    effectiveAttachmentOptions: list[str] | None = None

    @field_validator("effectiveAttachmentOptions")
    @classmethod
    def _sort_effective_attachment_options(
        cls, v: list[str] | None
    ) -> list[str] | None:
        # Option ordering is non-deterministic and can cause snapshot tests to
        # fail. Sort for stable, comparable output.
        return sorted(v) if v is not None else v

    @classmethod
    def from_detail(cls, meta: DetailPageMeta) -> "Sheet":
        # The remainder still carries the streamed-out `rows` key (as an
        # empty array) -- drop it rather than emit it.
        return cls(**meta.model_dump(exclude={"rows"}))


@dataclass(frozen=True, slots=True)
class SheetIdValidationContext(RowValidationContext):
    sheet_id: int

    @override
    def inject(self, meta: dict[str, JsonValue]) -> None:
        meta["sheet_id"] = self.sheet_id


class SheetScopedMixin(RowScopedMixin):
    """Documents scoped to a Smartsheet sheet."""

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)
        sheet_id: int = Field(
            default=-1,
            description="The Smartsheet sheet this row belongs to",
        )

    meta_: Meta = Field(
        default_factory=Meta,
        alias="_meta",
        description="Document metadata",
    )


class SheetRow(SheetScopedMixin, BaseDocument):
    resource_name: ClassVar[str] = "sheet_rows"

    id: int
    modifiedAt: AwareDatetime

    @staticmethod
    def build_url(region: str, sheet_id: int) -> str:
        return f"{base_url(region)}/sheets/{sheet_id}"


# =============================================================================
# Reports cluster: `reports` (catalog metadata) and `report_rows` (row data)
#
# Both streams are Snapshot replication -- unlike the Sheets cluster's
# `modifiedSince`/`rowsModifiedSince` cursors, `GET /reports`' own
# `modifiedSince` filter reflects the report *definition's* modified time
# (rename/recolumn/create), not the underlying sheet data a report renders,
# and there is no `GET /reports/{id}/version`-equivalent to fall back on.
# =============================================================================


class Report(BaseDocument):
    """One document per report, mirroring a `GET /reports` list item."""

    resource_name: ClassVar[str] = "reports"

    id: int

    @staticmethod
    def build_url(region: str) -> str:
        return f"{base_url(region)}/reports"


@dataclass(frozen=True, slots=True)
class ReportIdValidationContext(RowValidationContext):
    report_id: int

    @override
    def inject(self, meta: dict[str, JsonValue]) -> None:
        meta["report_id"] = self.report_id


class ReportScopedMixin(RowScopedMixin):
    """Documents scoped to a Smartsheet report."""

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)
        report_id: int = Field(
            default=-1,
            description="The Smartsheet report this row belongs to",
        )

    meta_: Meta = Field(
        default_factory=Meta,
        alias="_meta",
        description="Document metadata",
    )


class ReportRow(ReportScopedMixin, BaseDocument):
    resource_name: ClassVar[str] = "report_rows"

    id: int

    @staticmethod
    def build_url(region: str, report_id: int) -> str:
        return f"{base_url(region)}/reports/{report_id}"
