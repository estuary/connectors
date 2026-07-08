from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Literal

from estuary_cdk.capture.common import (
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
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
    pagination bound. `Sheet.from_detail` / `Report.from_detail` dump this
    wholesale into the metadata streams' output."""

    totalRowCount: int


class SortedAttachmentOptionsMixin(BaseDocument):
    """Shared by `Sheet` and `Report`: Smartsheet returns
    `effectiveAttachmentOptions` in non-deterministic order across
    otherwise-identical requests, so it's sorted for stable, comparable output.

    Validator-only (`check_fields=False`): subclasses declare the field
    themselves, keeping it after `id` in field -- and thus output -- order.
    """

    @field_validator("effectiveAttachmentOptions", check_fields=False)
    @classmethod
    def _sort_effective_attachment_options(
        cls, v: list[str] | None
    ) -> list[str] | None:
        return sorted(v) if v is not None else v


# =============================================================================
# Sheets cluster: `sheets` (catalog metadata) and `sheet_rows` (row data)
# =============================================================================


class RawRow(BaseModel, extra="allow"):
    modifiedAt: AwareDatetime


class SheetSummary(BaseModel, extra="allow"):
    id: int
    modifiedAt: AwareDatetime


class Sheet(SortedAttachmentOptionsMixin, BaseDocument):
    """One document per sheet: catalog/metadata only, never row data."""

    resource_name: ClassVar[str] = "sheets"

    id: int
    effectiveAttachmentOptions: list[str] | None = None

    @classmethod
    def from_detail(cls, meta: DetailPageMeta) -> "Sheet":
        # The remainder still carries the streamed-out `rows` key (as an
        # empty array) -- drop it rather than emit it.
        return cls(**meta.model_dump(exclude={"rows"}))


@dataclass(frozen=True, slots=True)
class SheetIdValidationContext:
    sheet_id: int


class SheetScopedMixin(BaseDocument):
    """Mixin for documents scoped to a Smartsheet sheet.

    Extends _meta with sheet_id and injects it from validation context.
    """

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

    @model_validator(mode="before")
    @classmethod
    def _inject_sheet_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, SheetIdValidationContext):
            meta = data.get("_meta") or {}
            meta["sheet_id"] = info.context.sheet_id
            data["_meta"] = meta
        return data


class SheetRow(SheetScopedMixin, BaseDocument):
    resource_name: ClassVar[str] = "sheet_rows"

    id: int


# =============================================================================
# Reports cluster: `reports` (catalog metadata) and `report_rows` (row data)
#
# Both streams are Snapshot replication -- unlike the Sheets cluster's
# `modifiedSince`/`rowsModifiedSince` cursors, `GET /reports`' own
# `modifiedSince` filter reflects the report *definition's* modified time
# (rename/recolumn/create), not the underlying sheet data a report renders,
# and there is no `GET /reports/{id}/version`-equivalent to fall back on.
# =============================================================================


class ReportSummary(BaseModel, extra="allow"):
    """One `GET /reports` list-page item. Only `id` is needed -- report
    metadata is refetched from the detail endpoint (cheaply, `pageSize=1`),
    not trusted from the list summary."""

    id: int


class Report(SortedAttachmentOptionsMixin, BaseDocument):
    """One document per report: catalog/metadata only, never row data"""

    resource_name: ClassVar[str] = "reports"

    id: int
    effectiveAttachmentOptions: list[str] | None = None

    @classmethod
    def from_detail(cls, meta: DetailPageMeta) -> "Report":
        # The remainder still carries the streamed-out `rows` key (as an
        # empty array); `columns[]` is report-definition detail that doesn't
        # belong on the metadata document -- drop both.
        return cls(**meta.model_dump(exclude={"columns", "rows"}))


@dataclass(frozen=True, slots=True)
class ReportIdValidationContext:
    """Validation context carrying the report_id for ReportRow construction."""

    report_id: int


class ReportScopedMixin(BaseDocument):
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

    @model_validator(mode="before")
    @classmethod
    def _inject_report_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, ReportIdValidationContext):
            meta = data.get("_meta") or {}
            meta["report_id"] = info.context.report_id
            data["_meta"] = meta
        return data


class ReportRow(ReportScopedMixin, BaseDocument):
    resource_name: ClassVar[str] = "report_rows"

    id: int
