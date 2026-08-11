import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import AwareDatetime, BaseModel, Field, ValidationInfo, model_validator

from estuary_cdk.capture.common import (
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import ClientCredentialsOAuth2Credentials
from estuary_cdk.incremental_csv_processor import BaseCSVRow


ZUORA_FOUNDING_DATE = datetime(2007, 1, 1, tzinfo=UTC)


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
        title="Base URL",
        description="Zuora REST API base URL for your Zuora data center and environment.",
        default=BaseURL.US_PRODUCTION,
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description=(
            "UTC date and time from which to start replicating data. "
            "Defaults to Zuora's founding year, January 1, 2007."
        ),
        default_factory=lambda: ZUORA_FOUNDING_DATE,
    )


ConnectorState = GenericConnectorState[ResourceState]


class UnknownZuoraTypeError(Exception):
    """Zuora described a field with a type this connector does not recognize.

    Raised rather than degrading the field to an untyped string: a string declaration
    would claim the connector knows the field's shape when it does not, and would strip
    whatever format inference had established for that column. Discovery fails instead,
    naming the field, so the type can be classified deliberately.
    """


class ZuoraType(StrEnum):
    """A field's declared type, as `<type>` in a GET /v1/describe/{object} response."""
    TEXT = "text"
    PICKLIST = "picklist"
    LONGTEXT = "longtext"
    ZOQL = "ZOQL"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    DECIMAL = "decimal"
    NUMBER = "number"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"

    @classmethod
    def parse(cls, raw: str | None, field_name: str) -> "ZuoraType":
        """Classify a describe `<type>`, or fail naming the field it came from."""
        try:
            return cls(raw)
        except ValueError:
            raise UnknownZuoraTypeError(
                f"{field_name}: Zuora declared the type {raw!r}, which this connector "
                f"does not recognize. It must be added to ZuoraType and "
                f"ZUORA_TYPE_SCHEMAS before this object can be captured."
            ) from None


# Zuora's declared type -> the JSON schema a SourcedSchema declares for that field.
ZUORA_TYPE_SCHEMAS: dict[ZuoraType, dict[str, str]] = {
    ZuoraType.TEXT: {"type": "string"},
    ZuoraType.PICKLIST: {"type": "string"},
    ZuoraType.LONGTEXT: {"type": "string"},
    ZuoraType.ZOQL: {"type": "string"},
    ZuoraType.BOOLEAN: {"type": "boolean"},
    ZuoraType.INTEGER: {"type": "string", "format": "integer"},
    ZuoraType.DECIMAL: {"type": "string", "format": "number"},
    ZuoraType.NUMBER: {"type": "string", "format": "number"},
    ZuoraType.DATE: {"type": "string", "format": "date"},
    ZuoraType.DATETIME: {"type": "string", "format": "date-time"},
    ZuoraType.TIMESTAMP: {"type": "string"},
}

# Make sure that every ZuoraType has an entry in ZUORA_TYPE_SCHEMAS.
assert ZUORA_TYPE_SCHEMAS.keys() == set(ZuoraType), (
    f"every ZuoraType needs a schema; missing {set(ZuoraType) - ZUORA_TYPE_SCHEMAS.keys()}"
)

# Types whose values the connector rewrites before emitting them in
# order to align with the emitted sourced schemas. Every member
# here needs a branch in ZuoraRow.transform_cells.
CONVERTED_TYPES: frozenset[ZuoraType] = frozenset(
    {
        ZuoraType.BOOLEAN,
        ZuoraType.DATETIME,
    }
)


def sourced_schema(field_types: dict[str, ZuoraType]) -> dict[str, object]:
    """Build a SourcedSchema value from an object's field name -> Zuora type map."""
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            name: ZUORA_TYPE_SCHEMAS[zuora_type]
            for name, zuora_type in field_types.items()
        },
    }


_BOOLEAN_TOKENS: dict[str, bool] = {"true": True, "false": False}


# Already-compliant: a colon-separated offset, or Z.
_RFC3339_DATETIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
# What AQuA actually emits. Almost RFC3339 compliant, but the offset's colon is missing.
_BASIC_OFFSET_DATETIME = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2})(\d{2})$"
)


def normalize_datetime(field_name: str, value: str) -> str:
    """Rewrite an export's datetime cell as RFC3339.

    AQuA renders datetimes with an ISO 8601 basic offset -- `+0000`, no colon -- which
    RFC3339 rejects. Inserting the colon is the whole transformation. Values that
    already comply pass through, so this is idempotent.
    """
    if _RFC3339_DATETIME.match(value):
        return value
    basic = _BASIC_OFFSET_DATETIME.match(value)
    if basic is None:
        raise ValueError(
            f"{field_name}: expected an RFC3339-convertible datetime, got {value!r}. "
            f"Zuora's describe types this field as datetime. Reach out to Estuary "
            f"support to update this connector to convert this datetime format to "
            f"be RFC3339 compliant."
        )
    stamp, offset_hours, offset_minutes = basic.groups()
    return f"{stamp}{offset_hours}:{offset_minutes}"


def parse_boolean(field_name: str, value: str) -> bool:
    """Convert an export's boolean cell to a real bool."""
    parsed = _BOOLEAN_TOKENS.get(value.strip().lower())
    if parsed is None:
        raise ValueError(
            f"{field_name}: expected a boolean, got {value!r}. Zuora's describe types "
            f"this field as boolean. Reach out to Estuary support for help resolving "
            f"this error."
        )
    return parsed


@dataclass(frozen=True)
class ValidationContext:
    """What a row needs to know about its object in order to validate."""
    object_name: str
    field_types: dict[str, ZuoraType]


def _column_to_field(column: str, object_name: str) -> str:
    """Map an export CSV column header to the field name documents carry.

    With useQueryLabels every header is "<Object>.<Field>", for both the exported
    object's own columns and any joined related object's. An own column drops the
    prefix so it matches its describe name ("Account.Id" of an Account export ->
    "Id"), while a joined column keeps it as a flattened name ("Account.Id" of an
    Invoice export -> "AccountId").
    """
    prefix, _, field = column.partition(".")
    if not field:
        return column
    return field if prefix == object_name else f"{prefix}{field}"


class ZuoraRow(BaseCSVRow):
    """Every exported row, incremental or snapshot.

    Renames each export column to the name documents carry, and rewrites the cells whose
    declared type the raw CSV text does not satisfy: a boolean column holds "true", and a
    datetime column holds an offset RFC3339 rejects.
    """

    @model_validator(mode="before")
    @classmethod
    def transform_cells(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(info.context, ValidationContext):
            # Every caller has the object's types to hand, and a row validated without
            # them would keep its raw cells and contradict the schema the binding
            # declares. Fail rather than convert nothing.
            raise RuntimeError(
                f"Implementation error: {cls.__name__} must be validated with a "
                f"ValidationContext, got {info.context!r}, so its cells would not "
                f"be converted."
            )
        if not isinstance(data, dict):
            return data

        # Rename first: field_types is keyed by the name the document carries, not the
        # header the export sent.
        converted = {
            _column_to_field(column, info.context.object_name): value
            for column, value in data.items()
        }
        field_types = info.context.field_types
        for name, zuora_type in field_types.items():
            if zuora_type not in CONVERTED_TYPES:
                continue
            value = converted.get(name)
            # This validator runs ahead of BaseCSVRow's null handling -- pydantic runs a
            # subclass's before-validator first -- so a raw export cell is still "" here.
            # None turns up only when re-validating a document that has already been
            # through it. Neither has anything to convert.
            if value is None or value == "":
                continue
            if not isinstance(value, str):
                continue  # already converted, e.g. a re-validated document
            if zuora_type == ZuoraType.BOOLEAN:
                converted[name] = parse_boolean(name, value)
            elif zuora_type == ZuoraType.DATETIME:
                converted[name] = normalize_datetime(name, value)
            else:
                raise RuntimeError(
                    f"Implementation error: '{zuora_type}' is in CONVERTED_TYPES but "
                    f"has no converter, so {name} would be declared as a type its "
                    f"value does not satisfy."
                )
        return converted


class ZuoraDocument(ZuoraRow):
    """Base for objects captured incrementally off a single date cursor.
    """
    CURSOR_FIELD: ClassVar[str]
    Id: str

    def get_cursor(self) -> AwareDatetime:
        raise NotImplementedError


class UpdatedDateDocument(ZuoraDocument):
    CURSOR_FIELD: ClassVar[str] = "UpdatedDate"
    UpdatedDate: AwareDatetime

    def get_cursor(self) -> AwareDatetime:
        return self.UpdatedDate


class UpdatedOnDocument(ZuoraDocument):
    """AchNocEventLog names its timestamps UpdatedOn/CreatedOn rather than
    UpdatedDate/CreatedDate, and exports no UpdatedDate at all.
    """
    CURSOR_FIELD: ClassVar[str] = "UpdatedOn"
    UpdatedOn: AwareDatetime

    def get_cursor(self) -> AwareDatetime:
        return self.UpdatedOn


class TransactionDateDocument(ZuoraDocument):
    """Append-only transaction logs (PaymentTransactionLog,
    PaymentMethodTransactionLog, ...) have no UpdatedDate. Their only date field
    is TransactionDate. Their rows are written once at transaction time and never
    mutated, so this creation-style timestamp is a valid incremental cursor.
    """
    CURSOR_FIELD: ClassVar[str] = "TransactionDate"
    TransactionDate: AwareDatetime

    def get_cursor(self) -> AwareDatetime:
        return self.TransactionDate


class AquaJobStatus(StrEnum):
    # https://developer.zuora.com/v1-api-reference/api/operation/GET_BatchQueryJob/
    SUBMITTED = "submitted"
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    ERROR = "error"
    ABORTED = "aborted"
    CANCELLED = "cancelled"
    # failed is undocumented, but it seems like it's a possible status since
    # the legacy Export API had it.
    FAILED = "failed"


class AquaBatch(BaseModel, extra="allow"):
    """One entry in an AQuA job's batches list, corresponding to one submitted
    query. When the tenant has AQuA file segmentation enabled, a large result
    arrives as multiple files listed in segments instead of a single fileId.
    """
    status: str | None = None
    fileId: str | None = None
    segments: list[str] | None = None
    recordCount: int | None = None
    message: str | None = None


class AquaJobResponse(BaseModel, extra="allow"):
    """Response from POST /v1/batch-query/ and GET /v1/batch-query/jobs/{id}.

    A submission rejected at validation time reports the problem in the
    top-level message field rather than an HTTP error status, so status and id
    are optional to keep such responses parseable.
    """
    id: str | None = None
    status: AquaJobStatus | None = None
    message: str | None = None
    batches: list[AquaBatch] = Field(default_factory=list)


class DescribeField(BaseModel, extra="allow"):
    """A <field> entry within a GET /v1/describe/{object} response."""
    name: str
    selectable: bool = False
    contexts: list[str] = Field(default_factory=list)
    # Zuora's declared type, e.g. text/decimal/datetime/boolean. Absent in
    # hand-written test fixtures, so optional.
    type: str | None = None

    @property
    def is_exportable(self) -> bool:
        # A selectable field can still be unavailable in the export ZOQL context.
        # Only fields whose contexts include "export" work in an export query,
        # otherwise the export job fails with "There is no field named X".
        # https://docs.zuora.com/en/zuora-platform/data/legacy-query-methods/export-zoql/changes-to-the-describe-api
        return self.selectable and "export" in self.contexts


# Related objects that must never be joined, even though describe advertises them.
UNJOINABLE_OBJECTS: frozenset[str] = frozenset({"SubscriptionStatusHistory"})


class DescribeObject(BaseModel, extra="allow"):
    """A GET /v1/describe/{object} response."""
    name: str = ""
    fields: list[DescribeField] = Field(default_factory=list)
    related_object_names: list[str] = Field(default_factory=list)

    @property
    def exportable_field_names(self) -> list[str]:
        return [f.name for f in self.fields if f.is_exportable]

    @property
    def joinable_object_names(self) -> list[str]:
        """The related objects whose Id this object's export can select.

        Three kinds of relationship are dropped:
        1. a self-reference, because `<Self>.Id` would duplicate the object's
           own Id column.
        2. the UNJOINABLE_OBJECTS that fail the export job outright.
        3.  any relationship the object already exposes as a scalar `<Name>Id` field.
        """
        own_field_names = set(self.exportable_field_names)
        return [
            name
            for name in self.related_object_names
            if name != self.name
            and name not in UNJOINABLE_OBJECTS
            and f"{name}Id" not in own_field_names
        ]

    @property
    def query_field_names(self) -> list[str]:
        """Every field an export of this object selects. Its own exportable
        fields, then `<RelatedObject>.Id` for each joinable related object.

        Export ZOQL exposes an object's foreign keys only as joins on the
        related object rather than as scalar `<Related>Id` fields, and describe
        lists those relationships in `<related-objects>` instead of `<fields>`.
        Selecting only `<fields>` therefore yields rows with no way to reference
        what they belong to: an InvoiceItem with no InvoiceId, a RatePlan with
        no SubscriptionId.
        """
        return self.exportable_field_names + [
            f"{name}.Id" for name in self.joinable_object_names
        ]

    @property
    def query_field_types(self) -> dict[str, ZuoraType]:
        """Zuora's declared type for every column an export selects, keyed by the name
        the *document* carries rather than the name the query selects: a joined
        `<Related>.Id` arrives as `<Related>Id` (see _column_to_field).

        This is where raw describe strings become ZuoraType, so nothing downstream has
        to cope with an unrecognized one. Joined columns are always Zuora ids, hence
        text; describe says nothing about them because they are relationships rather
        than fields.
        """
        types = {
            f.name: ZuoraType.parse(f.type, f.name)
            for f in self.fields
            if f.is_exportable
        }
        for related in self.joinable_object_names:
            types[f"{related}Id"] = ZuoraType.TEXT
        return types


class CatalogObject(BaseModel, extra="allow"):
    name: str


class DescribeCatalog(BaseModel, extra="allow"):
    """A GET /v1/describe response."""
    objects: list[CatalogObject] = Field(default_factory=list)
