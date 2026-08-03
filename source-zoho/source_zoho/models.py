from __future__ import annotations

import urllib.parse
from abc import ABCMeta
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Self

from estuary_cdk.capture.common import (
    CRON_REGEX,
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    BaseOAuth2Credentials,
    OAuth2Spec,
)
from estuary_cdk.incremental_csv_processor import BaseCSVRow
from pydantic import (
    AliasChoices,
    AwareDatetime,
    BaseModel,
    Field,
    ValidationInfo,
    model_validator,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


REQUIRED_SCOPES = [
    "ZohoCRM.bulk.READ",  # For Bulk Read API (backfills)
    "ZohoCRM.coql.READ",  # For COQL queries (incremental)
    "ZohoCRM.modules.READ",  # For reading module data
    "ZohoCRM.settings.modules.READ",  # For reading module metadata
    "ZohoCRM.settings.fields.READ",  # For field metadata
]
ALWAYS_REQUIRED_DOCUMENT_FIELDS = {"id", "Modified_Time"}

UNSUPPORTED_MODULES = {
    # Modules that don't support Bulk API (per Zoho docs)
    "Notes",
    "Attachments",
    "Emails",
    # Modules that get listed as available but aren't actually supported.
    # Attempting to query the following will return a `INVALID_MODULE` error.
    "CTI_Entry",
    # Modules that get listed as available but aren't actually supported.
    # Attempting to query the following will return a `INVALID_QUERY` error.
    "Email_Analytics",
    "Email_Template_Analytics",
    "Functions__s",
    # Modules whose required OAuth scopes are undocumented and unclear.
    "Email_Sentiment",
    "Locking_Information__s",
}
UNSUPPORTED_GENERATED_TYPES = {"linking"}
UNSUPPORTED_FIELD_DATA_TYPES = {"fileupload", "multiselectlookup"}

COLORED_TAG_DATA_TYPE_FIELDS = {"id", "name", "color_code"}


OAUTH2_SPEC = OAuth2Spec(
    provider="zoho",
    authUrlTemplate=(
        "https://accounts.zoho.com/oauth/v2/auth?"
        + r"response_type=code"
        + r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        + r"&scope="
        + urllib.parse.quote(",".join(REQUIRED_SCOPES))
        + r"&access_type=offline"
        + r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        + r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://accounts.zoho.com/oauth/v2/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        + r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        + r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        + r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        + r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
        "api_domain": "/api_domain",
    },
)


class ZohoOAuth2Credentials(BaseOAuth2Credentials, metaclass=ABCMeta):
    api_domain: str = Field(
        title="API Domain",
        description="Automatically detected from OAuth response",
    )


if TYPE_CHECKING:
    OAuth2Credentials = ZohoOAuth2Credentials
else:
    OAuth2Credentials = ZohoOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


class ZohoResourceConfigWithSchedule(ResourceConfigWithSchedule):
    schedule: str = Field(
        default="",
        title="Formula Field Refresh Schedule",
        description="Schedule to automatically refresh formula fields. Accepts a cron expression.",
        pattern=CRON_REGEX,
    )


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials = Field(
        title="Authentication",
        json_schema_extra={"order": 0},
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. "
            "Any data generated before this date will not be replicated. "
            "If left blank, the start date will be set to 30 days before the present."
        ),
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
        json_schema_extra={"order": 1},
    )


ConnectorState = GenericConnectorState[ResourceState]


class BulkJobState(StrEnum):
    ADDED = "ADDED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN PROGRESS"
    COMPLETED = "COMPLETED"


class BulkJobResult(BaseModel, extra="allow"):
    download_path: str = Field(alias="download_url")
    count: int
    more_records: bool
    next_page_token: str | None = None


class BulkJobDetails(BaseModel, extra="allow"):
    id: str
    state: BulkJobState
    result: BulkJobResult | None = None


class BulkJobDetailsResponse(BaseModel, extra="allow"):
    data: list[BulkJobDetails]


class BulkJobCreateDetails(BaseModel, extra="allow"):
    id: str


class BulkJobCreateResponseData(BaseModel, extra="allow"):
    status: str
    code: str
    message: str
    details: BulkJobCreateDetails


class BulkJobCreateResponse(BaseModel, extra="allow"):
    data: list[BulkJobCreateResponseData]


class ModuleListing(BaseModel, extra="allow"):
    module_name: str
    api_name: str
    api_supported: bool
    generated_type: str | None = (
        None  # "default", "linking", "subform", "web", "custom"
    )


class ModuleFieldListing(BaseModel, extra="allow"):
    api_name: str
    data_type: str
    json_type: str

    @property
    def is_formula_field(self) -> bool:
        return self.data_type == "formula"

    @property
    def is_api_supported(self) -> bool:
        return self.data_type not in UNSUPPORTED_FIELD_DATA_TYPES


class FieldTypeFilter(StrEnum):
    ALL = auto()
    FORMULA = auto()


class ZohoModule(BaseCSVRow, extra="allow"):
    api_name: ClassVar[str]
    generated_type: ClassVar[
        str | None
    ]  # "default", "linking", "subform", "web", "custom"
    Modified_Time: AwareDatetime

    # Responses might capitalise the `Id` field even if the API only accepts
    # lowercase `id`s
    id: int = Field(validation_alias=AliasChoices("id", "Id"))

    # Field metadata as described by the API. This is a subset of the actual set of queryable
    # fields, which can only be discovered by using the bulk API.
    field_metadata: ClassVar[dict[str, ModuleFieldListing]]

    # We are deliberately not collecting all queryable fields as the undocumented ones are
    # not user facing and dynamically listing them would be too costly
    # all_field_names: list[str]

    @classmethod
    def _normalize_bulk_api_field(cls, name: str, value: Any) -> Any:
        if name not in cls.field_metadata.keys():
            return value

        json_type = cls.field_metadata[name].json_type
        data_type = cls.field_metadata[name].data_type

        if json_type == "boolean" and data_type == "boolean":
            assert isinstance(value, str), "Malformed boolean value found"
            return value.lower() == "true"

        return value

    @classmethod
    def _normalize_coql_field(cls, name: str, value: Any) -> Any:
        if name not in cls.field_metadata.keys():
            return value

        json_type = cls.field_metadata[name].json_type
        data_type = cls.field_metadata[name].data_type

        match json_type:
            case "string" | "integer" if isinstance(value, str | None):
                return value

            case "boolean" if data_type == "boolean" and isinstance(value, bool):
                return value

            case "double" | "integer" if isinstance(
                value, float | int | Decimal | None
            ):
                return str(value) if value is not None else None

            case "jsonobject" | "jsonarray" if value is None:
                return None

            case "jsonobject":
                match data_type:
                    case "RRULE" if value.keys() == {"RRULE"}:
                        return value["RRULE"]
                    case "lookup" | "userlookup" | "ownerlookup":
                        return value["id"]
                    case "ALARM" if isinstance(value, str):
                        return value
                    case _:
                        raise ValueError(
                            f"Unhandled JSON object data type: {data_type}"
                        )

            # Unlike most other lookup types, tag lists resolve to a list of tag names rather
            # than their respective ids. Tags are not an explicit data type in field listings,
            # so we attempt to duck type the expected schema
            case "jsonarray" if (
                next(iter(value), {}).keys() == COLORED_TAG_DATA_TYPE_FIELDS
            ):
                tag_names = [tag["name"] for tag in value]
                return ",".join(tag_names) if tag_names else None

            case "jsonarray" if any(isinstance(v, dict) for v in value):
                raise ValueError(
                    f"Array of JSON objects is unhandled. Data type: {data_type}"
                )

            case "jsonarray":
                return ",".join(value)

            case _:
                raise ValueError(
                    f"Unexpected field type found. Data type: {data_type}, JSON type: {json_type}"
                )

    @model_validator(mode="before")
    @classmethod
    def normalize_coql_data(cls, data: Any, context: ValidationInfo) -> Any:
        """
        Convert COQL output to the bulk API format for consistency.

        Though storing documents in COQL format would be preferable for its type expressivity,
        there are certain data types that get "flattened" in the Bulk API's CSV format. One
        such instance is with `ownerlookup` type fields.

        COQL format: `{..., "owner": {"id": 1234, "name": "John Doe"}, ...}`
        Bulk API format: `...,1234,...`

        Since doing Bulk -> COQL conversions would be impossible without implementing some lookup
        system, it was decided to do COQL -> Bulk instead.
        """
        is_coql_context = context.context is not None and context.context.get(
            "is_coql_data", False
        )

        field_normalization_fn = (
            cls._normalize_coql_field
            if is_coql_context
            else cls._normalize_bulk_api_field
        )
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected model input data type: {type(data)}")

        return {
            field: field_normalization_fn(field, value) for field, value in data.items()
        }

    @classmethod
    def is_api_supported(cls) -> bool:
        return (
            cls.api_name not in UNSUPPORTED_MODULES
            and cls.generated_type not in UNSUPPORTED_GENERATED_TYPES
        )

    def update(self, partial: Self) -> Self:
        """Merge fields from partial document into this document."""
        return self.model_copy(update=partial.model_dump(exclude_unset=True))

    @classmethod
    def get_field_api_names(
        cls, field_type: FieldTypeFilter = FieldTypeFilter.ALL
    ) -> set[str]:
        match field_type:
            case FieldTypeFilter.ALL:
                return {field.api_name for field in cls.field_metadata.values()}

            case FieldTypeFilter.FORMULA:
                return {
                    name
                    for name, description in cls.field_metadata.items()
                    if description.is_formula_field
                } | ALWAYS_REQUIRED_DOCUMENT_FIELDS

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise RuntimeError(
                    f"Unsupported field type filter: {field_type}"
                )  # pyright: ignore[reportUnreachable]

    @classmethod
    def has_formula_fields(cls) -> bool:
        return any(field.is_formula_field for field in cls.field_metadata.values())

    def is_complete_document(self, expected_field_type: FieldTypeFilter) -> bool:
        """Check if document has all expected fields for the given field type."""
        expected_field_names = type(self).get_field_api_names(expected_field_type)
        return expected_field_names.issubset(self.model_dump(exclude_unset=True).keys())
