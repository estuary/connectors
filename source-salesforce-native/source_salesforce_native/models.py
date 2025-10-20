from datetime import datetime, UTC
from enum import StrEnum
from typing import Any, Iterator, Annotated, ClassVar

from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    ValidationInfo,
    create_model,
    model_validator,
)
from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfigWithSchedule,
    CRON_REGEX,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)

from .auth import OAuth2Credentials, UserPass
from .shared import dt_to_str, str_to_dt


EARLIEST_VALID_DATE_IN_SALESFORCE = datetime(1700, 1, 1, tzinfo=UTC)
# Salesforce was founded on 03FEB1999. As long as cursor values aren't backdated before this date (which only seems possible
# for a select few of them), Salesforce's founding date should be a good start date for most users.
SALESFORCE_FOUNDING_DATE = datetime(1999, 2, 3, tzinfo=UTC)


class SalesforceResourceConfigWithSchedule(ResourceConfigWithSchedule):
    schedule: str = Field(
        default="",
        title="Formula Field Refresh Schedule",
        description="Schedule to automatically refresh formula fields. Accepts a cron expression.",
        pattern=CRON_REGEX
    )


def default_start_date():
    return SALESFORCE_FOUNDING_DATE


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to Salesforce's founding date (1999-02-03T00:00:00Z).",
        default_factory=default_start_date,
        ge=EARLIEST_VALID_DATE_IN_SALESFORCE,
    )
    is_sandbox: bool = Field(
        title="Sandbox",
        description="Toggle if you're using a Salesforce Sandbox.",
        default=False,
    )
    credentials: OAuth2Credentials | UserPass = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    class Advanced(BaseModel):
        window_size: Annotated[int, Field(
            description="Date window size for Bulk API 2.0 queries (in days). Typically left as the default unless Estuary Support or the connector logs indicate otherwise.",
            title="Window size",
            default=18250,
            gt=0,
        )]

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

ConnectorState = GenericConnectorState[ResourceState]


class PartialSObject(BaseModel, extra="allow"):
    name: str
    queryable: bool


class GlobalDescribeObjectsResponse(BaseModel, extra="allow"):
    sobjects: list[PartialSObject]


class SoapTypes(StrEnum):
    ID = "tns:ID"
    ANY_TYPE = "xsd:anyType"
    BASE64 = "xsd:base64Binary"
    BOOLEAN = "xsd:boolean"
    DATE = "xsd:date"
    DATETIME = "xsd:dateTime"
    DOUBLE = "xsd:double"
    INTEGER = "xsd:int"
    STRING = "xsd:string"
    # The following are not mentioned in Salesforce's documentation but do exist when decribing an object's fields.
    LONG = "xsd:long"
    TIME = "xsd:time"
    JSON = "tns:json"
    ADDRESS = "urn:address"
    JUNCTION_ID_LIST_NAMES = "urn:JunctionIdListNames"
    LOCATION = "urn:location"
    RELATIONSHIP_REFERENCE_TO = "urn:RelationshipReferenceTo"
    RECORD_TYPES_SUPPORTED = "urn:RecordTypesSupported"
    SEARCH_LAYOUT_BUTTONS_DISPLAYED = "urn:SearchLayoutButtonsDisplayed"
    SEARCH_LAYOUT_FIELDS_DISPLAYED = "urn:SearchLayoutFieldsDisplayed"
    SEARCH_LAYOUT_BUTTON = "urn:SearchLayoutButton"
    SEARCH_LAYOUT_FIELD = "urn:SearchLayoutField"


SOAP_TYPES_NOT_SUPPORTED_BY_BULK_API = [
    SoapTypes.BASE64,
    SoapTypes.JSON,
    SoapTypes.ADDRESS,
    SoapTypes.LOCATION,
    SoapTypes.SEARCH_LAYOUT_BUTTON,
    SoapTypes.SEARCH_LAYOUT_BUTTONS_DISPLAYED,
    SoapTypes.SEARCH_LAYOUT_FIELD,
    SoapTypes.SEARCH_LAYOUT_FIELDS_DISPLAYED,
]

# BaseFieldDetails represents field metadata returned from Salesforce.
class BaseFieldDetails(BaseModel, extra="allow"):
    soapType: SoapTypes # Type of field
    calculated: bool # Indicates whether or not this is a formula field.
    custom: bool # Indicates whether or not this is a custom field.


class SObject(PartialSObject):
    class ObjectField(BaseFieldDetails):
        name: str

    fields: list[ObjectField]


# FieldDetails is used by the connector to make decisions based on field metadata, like type
# conversions or custom/formula field specific behavior.
class FieldDetails(BaseFieldDetails, extra="allow"):
    should_include_in_refresh: bool = False


# FieldDetailsDict is a connector-internal structure used to convert field
# types in records fetched via the Bulk API.
class FieldDetailsDict(BaseModel):
    fields: dict[str, FieldDetails]

    @classmethod
    def model_validate(cls, obj: Any, *args, **kwargs):
        if isinstance(obj, dict) and "fields" not in obj:
            obj = {"fields": obj}
        return super().model_validate(obj, *args, **kwargs)

    def __getitem__(self, key: str) -> FieldDetails:
        return self.fields[key]

    def __setitem__(self, key: str, value: FieldDetails) -> None:
        self.fields[key] = value

    def __iter__(self) -> Iterator[str]:
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def keys(self):
        return self.fields.keys()

    def values(self):
        return self.fields.values()

    def items(self):
        return self.fields.items()


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class SalesforceResource(FullRefreshResource):
    Id: str


class CursorFields(StrEnum):
    SYSTEM_MODSTAMP = "SystemModstamp"
    LAST_MODIFIED_DATE = "LastModifiedDate"
    CREATED_DATE = "CreatedDate"
    LOGIN_TIME = "LoginTime"

# REST API Related Models

class QueryResponse(BaseModel, extra="forbid"):
    totalSize: int
    done: bool
    records: list[dict[str, Any]]
    nextRecordsUrl: str | None = None

# Bulk Job Related Models

class BulkJobStates(StrEnum):
    UPLOAD_COMPLETE = "UploadComplete"
    IN_PROGRESS = "InProgress"
    ABORTED = "Aborted"
    JOB_COMPLETE = "JobComplete"
    FAILED = "Failed"


class BulkJobSubmitResponse(BaseModel, extra="allow"):
    id: str
    state: BulkJobStates
    object: str
    operation: str
    createdDate: str
    systemModstamp: str


class BulkJobCheckStatusResponse(BulkJobSubmitResponse):
    retries: int
    numberRecordsProcessed: int | None = None
    totalProcessingTime: int
    errorMessage: str | None = None


class BulkJobError(RuntimeError):
    """Exception raised for errors when executing a bulk query job."""
    def __init__(self, message: str, query: str | None = None, error: str | None = None):
        self.message = message
        self.query = query
        self.errors = error

        self.details: dict[str, Any] = {
            "message": self.message
        }

        if self.errors:
            self.details["errors"] = self.errors
        if self.query:
            self.details["query"] = self.query

        super().__init__(self.details)

    def __str__(self):
        return f"BulkJobError: {self.message}"

    def __repr__(self):
        return (
            f"BulkJobError: {self.message},"
            f"query: {self.query},"
            f"errors: {self.errors}"
        )


class SalesforceDataSource(StrEnum):
    REST_API = "rest_api"
    BULK_API = "bulk_api"


class ValidationContext:
    def __init__(self, data_source: SalesforceDataSource):
        self.data_source = data_source


class SalesforceRecord(BaseDocument, extra="allow"):
    field_details: ClassVar[FieldDetailsDict]

    # Since SalesforceRecord has extra="allow", cursor fields like SystemModstamp
    # and other Salesforce fields are stored in __pydantic_extra__ rather than as
    # regular model attributes. This override enables easy access to these
    # extra fields with `getattr``.
    def __getattr__(self, name: str) -> Any:
        extra = getattr(self, '__pydantic_extra__', None)
        if extra is not None and name in extra:
            return extra[name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    @model_validator(mode="before")
    @classmethod
    def _transform_fields(cls, values: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        if not hasattr(cls, 'field_details'):
            raise RuntimeError(
                "field_details must be set on the SalesforceRecord subclass before validation."
            )

        if not info.context or not isinstance(info.context, ValidationContext):
            raise RuntimeError(f"Validation context must be of type ValidationContext: {info.context}")

        data_source = info.context.data_source

        transformed: dict[str, Any] = {}

        # Iterate over the values to transform them based on field details
        # and the data source/API.
        for field_name, pre_value in values.items():
            match data_source:
                case SalesforceDataSource.BULK_API:
                    transformed_value = cls._transform_bulk_value(
                        field_name,
                        cls.field_details[field_name],
                        pre_value,
                    )
                case SalesforceDataSource.REST_API:
                    # REST API query results have an extraneous "attributes" field with a small amount of metadata.
                    # This metadata isn't present in the Bulk API response, so we remove it from records fetched
                    # via the REST API.
                    if field_name == "attributes":
                            continue

                    transformed_value = cls._transform_rest_value(
                        cls.field_details[field_name],
                        pre_value,
                    )
                case _:
                    raise ValueError(f"Unsupported data source: {data_source}")

            transformed[field_name] = transformed_value

        return transformed

    @classmethod
    def _transform_bulk_value(
        cls,
        name: str,
        field_details: FieldDetails,
        value: str,
    ) -> Any:
        # Salesforce represents null values with empty strings in Bulk API responses.
        if value == "":
            return None

        # Since Salesforce is often wrong about the types of custom fields, we
        # leave them as a string to align with the sourced schemas the connector
        # emits & the materialized columns types created based off of those
        # sourced schemas.
        if field_details.custom:
            return value

        # Transform standard fields to the correct type.
        match field_details.soapType:
            case SoapTypes.ID | SoapTypes.STRING | SoapTypes.DATE | SoapTypes.DATETIME | SoapTypes.TIME | SoapTypes.BASE64:
                transformed_value = value
            case SoapTypes.BOOLEAN:
                transformed_value = cls._bool_str_to_bool(value)
            case SoapTypes.INTEGER | SoapTypes.LONG:
                # Sometimes even standard Salesforce integer fields _actually_ contain floats. So
                # we try to convert them to integers first and fallback to converting to floats.
                transformed_value = cls._str_to_number(value)
            case SoapTypes.DOUBLE:
                transformed_value = float(value)
            case SoapTypes.ANY_TYPE:
                transformed_value = cls._str_to_anytype(value)
            case _:
                raise BulkJobError(f"Unanticipated field type {field_details.soapType} for field {name}. Please reach out to Estuary support for help resolving this issue.")

        return transformed_value

    @classmethod
    def _transform_rest_value(
        cls,
        field_details: FieldDetails,
        value: Any,
    ) -> Any:
        """Transform REST API specific value transformations for already-typed values."""
        # The datetime field formats between REST and Bulk API are different, so we try
        # to convert any cursor fields to the same format to keep values consistent between them.
        if field_details.soapType == SoapTypes.DATETIME and isinstance(value, str):
            try:
                return dt_to_str(str_to_dt(value))
            except ValueError:
                pass

        # Since Salesforce is often wrong about the types of custom fields, we
        # cast them to strings to align with the sourced schemas the connector
        # emits & the materialized columns types created based off of those
        # sourced schemas.
        if field_details.custom:
            return cls._cast_custom_field_to_string(value)

        return value

    # _cast_custom_field_to_string does not use Salesforce's reported SOAP type because we've seen
    # Salesforce be incorrect about types for custom fields. Instead, the field's value is inspected
    # and then cast based off that inspection.
    @classmethod
    def _cast_custom_field_to_string(cls, value: Any | None) -> Any:
        # If the value is already a string or None, don't cast it.
        if value is None or isinstance(value, str):
            return value

        if isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        else:
            # All other types are left unchanged. Most of these are complex object types
            # that can't be cast to strings, like urn:location or urn:address.
            return value

    @classmethod
    def _bool_str_to_bool(cls, string: str) -> bool:
        lowercase_string = string.lower()
        if lowercase_string == "true":
            return True
        elif lowercase_string == "false":
            return False
        else:
            raise ValueError(f"No boolean equivalent for {string}.")

    @classmethod
    def _str_to_number(cls, string: str) -> int | float:
        try:
            return int(string)
        except ValueError:
            return float(string)

    @classmethod
    def _str_to_anytype(cls, string: str) -> str | bool | int | float:
        if string.lower() in ["true", "false"]:
            return cls._bool_str_to_bool(string)

        try:
            return cls._str_to_number(string)
        except ValueError:
            return string

    # sourced_schema builds a schema to be included in a SourcedSchema response to
    # the runtime. This ultimately tells Flow what each field's type is & causes resulting columns
    # to be included in materializations even when we haven't seen data for that field yet.
    # Since Salesforce is frequently incorrect about custom field types, custom fields are
    # always schematized as & cast to strings.
    @classmethod
    def sourced_schema(cls) -> dict[str, Any]:
        if not hasattr(cls, 'field_details'):
            raise RuntimeError(
                "field_details must be set on the SalesforceRecord subclass before generating a sourced schema."
            )

        schema = {
            "additionalProperties": False,
            "type": "object",
            "properties": {}
        }

        for field, details in cls.field_details.items():
            field_schema: dict[str, Any] = {}
            match details.soapType:
                case SoapTypes.ID | SoapTypes.BASE64 | SoapTypes.STRING:
                    field_schema = {"type": "string"}
                case SoapTypes.BOOLEAN:
                    if details.custom:
                        field_schema = {"type": "string"}
                    else:
                        field_schema = {"type": "boolean"}
                case SoapTypes.DATE:
                    field_schema = {
                        "type": "string",
                        "format": "date"
                    }
                case SoapTypes.DATETIME:
                    field_schema = {
                        "type": "string",
                        "format": "date-time",
                    }
                case SoapTypes.TIME:
                    field_schema = {
                        "type": "string",
                        "format": "time",
                    }
                case SoapTypes.INTEGER:
                    if details.custom:
                        field_schema = {
                            "type": "string",
                            "format": "integer",
                        }
                    else:
                        field_schema = {"type": "integer"}
                case SoapTypes.DOUBLE | SoapTypes.LONG:
                    if details.custom:
                        field_schema = {
                            "type": "string",
                            "format": "number",
                        }
                    else:
                        field_schema = {"type": "number"}
                case SoapTypes.ADDRESS:
                    field_schema = {
                        "additionalProperties": False,
                        "type": "object",
                        "properties": {
                            "city": {"type": "string"},
                            "country": {"type": "string"},
                            "geocodeAccuracy": {"type": "string"},
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                            "postalCode": {"type": "string"},
                            "state": {"type": "string"},
                            "street": {"type": "string"},
                        }
                    }
                case SoapTypes.LOCATION:
                    field_schema = {
                        "additionalProperties": False,
                        "type": "object",
                        "properties": {
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                        }
                    }
                case SoapTypes.ANY_TYPE:
                    field_schema = {
                        "type": [
                            "string",
                            "number",
                            "boolean",
                        ],
                    }
                case _:
                    # Omit fields of all other types from the sourced schema.
                    # The remaining types are uncommon and haven't been observed yet in production. Once
                    # fields with these types are captured and Flow schematizes them, we can add those
                    # types to the emitted sourced schemas.
                    continue

            schema["properties"][field] = field_schema

        return schema


def create_salesforce_model(object_name: str, fields: FieldDetailsDict) -> type[SalesforceRecord]:
    field_defs = {}

    # No fields other than `Id` are included in the model by default since what fields should be included differ
    # between normal incremental replication/backfills and formula field refreshes.
    if "Id" in fields.keys():
        field_defs["Id"] = (str, ...)

    model = create_model(
        object_name,
        __base__=SalesforceRecord,
        **field_defs,
    )

    model.field_details = fields

    return model
