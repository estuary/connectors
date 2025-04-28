
import abc
from datetime import datetime, timedelta, UTC
from enum import StrEnum
from typing import Any, TYPE_CHECKING, Iterator, Annotated

from pydantic import AwareDatetime, BaseModel, Field, model_validator
from estuary_cdk.capture.common import (
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfigWithSchedule,
    CRON_REGEX,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.http import (
    TokenSource,
)

EARLIEST_VALID_DATE_IN_SALESFORCE = datetime(1700, 1, 1, tzinfo=UTC)
# Salesforce was founded on 03FEB1999. As long as cursor values aren't backdated before this date (which only seems possible
# for a select few of them), Salesforce's founding date should be a good start date for most users.
SALESFORCE_FOUNDING_DATE = datetime(1999, 2, 3, tzinfo=UTC)


OAUTH2_SPEC = OAuth2Spec(
    provider="salesforce",
    authUrlTemplate=(
        "https://"
        r"{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}"
        ".salesforce.com/services/oauth2/authorize?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&response_type=code"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate=(
        "https://"
        r"{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}"
        ".salesforce.com/services/oauth2/token"
    ),
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
        "instance_url": "/instance_url",
    },
)

# Mustache templates in accessTokenUrlTemplate are not interpolated within the connector, so update_oauth_spec reassigns it for use within the connector.
def update_oauth_spec(is_sandbox: bool):
    OAUTH2_SPEC.accessTokenUrlTemplate = f"https://{'test' if is_sandbox else 'login'}.salesforce.com/services/oauth2/token"


# The access token response does not contain any field indicating if/when the access token we receive expires.
# The default TokenSource.AccessTokenResponse.expires_in is 0, causing every request to fetch a new access token.
# The actual expires_in value depends on the session settings in the user's Salesforce account. The default seems
# to be 2 hours. More importantly, each time the access token is used, it's validity is extended. Meaning that as long
# as the access token is actively used, it shouldn't expire. Setting an expires_in of 1 hour should be a 
# safe fallback in case a user changes all enabled bindings' intervals to an hour or greater.
class SalesforceTokenSource(TokenSource):
    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        expires_in: int = 1 * 60 * 60


class SalesforceOAuth2Credentials(BaseOAuth2Credentials):
    instance_url: str = Field(
        title="Instance URL",
    )

    @staticmethod
    def for_provider(provider: str) -> type["SalesforceOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(SalesforceOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials


if TYPE_CHECKING:
    OAuth2Credentials = SalesforceOAuth2Credentials
else:
    OAuth2Credentials = SalesforceOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


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
    credentials: OAuth2Credentials = Field(
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


# FieldDetails is used by the connector to make decisions based on field metadata, like type
# conversions or custom/formula field specific behavior.
class FieldDetails(BaseModel, extra="allow"):
    soapType: SoapTypes # Type of field
    calculated: bool # Indicates whether or not this is a formula field.
    custom: bool # Indicates whether or not this is a custom field.


class SObject(PartialSObject):
    class ObjectField(FieldDetails):
        name: str

    fields: list[ObjectField]


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


# field_details_dict_to_schema builds a schema to be included in a SourcedSchema response to
# the runtime. This ultimately tells Flow what each field's type is & causes resulting columns
# to be included in materializations even when we haven't seen data for that field yet.
# Since Salesforce is frequently incorrect about custom field types, custom fields are
# always schematized as & cast to strings.
def field_details_dict_to_schema(fields_dict: FieldDetailsDict) -> dict[str, Any]:
    schema = {
        "additionalProperties": False,
        "type": "object",
        "properties": {}
    }

    for field, details in fields_dict.items():
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
