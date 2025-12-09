from enum import StrEnum
from typing import Any, ClassVar, Optional, TYPE_CHECKING

from pydantic import (
    AwareDatetime,
    BaseModel,
    ValidationInfo,
    Field,
    create_model,
    model_validator,
)
from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfigWithSchedule,
    CRON_REGEX,
    ReductionStrategy,
    ResourceState,
)
from estuary_cdk.flow import (
    AccessToken,
    RotatingOAuth2Credentials,
    OAuth2ClientCredentialsPlacement,
    OAuth2Spec
)


# Fixed Python attribute name for the lastModifiedTime cursor field.
# Airtable field names can contain spaces and special characters, so we use
# this constant as the internal attribute name and map it via Pydantic's alias
# to the actual field name. Uses trailing underscore to avoid collisions with
# user-defined Airtable fields.
CURSOR_FIELD_ATTR = "last_modified_time_value_"


OAUTH2_SPEC = OAuth2Spec(
    provider="airtable",
    authUrlTemplate=(
        "https://airtable.com/oauth2/v1/authorize?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        "&response_type=code"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
        "&scope=data.records:read%20data.recordComments:read%20schema.bases:read"
        r"&code_challenge={{#urlencode}}{{{ code_challenge }}}{{/urlencode}}"
        r"&code_challenge_method=S256"
    ),
    accessTokenUrlTemplate=(
        "https://airtable.com/oauth2/v1/token"
    ),
    accessTokenHeaders={
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": r"Basic {{#basicauth}}{{{ client_id }}}:{{{ client_secret }}}{{/basicauth}}",
    },
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
        r"&code_verifier={{#urlencode}}{{{ code_verifier }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "access_token": "/access_token",
        "refresh_token": "/refresh_token",
        "access_token_expires_at": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
)

if TYPE_CHECKING:
    OAuth2Credentials = RotatingOAuth2Credentials
else:
    OAuth2Credentials = RotatingOAuth2Credentials.with_client_credentials_placement(
        OAuth2ClientCredentialsPlacement.HEADERS
    ).for_provider(
        OAUTH2_SPEC.provider
    )


class AirtableResourceConfigWithSchedule(ResourceConfigWithSchedule):
    schedule: str = Field(
        default="",
        title="Formula Field Refresh Schedule",
        description="Schedule to automatically refresh formula fields. Accepts a cron expression.",
        pattern=CRON_REGEX
    )


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class PermissionLevel(StrEnum):
    NONE = "none"
    CREATE = "create"
    READ = "read"
    COMMENT = "comment"
    EDIT = "edit"


class Base(BaseDocument, extra="allow"):
    id: str
    name: str
    permissionLevel: PermissionLevel


class BasesResponse(BaseModel, extra="allow"):
    bases: list[Base]
    offset: str | None = None


class AirtableFieldOptions(BaseModel, extra="allow"):
    class Result(BaseModel, extra="allow"):
        type: str

    result: Result | None = None
    referencedFieldIds: list[str] | None = None


class AirtableField(BaseModel, extra="allow"):
    type: str
    id: str
    name: str
    options: AirtableFieldOptions | None = None

    # is_valid_cursor_field checks for a lastModifiedTime field that will update
    # whenever any changes are made to a record. Airtable names this field "Last Modified",
    # but users can renamed that field to something else, like "Recent Changes", and the
    # connector has to dynamically determine if an appropriate lastModifiedTime field exists
    # instead of assuming the "Last Modified" field will always be an appropriate cursor field.
    def is_valid_cursor_field(self) -> bool:
        """Check if this field can be used as an incremental cursor.

        A valid cursor field must be:
        - Of type 'lastModifiedTime'
        - Have options.result.type of 'dateTime' (not just 'date')
        - Have options.referencedFieldIds as an empty array (tracks all fields)
        """
        return (
            self.type == "lastModifiedTime"
            and self.options is not None
            and self.options.result is not None
            and self.options.result.type == "dateTime"
            and self.options.referencedFieldIds == []
        )


class TableValidationContext:
    def __init__(self, base_id: str):
        self.base_id = base_id


class Table(BaseDocument, extra="allow"):
    id: str
    name: str
    fields: list[AirtableField]

    baseId: str

    @model_validator(mode="before")
    @classmethod
    def _add_base_id(cls, values: dict[str, Any], info: ValidationInfo):
        if not info.context or not isinstance(info.context, TableValidationContext):
            raise RuntimeError(f"Validation context must be of type TableValidationContext: {info.context}")

        values["baseId"] = info.context.base_id

        return values


def _is_formula_error(value: Any) -> bool:
    """Check if a value represents an Airtable formula error.

    Airtable formula errors are returned as objects in these specific formats:
    - {"error": "#ERROR!"} - Generic errors (syntax, missing fields, null values)
    - {"error": "circular reference"} - Circular reference between fields
    - {"specialValue": "NaN"} - Not a number (0/0 or date field without value)
    - {"specialValue": "Infinity"} - Division by zero (e.g., 5/0)
    """
    if isinstance(value, dict):
        keys = set(value.keys())

        for error_key in ["error", "specialValue"]:
            if keys == {error_key} and isinstance(value[error_key], str):
                return True
    return False


class AirtableRecordFields(BaseModel, extra="allow"):
    # When the formula for a formula field results in an error (circular reference, NaN, divide by zero, etc),
    # Airtable returns an object like {"error": "#ERROR!"} or {"specialValue": "NaN"}.
    #
    # Allowing these errors in documents would mangle collections' inferred schemas, and the
    # inferred schema would end up saying these formula fields could be either the type of the normally calculated value
    # (like a string or number) or it could be an object. I doubt users will want their inferred schemas to
    # get widened when these errors occur, so I'm filtering them out before emitting any documents.
    @model_validator(mode='before')
    @classmethod
    def remove_formula_errors(cls, data: Any) -> Any:
        """Remove fields that contain formula errors.

        See: https://support.airtable.com/docs/common-formula-errors-and-how-to-fix-them
        """
        if isinstance(data, dict):
            return {
                k: v for k, v in data.items()
                if not _is_formula_error(v)
            }
        return data


class AirtableRecord(BaseDocument, extra="allow"):
    id: str
    createdTime: AwareDatetime
    fields: AirtableRecordFields = Field(
        json_schema_extra={"reduce": {"strategy": ReductionStrategy.MERGE}},
    )


class IncrementalAirtableRecord(AirtableRecord, extra="allow"):
    """Airtable record with cursor field support for incremental replication.

    Use create_incremental_record_model() to create properly configured subclasses
    when tables have a column that can be used as an incremental cursor.
    """

    cursor_field_name: ClassVar[str]

    @property
    def cursor_value(self) -> AwareDatetime:
        value = getattr(self.fields, CURSOR_FIELD_ATTR, None)
        if value is None:
            return self.createdTime
        return value


def create_incremental_record_model(cursor_field_name: str) -> type[IncrementalAirtableRecord]:
    fields_model = create_model(
        f"Fields_{cursor_field_name}",
        __base__=AirtableRecordFields,
        **{
            CURSOR_FIELD_ATTR: (
                Optional[AwareDatetime],
                Field(
                    alias=cursor_field_name,
                    default=None,
                    # Don't schematize the default value.
                    json_schema_extra=lambda x: x.pop('default')  # type: ignore
                )
            )
        },
    )

    record_model = create_model(
        f"IncrementalAirtableRecord_{cursor_field_name}",
        __base__=IncrementalAirtableRecord,
        fields=(
            fields_model,
            Field(json_schema_extra={"reduce": {"strategy": ReductionStrategy.MERGE}}),
        ),
    )

    record_model.cursor_field_name = cursor_field_name

    return record_model


class RecordsResponse(BaseModel, extra="allow"):
    records: list[AirtableRecord]
    offset: str | None = None
