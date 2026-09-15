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
        pattern=CRON_REGEX,
        json_schema_extra={"nonsensitive": True},
    )


class EndpointConfig(BaseModel):
    credentials: (
        AccessToken
        # Disable the OAuth authentication option until we have an OAuth app in Airtable set up.
        # | OAuth2Credentials 
    ) = Field(
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


# Field types whose "empty" state, per Airtable's own cell format for the
# type, can only ever be the single falsy value below - so injecting it back
# in place of an omitted field is lossless, unlike e.g. injecting 0 for a
# cleared number field (a real, distinguishable value that just happens to
# also be falsy). See https://airtable.com/developers/web/api/field-model
# for each type's documented cell format.
_UNAMBIGUOUS_EMPTY_BOOLEAN_TYPES = frozenset({
    "checkbox",  # cell format is "boolean (true only)" - never explicitly false
})
_UNAMBIGUOUS_EMPTY_STRING_TYPES = frozenset({
    "singleLineText", "multilineText", "richText", "email", "url", "phoneNumber",
})
_UNAMBIGUOUS_EMPTY_ARRAY_TYPES = frozenset({
    "multipleSelects", "multipleRecordLinks", "multipleAttachments", "multipleCollaborators",
    # multipleLookupValues' cell format is unconditionally "array<...>" (V1)
    # regardless of what the linked field's own type is - the outer shape
    # never varies, only the element type does, so an omitted field is still
    # safely "no values to show" as [].
    "multipleLookupValues",
})
# Deliberately NOT included, even though they look plausible at a glance.
# Every type below still gets covered by this fix - it's just via the
# `None` fallback in _empty_value_for_field_type, not a type-specific value:
# - singleSelect: cell format is a string, but it's one of a constrained set
#   of configured option names, not free text - "" isn't a documented "no
#   selection" convention the way it is for free-text fields.
# - formula, rollup: cell format varies per-record based on a nested `result`
#   type (Airtable's own docs note `result` "can be null if invalid"), so
#   there's no single static shape to assume from the field's own type.
# - number-like (number, currency, percent, duration, rating, count,
#   autoNumber) and date-like (date, dateTime) types: Airtable's docs give no
#   empty-value convention for these at all, unlike the explicit "", [],
#   false examples for other types - there's no safe non-null empty value.

assert not (
    _UNAMBIGUOUS_EMPTY_BOOLEAN_TYPES & _UNAMBIGUOUS_EMPTY_STRING_TYPES
    | _UNAMBIGUOUS_EMPTY_BOOLEAN_TYPES & _UNAMBIGUOUS_EMPTY_ARRAY_TYPES
    | _UNAMBIGUOUS_EMPTY_STRING_TYPES & _UNAMBIGUOUS_EMPTY_ARRAY_TYPES
), "a field type must not appear in more than one _UNAMBIGUOUS_EMPTY_*_TYPES set"


def _empty_value_for_field_type(field_type: str) -> Any:
    """The value to inject when Airtable omits a field of this type because
    it's empty, per _UNAMBIGUOUS_EMPTY_*_TYPES above. Every other type falls
    back to `None` here, including field types not yet known to this
    connector - `None` is always a safe, if less precise, choice.
    """
    if field_type in _UNAMBIGUOUS_EMPTY_BOOLEAN_TYPES:
        return False
    if field_type in _UNAMBIGUOUS_EMPTY_STRING_TYPES:
        return ""
    if field_type in _UNAMBIGUOUS_EMPTY_ARRAY_TYPES:
        return []
    return None


def expected_field_defaults(
    fields: list[AirtableField],
    exclude: str | None = None,
) -> dict[str, Any]:
    """Maps each field name Airtable returns by default for a request with no
    `fields` query param to the value that should be injected if Airtable
    omits that field from a given record. `exclude` is a record model's
    cursor field name, when one exists — the cursor is tracked through its
    own aliased pydantic field, never through this fill, so its literal name
    must never appear here.
    """
    return {
        f.name: _empty_value_for_field_type(f.type)
        for f in fields
        if f.name != exclude
    }


class FieldPresenceContext:
    def __init__(self, expected_field_defaults: dict[str, Any]):
        self.expected_field_defaults = expected_field_defaults


class AirtableRecordFields(BaseModel, extra="allow"):
    @model_validator(mode='before')
    @classmethod
    def normalize_fields(cls, data: Any, info: ValidationInfo) -> Any:
        """Clean up the raw `fields` object Airtable returns before construction.

        Two things happen here, both hinging on whether a field name is present
        in the raw response, so they're done together in a single pass instead
        of as separate validators (a separate "fill missing fields" validator
        would need to run after this one to see the right picture of what's
        missing, and Pydantic runs multiple `mode='before'` validators on the
        same model in reverse declaration order, which is easy to get backwards).

        1. When the formula for a formula field results in an error (circular
           reference, NaN, divide by zero, etc), Airtable returns an object like
           {"error": "#ERROR!"} or {"specialValue": "NaN"} instead of the usual
           scalar result. Allowing these into documents would widen the inferred
           schema to say these fields could be either their normal scalar type
           or an object, which likely isn't what users want, so they're removed
           here. Unlike step 2 below, an error'd field is deliberately left
           OMITTED rather than filled with a default: the error is Airtable's
           formula engine transiently choking (a dependent field mid-edit,
           recompute lag), not evidence the field's real value is now empty, so
           under `merge` it should read as "no change" and keep whatever value
           was last known-good - not get overwritten to null.
           See: https://support.airtable.com/docs/common-formula-errors-and-how-to-fix-them

        2. Airtable omits a field from `fields` entirely once its value becomes
           empty/falsy - it never sends an explicit null. Under this
           connector's `merge` reduction strategy, an omitted key means "no
           change" while an explicit null means "clear it", so a field that's
           actually been cleared in Airtable would otherwise stay frozen at its
           last non-empty value forever. `FieldPresenceContext` carries the
           field names this fetch actually requested from Airtable (excluding
           a record model's cursor field, which is tracked through its own
           aliased pydantic field, never through this fill), mapped to the
           value to inject if still missing - `None` for most field types, or
           a type-specific falsy value (`""`, `[]`, `False`) for the handful
           of types where that's the type's only possible empty representation
           (see _empty_value_for_field_type above). Fields omitted because of
           step 1's error-stripping are excluded from this fill (see above).
        """
        if not info.context or not isinstance(info.context, FieldPresenceContext):
            raise RuntimeError(f"Validation context must be of type FieldPresenceContext: {info.context}")

        if not isinstance(data, dict):
            return data

        error_field_names = {
            k for k, v in data.items()
            if _is_formula_error(v)
        }
        cleaned = {
            k: v for k, v in data.items()
            if k not in error_field_names
        }

        for name, default in info.context.expected_field_defaults.items():
            if name not in error_field_names:
                cleaned.setdefault(name, default)

        return cleaned


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
