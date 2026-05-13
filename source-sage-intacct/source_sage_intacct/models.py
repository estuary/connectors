from typing import (
    Any,
    Literal,
    Optional,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import ValidationError
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    field_validator,
)

COMPANY_ID_FIELD = "COMPANY_ID"


class EndpointConfig(BaseModel):
    sender_id: str = Field(
        description="Web Services Sender ID",
        title="Sender ID",
    )
    sender_password: str = Field(
        description="Web Services Sender Password",
        title="Sender Password",
        json_schema_extra={"secret": True},
    )
    company_id: str = Field(
        description="Sage Intacct Company ID",
        title="Company ID",
    )
    user_id: str = Field(
        description="Sage Intacct User ID",
        title="User ID",
    )
    password: str = Field(
        description="Sage Intacct Password",
        title="Password",
        json_schema_extra={"secret": True},
    )

    class Advanced(BaseModel):
        include_company_id_in_documents: bool = Field(
            description=f"Include the configured Sage Intacct Company ID in captured documents, with the field name '{COMPANY_ID_FIELD}'. Every captured document will have the same value for this field, equal to the configured Sage Intacct Company ID.",
            title="Include Company ID in Documents",
            default=False,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class SagePermissionError(Exception):
    """Raised when a Sage Intacct API call fails because the authenticated
    user's role lacks permission for the requested operation."""


class ApiResponse(BaseModel):
    class ErrorMessage(BaseModel):
        class Error(BaseModel):
            errorno: str | None = None
            description2: str | None = None

            def __str__(self) -> str:
                parts = []
                if self.errorno:
                    parts.append(f"Error: {self.errorno}")
                if self.description2:
                    parts.append(self.description2)
                return " - ".join(parts) if parts else "unspecified Sage Intacct error"

            def is_permission_error(self) -> bool:
                return self.errorno == "PL04000005"

        error: list[Error] | Error

        def _errors(self) -> list["ApiResponse.ErrorMessage.Error"]:
            return self.error if isinstance(self.error, list) else [self.error]

        def __str__(self) -> str:
            return str(self._errors()[0])

        def is_permission_error(self) -> bool:
            return any(e.is_permission_error() for e in self._errors())

    class Response(BaseModel):
        class Operation(BaseModel):
            class Result(BaseModel):
                status: str
                data: Any | None = None
                errormessage: "ApiResponse.ErrorMessage | None" = None

            class Authentication(BaseModel):
                status: str
                userid: str
                sessiontimeout: AwareDatetime | None = None

            authentication: Authentication
            result: Result | None = None
            errormessage: "ApiResponse.ErrorMessage | None" = None

        errormessage: "ApiResponse.ErrorMessage | None" = None
        operation: Operation | None = None

    response: "ApiResponse.Response"

    def raise_for_error(self):
        if self.response.errormessage:
            self._raise(self.response.errormessage)

        if self.response.operation and self.response.operation.errormessage:
            self._raise(self.response.operation.errormessage)

        if self.response.operation:
            if self.response.operation.authentication.status != "success":
                raise ValidationError(
                    [
                        f"authentication status: {self.response.operation.authentication.status}"
                    ]
                )

            if self.response.operation.result:
                if self.response.operation.result.errormessage:
                    self._raise(self.response.operation.result.errormessage)

                if self.response.operation.result.status != "success":
                    raise ValidationError(
                        [f"result status: {self.response.operation.result.status}"]
                    )

    @staticmethod
    def _raise(err: "ApiResponse.ErrorMessage") -> None:
        if err.is_permission_error():
            raise SagePermissionError(str(err))
        raise ValidationError([str(err)])


class GenerateApiSessionResponse(BaseModel):
    class API(BaseModel):
        sessionid: str
        endpoint: str

    api: "GenerateApiSessionResponse.API"


class GetUserByIDResponse(BaseModel):
    class UserInfo(BaseModel):
        RECORDNO: int

    USERINFO: "GetUserByIDResponse.UserInfo"


class ListUserDateAndTimestampFormattingResponse(BaseModel):
    class UserFormatting(BaseModel):
        locale: str
        dateformat: str
        gmtoffset: str
        clock: Literal["12", "24"]

    userformatting: "ListUserDateAndTimestampFormattingResponse.UserFormatting"


class FieldDefinition(BaseModel):
    ID: str
    DATATYPE: str


class ObjectDefinition(BaseModel):
    class Type_(BaseModel):
        class Fields_(BaseModel):
            Field: list[FieldDefinition]

        Fields: Fields_

    Type: Type_


class SnapshotResource(BaseDocument, extra="allow"):
    RECORDNO: Optional[int] = Field(default=None, exclude=True)


class IncrementalResource(BaseDocument, extra="allow"):
    RECORDNO: int
    WHENMODIFIED: AwareDatetime

    def cursor_value(self) -> AwareDatetime:
        return self.WHENMODIFIED


# CreationRecord captures records that exist in Sage with a null WHENMODIFIED.
# They cannot be captured by the WHENMODIFIED-keyed incremental query, so a
# parallel sub-task keyed on WHENCREATED picks them up.
class CreationRecord(BaseDocument, extra="allow"):
    RECORDNO: int
    WHENCREATED: AwareDatetime

    def cursor_value(self) -> AwareDatetime:
        return self.WHENCREATED


def parse_backfill_record(raw: dict) -> "IncrementalResource | CreationRecord":
    """Routes to IncrementalResource when WHENMODIFIED is present, else
    CreationRecord."""
    if raw.get("WHENMODIFIED") is not None:
        return IncrementalResource.model_validate(raw)
    return CreationRecord.model_validate(raw)


class DeletionRecord(BaseDocument, extra="forbid"):
    RECORDNO: int = Field(alias="OBJECTKEY", serialization_alias="RECORDNO")
    WHENMODIFIED: AwareDatetime = Field(alias="ACCESSTIME", serialization_alias="WHENMODIFIED")
    ID: str = Field(exclude=True)

    meta_: "DeletionRecord.Meta" = Field(
        default_factory=lambda: DeletionRecord.Meta(op="d")
    )

    def cursor_value(self) -> AwareDatetime:
        return self.WHENMODIFIED

    @field_validator("RECORDNO", mode="before")
    @classmethod
    def parse_object_key(cls, value: str) -> int:
        assert isinstance(value, str)

        # Handle case where OBJECTKEY is just a stringified integer, as it is
        # for TASK objects and perhaps others.
        if "--" not in value:
            try:
                record_no = int(value)
                if record_no <= 0:
                    raise ValueError(
                        f"RECORDNO in OBJECTKEY must be positive, got {record_no}"
                    )
                return record_no
            except ValueError:
                raise ValueError(
                    f"OBJECTKEY must be an integer or in format 'RECORDNO--REC', got {value}"
                )

        # Handle case where OBJECTKEY is in format "RECORDNO--REC".
        parts = value.split("--")
        if len(parts) != 2:
            raise ValueError(
                f"OBJECTKEY must be in format 'RECORDNO--REC', got {value}"
            )

        if parts[1] != "REC":
            raise ValueError(f"OBJECTKEY suffix must be 'REC', got {parts[1]}")

        try:
            record_no = int(parts[0])
            if record_no <= 0:
                raise ValueError(
                    f"RECORDNO in OBJECTKEY must be positive, got {record_no}"
                )
            return record_no
        except ValueError:
            raise ValueError(
                f"RECORDNO in OBJECTKEY must be an integer, got {parts[0]}"
            )
