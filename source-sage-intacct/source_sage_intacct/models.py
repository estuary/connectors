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


class ApiResponse(BaseModel):
    class ErrorMessage(BaseModel):
        class Error(BaseModel):
            errorno: str
            description2: str

        error: list[Error] | Error

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
            if isinstance(self.response.errormessage.error, list):
                error = self.response.errormessage.error[0]
            else:
                error = self.response.errormessage.error
            raise ValidationError([f"Error: {error.errorno} - {error.description2}"])

        if self.response.operation and self.response.operation.errormessage:
            if isinstance(self.response.operation.errormessage.error, list):
                error = self.response.operation.errormessage.error[0]
            else:
                error = self.response.operation.errormessage.error
            raise ValidationError([f"Error: {error.errorno} - {error.description2}"])

        if self.response.operation:
            if self.response.operation.authentication.status != "success":
                raise ValidationError(
                    [
                        f"authentication status: {self.response.operation.authentication.status}"
                    ]
                )

            if self.response.operation.result:
                if self.response.operation.result.status != "success":
                    raise ValidationError(
                        [f"result status: {self.response.operation.result.status}"]
                    )


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


class DeletionRecord(BaseDocument, extra="forbid"):
    RECORDNO: int = Field(alias="OBJECTKEY")
    WHENMODIFIED: AwareDatetime = Field(alias="ACCESSTIME")
    ID: str = Field(exclude=True)

    meta_: "DeletionRecord.Meta" = Field(
        default_factory=lambda: DeletionRecord.Meta(op="d")
    )

    @field_validator("RECORDNO", mode="before")
    @classmethod
    def parse_object_key(cls, value: str) -> int:
        assert isinstance(value, str)

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
