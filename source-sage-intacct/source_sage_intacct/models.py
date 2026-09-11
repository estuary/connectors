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
                if self.errorno == "PL04000005":
                    return True
                # AUDITHISTORY denials arrive without an `errorno`, so fall
                # back to matching the literal description Sage emits.
                if self.description2 and "do not have permission to view audit history" in self.description2.lower():
                    return True
                return False

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


# Document is the model used to derive the collection's write schema for
# incremental bindings. Only RECORDNO is required because the three runtime
# document shapes diverge on the rest: update docs carry WHENMODIFIED,
# creation docs carry WHENCREATED without WHENMODIFIED, and deletion docs
# carry WHENMODIFIED without WHENCREATED. Marking either timestamp required
# would reject one of those shapes during write-schema validation.
class Document(BaseDocument, extra="allow"):
    RECORDNO: int


class IncrementalResource(Document):
    WHENMODIFIED: AwareDatetime

    def cursor_value(self) -> AwareDatetime:
        return self.WHENMODIFIED


# CreationRecord captures records that exist in Sage with a null WHENMODIFIED.
# They cannot be captured by the WHENMODIFIED-keyed incremental query, so a
# parallel sub-task keyed on WHENCREATED picks them up.
class CreationRecord(Document):
    WHENCREATED: AwareDatetime

    def cursor_value(self) -> AwareDatetime:
        return self.WHENCREATED


def parse_backfill_record(raw: dict) -> "IncrementalResource | CreationRecord":
    """Routes to IncrementalResource when WHENMODIFIED is present, else
    CreationRecord."""
    if raw.get("WHENMODIFIED") is not None:
        return IncrementalResource.model_validate(raw)
    return CreationRecord.model_validate(raw)


# DeletionEvent is an AUDITHISTORY row with ACCESSMODE=D. Sage fills OBJECTKEY
# with the deleted object's unique name field, which differs based on the
# object type: a RECORDNO (bare, or as "<RECORDNO>--REC") for some objects, and
# a user-facing text ID (VENDORID, CUSTOMERID, ...) for others. Only the former
# can be turned into a tombstone, since the collection is keyed on RECORDNO and
# the deleted record can no longer be looked up.
class ObjectKeyNotRecordNo(ValueError):
    """Raised when an AUDITHISTORY OBJECTKEY identifies the deleted object by
    something other than its RECORDNO, so no tombstone can be built for it."""


class DeletionEvent(BaseModel):
    OBJECTKEY: str
    ACCESSTIME: AwareDatetime
    ID: str

    def cursor_value(self) -> AwareDatetime:
        return self.ACCESSTIME

    def record_no(self) -> int:
        """The RECORDNO named by OBJECTKEY. Raises ObjectKeyNotRecordNo when
        OBJECTKEY is some other identifier of the deleted object."""
        head, sep, tail = self.OBJECTKEY.partition("--")
        if sep and tail != "REC":
            raise ObjectKeyNotRecordNo(self.OBJECTKEY)
        try:
            record_no = int(head)
        except ValueError:
            raise ObjectKeyNotRecordNo(self.OBJECTKEY) from None
        if record_no <= 0:
            raise ObjectKeyNotRecordNo(self.OBJECTKEY)
        return record_no


class DeletionRecord(BaseDocument, extra="forbid"):
    RECORDNO: int
    WHENMODIFIED: AwareDatetime

    @classmethod
    def try_from_event(cls, event: DeletionEvent) -> "DeletionRecord":
        """The tombstone for `event`. Raises ObjectKeyNotRecordNo when the
        event does not name a RECORDNO and so cannot address a document in
        the collection."""
        doc = cls(RECORDNO=event.record_no(), WHENMODIFIED=event.ACCESSTIME)
        # Assigned rather than defaulted so that it survives the CDK's
        # exclude_unset serialization.
        doc.meta_ = cls.Meta(op="d")
        return doc
