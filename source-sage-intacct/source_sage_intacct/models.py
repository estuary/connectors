from datetime import datetime
from typing import (
    Any,
    ClassVar,
    Literal,
    TypeVar,
    Union,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    model_validator,
)


class EndpointConfig(BaseModel):
    sender_id: str = Field(
        description="Sender ID for the API",
        title="Sender ID",
    )
    sender_password: str = Field(
        description="Sender Password for the API",
        title="Sender Password",
        json_schema_extra={"secret": True},
    )
    company_id: str = Field(
        description="Sage Intacct Company ID",
        title="Company ID",
    )
    user_id: str = Field(
        description="User ID for the API",
        title="User ID",
    )
    password: str = Field(
        description="Password for the API",
        title="Password",
        json_schema_extra={"secret": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class ApiResponse(BaseModel):
    class ErrorMessage(BaseModel):
        class Error(BaseModel):
            errorno: str
            description: str | None
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
                sessiontimeout: AwareDatetime

            authentication: Authentication
            result: Result

        errormessage: "ApiResponse.ErrorMessage | None" = None
        operation: Operation | None = None

    response: "ApiResponse.Response"


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


class DynamicRecordModel(BaseDocument):
    tz_dt: ClassVar[datetime]
    field_names: ClassVar[list[str]]

    WHENMODIFIED: AwareDatetime
    RECORDNO: int

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        out = {}
        for field_name, field_info in cls.model_fields.items():
            if field_name not in values or (val := values[field_name]) is None:
                continue

            # Get the actual type, handling Optional types
            annotation = field_info.annotation
            assert annotation is not None
            if hasattr(annotation, "__origin__") and annotation.__origin__ is Union:
                # Optional[X] is actually Union[X, None]
                # Extract the first type argument (which is the actual type)
                annotation_args = getattr(annotation, "__args__", ())
                if type(None) in annotation_args and len(annotation_args) == 2:
                    # This is an Optional[X], so get X
                    annotation = next(
                        arg for arg in annotation_args if arg is not type(None)
                    )

            if annotation == AwareDatetime:
                assert isinstance(val, str)
                parsed = False
                for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
                    try:
                        dt_naive = datetime.strptime(val, fmt)
                        parsed = True
                        break
                    except ValueError:
                        continue

                if not parsed:
                    raise ValueError(
                        f"Invalid AwareDatetime format for {field_name}: {val}"
                    )

                dt_aware = dt_naive.replace(tzinfo=cls.tz_dt.tzinfo)
                val = dt_aware
            elif annotation == bool:
                assert isinstance(val, str)
                if val == "":
                    continue

            out[field_name] = val

        return out


XMLRecord = TypeVar("XMLRecord", bound=BaseModel)

JSONRecord = TypeVar("JSONRecord", bound=DynamicRecordModel)
