from datetime import datetime, UTC, timedelta
from typing import ClassVar, Generic, TypeVar, List
from pydantic import AwareDatetime, BaseModel, Field, HttpUrl, model_validator

from estuary_cdk.flow import AccessToken
from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    domain: HttpUrl = Field(
        description="The domain of your Gainsight NXT account. For example, https://my-company.gainsight.com or https://my-custom-domain.com.",
        title="Domain",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated. If left blank, defaults to 30 days before current time.",
        title="Start Date",
        default_factory=default_start_date,
    )


ConnectorState = GenericConnectorState[ResourceState]


ResponseDataObject = TypeVar("ResponseDataObject", bound=BaseModel)


class RecordsDataObject(BaseModel, Generic[ResponseDataObject], extra="allow"):
    records: list[ResponseDataObject] | None = None


class APIResponse(BaseModel, Generic[ResponseDataObject], extra="allow"):
    result: bool
    requestId: str
    errorCode: str | None = None
    errorDesc: str | None = None
    message: str | None = None
    data: list[ResponseDataObject] | RecordsDataObject[ResponseDataObject] | None = None

    def is_error(self) -> bool:
        return (
            not self.result or self.errorCode is not None or self.errorDesc is not None
        )


class DescribeObjectData(BaseModel, extra="allow"):
    class DescribeObjectField(BaseModel, extra="allow"):
        fieldName: str

    fields: list[DescribeObjectField]


class DescribeObjectResponse(APIResponse[DescribeObjectData]):
    data: list[DescribeObjectData] | None = None


class GainsightResource(BaseDocument, extra="allow"):
    OBJECT_NAME: ClassVar[str]
    MODIFIED_DATE_FIELD: ClassVar[str] = "ModifiedDate"


class GainsightResourceWithModifiedDate(GainsightResource):
    Gsid: str
    cursor_value: datetime = Field(exclude=True)

    @staticmethod
    def validate_required_class_attrs(class_obj, attrs: List[str]) -> None:
        for attr in attrs:
            if not hasattr(class_obj, attr) or getattr(class_obj, attr) == "":
                raise TypeError(
                    f"Class {class_obj.__name__} must define class attribute '{attr}'"
                )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        GainsightResourceWithModifiedDate.validate_required_class_attrs(
            cls,
            [
                "OBJECT_NAME",
            ],
        )

    @model_validator(mode="before")
    @classmethod
    def set_cursor_value(cls, data):
        if isinstance(data, dict) and cls.MODIFIED_DATE_FIELD in data:
            modified_date_str = data[cls.MODIFIED_DATE_FIELD]
            if isinstance(modified_date_str, str):
                data["cursor_value"] = datetime.fromisoformat(
                    modified_date_str.replace("Z", "+00:00")
                )
            else:
                data["cursor_value"] = modified_date_str
        return data


class Company(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "Company"


class User(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "gsuser"


class CsTask(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "cs_task"


class SuccessPlan(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "cta_group"


class ActivityTimeline(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "activity_timeline"
    MODIFIED_DATE_FIELD: ClassVar[str] = "LastModifiedDate"


class CallToAction(GainsightResourceWithModifiedDate):
    OBJECT_NAME: ClassVar[str] = "call_to_action"


class DaPicklist(GainsightResource):
    OBJECT_NAME: ClassVar[str] = "da_picklist"
    Gsid: str
