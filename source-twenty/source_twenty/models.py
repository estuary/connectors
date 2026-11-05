import abc
from decimal import Decimal
from typing import Any, ClassVar, Literal, Self

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceConfigWithSchedule as ResourceConfig,
)
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, model_validator


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key", json_schema_extra={"type": "string", "order": 0}
    )
    access_token: str = Field(
        title="API Key",
        description="A Twenty API key, created in Twenty under Settings > APIs & Webhooks.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    base_url: str = Field(
        default="https://api.twenty.com",
        title="Base URL",
        description="The address of your Twenty server's API. Leave the default for Twenty Cloud; for self-hosted Twenty, use your server's address, like https://twenty.example.com.",
        pattern="^https?://",
        json_schema_extra={"order": 0},
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 1},
    )


ConnectorState = GenericConnectorState[ResourceState]


class IncrementalRecord(BaseDocument, abc.ABC, extra="allow"):
    @abc.abstractmethod
    def get_cursor(self) -> AwareDatetime: ...


def _floats(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _floats(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_floats(v) for v in value]
    return value


class TwentyRecord(IncrementalRecord, extra="allow"):
    PATH_TEMPLATE: ClassVar[str] = "/rest/{name_plural}"
    ITEMS_KEY: ClassVar[str] = "{name_plural}"
    SINCE_PARAM: ClassVar[str] = "updatedAt[gte]"
    BEFORE_PARAM: ClassVar[str] = "updatedAt[lt]"
    ORDER_BY: ClassVar[str] = "updatedAt[AscNullsFirst],id[AscNullsFirst]"
    INCLUDE_DELETED_FILTER: ClassVar[str] = (
        "or(deletedAt[is]:NULL,deletedAt[is]:NOT_NULL)"
    )

    id: str
    updatedAt: AwareDatetime
    deletedAt: AwareDatetime | None
    searchVector: Any = Field(default=None, exclude=True)

    def get_cursor(self) -> AwareDatetime:
        return self.updatedAt

    @model_validator(mode="before")
    @classmethod
    def restore_floats(cls, data: Any) -> Any:
        # The streaming parser reads fractional numbers as Decimal, which the CDK
        # would write out as strings; keep them numbers, as plain JSON parsing does.
        return _floats(data)

    @model_validator(mode="after")
    def set_operation(self) -> Self:
        # Assigning (rather than mutating) marks _meta as set, so the CDK's
        # exclude_unset serialization keeps it.
        if self.deletedAt is not None:
            self.meta_ = TwentyRecord.Meta(op="d")
        return self


class PageInfo(BaseModel):
    hasNextPage: bool
    endCursor: str | None

    @model_validator(mode="after")
    def require_next_cursor(self) -> Self:
        if self.hasNextPage and not self.endCursor:
            raise ValueError("A page with hasNextPage must have an endCursor.")
        return self


class RecordsRemainder(BaseModel):
    data: dict[str, list[TwentyRecord]] = Field(min_length=1, max_length=1)
    pageInfo: PageInfo

    def page_info(self, object_name: str) -> PageInfo:
        key = TwentyRecord.ITEMS_KEY.format(name_plural=object_name)
        if key not in self.data:
            raise ValueError(
                f"Expected object key '{key}' in response data; found keys: {list(self.data)}."
            )
        return self.pageInfo


class ObjectMetadata(BaseModel):
    PATH: ClassVar[str] = "/rest/metadata/objects"

    namePlural: str
    isActive: bool
    isSystem: bool
    isRemote: bool


class MetadataResponse(BaseModel):
    data: list[ObjectMetadata]
    pageInfo: PageInfo


class ErrorResponse(BaseModel):
    code: str
