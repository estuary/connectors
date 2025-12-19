import re
from datetime import datetime, timedelta, timezone
from typing import Annotated, ClassVar, Generic, TypeVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import AccessToken

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field


_TZ_PATTERN = re.compile(r'(Z|[+-]\d{2}:?\d{2})$')


def _assume_utc(v: str | datetime) -> str | datetime:
    """Assume naive datetime strings are UTC by appending 'Z'."""
    if isinstance(v, str) and not _TZ_PATTERN.search(v):
        return v + 'Z'
    return v


UtcDatetime = Annotated[AwareDatetime, BeforeValidator(_assume_utc)]


EXPORT_RETENTION_LIMIT = timedelta(days=365) # 12 months


def default_start_date():
    dt = datetime.now(timezone.utc) - EXPORT_RETENTION_LIMIT
    return dt


class ApiKey(AccessToken):
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    bot_handle: str = Field(
        description="Your Ada bot handle. This can be found in the URL of your bot's dashboard. ex: {BOT_HANDLE}.ada.support",
        title="Bot Handle",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 12 months before the present date. Note: due to Ada's data retention limits, only the past 12 months of data can be captured.",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class AdaResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]


class ExportResource(AdaResource):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    lower_bound_param: ClassVar[str]
    upper_bound_param: ClassVar[str]

    id: str = Field(..., alias="_id")

    @property
    def cursor_date(self) -> AwareDatetime:
        raise NotImplementedError


class ResponseMeta(BaseModel, extra="allow"):
    next_page_uri: str | None = None


class AdaResponse(BaseModel, extra="allow"):
    data: list[AdaResource]
    meta: ResponseMeta | None = None


TExportResource = TypeVar("TExportResource", bound=ExportResource)


class ExportResponse(BaseModel, Generic[TExportResource], extra="allow"):
    data: list[TExportResource]
    meta: ResponseMeta | None = None


# Full refresh streams
class PlatformIntegrations(AdaResource):
    name: ClassVar[str] = "platform_integrations"
    path: ClassVar[str] = "platform-integrations"


class Articles(AdaResource):
    name: ClassVar[str] = "articles"
    path: ClassVar[str] = "knowledge/articles"


class Sources(AdaResource):
    name: ClassVar[str] = "sources"
    path: ClassVar[str] = "knowledge/sources"


class Tags(AdaResource):
    name: ClassVar[str] = "tags"
    path: ClassVar[str] = "knowledge/tags"


FULL_REFRESH_RESOURCES: list[type[AdaResource]] = [
    Articles,
    PlatformIntegrations,
    Sources,
    Tags
]


# Incremental streams
class Conversations(ExportResource):
    name: ClassVar[str] = "conversations"
    path: ClassVar[str] = "conversations"
    lower_bound_param: ClassVar[str] = "updated_since"
    upper_bound_param: ClassVar[str] = "updated_to"

    # date_created and date_updated are UTC datetimes in Ada, but
    # the conversations export API response doesn't append a timezone.
    # We use the custom UtcDatetime type to append a 'Z' at the end 
    # of the timezone-less strings we receive from the API before
    # Pydantic converts these fields to AwareDatetimes.
    date_created: UtcDatetime
    date_updated: UtcDatetime

    @property
    def cursor_date(self) -> AwareDatetime:
        return self.date_updated


class Messages(ExportResource):
    name: ClassVar[str] = "messages"
    path: ClassVar[str] = "messages"
    lower_bound_param: ClassVar[str] = "created_since"
    upper_bound_param: ClassVar[str] = "created_to"

    date_created: AwareDatetime

    @property
    def cursor_date(self) -> AwareDatetime:
        return self.date_created


INCREMENTAL_EXPORT_RESOURCES: list[type[ExportResource]] = [
    Conversations,
    Messages,
]

