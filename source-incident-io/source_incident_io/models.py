from typing import ClassVar, Generic, TypeVar

from pydantic import AwareDatetime, BaseModel, Field, create_model

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import AccessToken


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 0}
    )


ConnectorState = GenericConnectorState[ResourceState]


_TBaseDocument = TypeVar('_TBaseDocument', bound=BaseDocument)
TStream = TypeVar('TStream', bound='Stream')
TIncrementalStream = TypeVar('TIncrementalStream', bound='IncrementalStream')


class Response(BaseModel, Generic[_TBaseDocument]):
    resources: list[_TBaseDocument]

    class Config:
        extra = "allow"


class PaginatedResponse(Response[_TBaseDocument]):
    class PaginationMeta(BaseModel):
        page_size: int
        after: str | None = None

        class Config:
            extra = "allow"

    pagination_meta: PaginationMeta

    class Config:
        extra = "allow"


# Depending on the endpoint, the API response contains records under differently named fields, like "users" or "entities".
# create_response_model and create_paginated_response_model alias these fields to a common "resources" field to simplify
# handling within the connector.
def create_response_model(resource_model: type[TStream], field_name: str) -> type[Response[TStream]]:
    return create_model(
        f"{resource_model.__name__}Response",
        resources=(list[resource_model], Field(alias=field_name)),
        __base__=Response[resource_model]
    )

def create_paginated_response_model(resource_model: type[TStream], field_name: str) -> type[PaginatedResponse[TStream]]:
    return create_model(
        f"{resource_model.__name__}PaginatedResponse",
        resources=(list[resource_model], Field(alias=field_name)),
        __base__=PaginatedResponse[resource_model]
    )


class PaginatedMixin():
    page_size: ClassVar[int]


class Stream(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]
    response_field: ClassVar[str]


class FullRefreshStream(Stream):
    pass


class PaginatedFullRefreshStream(FullRefreshStream, PaginatedMixin):
    pass


class IncidentTimestamps(FullRefreshStream):
    name: ClassVar[str] = "incident_timestamps"
    path: ClassVar[str] = "v2/incident_timestamps"
    response_field: ClassVar[str] = "incident_timestamps"


class CatalogResources(FullRefreshStream):
    name: ClassVar[str] = "catalog_resources"
    path: ClassVar[str] = "v3/catalog_resources"
    response_field: ClassVar[str] = "resources"


class Users(PaginatedFullRefreshStream):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "v2/users"
    response_field: ClassVar[str] = "users"
    page_size: ClassVar[int] = 10_000


# IncidentAttachments is a child stream of Incidents.
class IncidentAttachments(FullRefreshStream):
    name: ClassVar[str] = "incident_attachments"
    path: ClassVar[str] = "v1/incident_attachments"
    response_field: ClassVar[str] = "incident_attachments"


class IncrementalStream(Stream):
    id: str
    updated_at: AwareDatetime


class PaginatedIncrementalStream(IncrementalStream, PaginatedMixin):
    pass


class CustomFields(IncrementalStream):
    name: ClassVar[str] = "custom_fields"
    path: ClassVar[str] = "v2/custom_fields"
    response_field: ClassVar[str] = "custom_fields"


class CatalogTypes(IncrementalStream):
    name: ClassVar[str] = "catalog_types"
    path: ClassVar[str] = "v3/catalog_types"
    response_field: ClassVar[str] = "catalog_types"


class IncidentStatuses(IncrementalStream):
    name: ClassVar[str] = "incident_statuses"
    path: ClassVar[str] = "v1/incident_statuses"
    response_field: ClassVar[str] = "incident_statuses"


class IncidentTypes(IncrementalStream):
    name: ClassVar[str] = "incident_types"
    path: ClassVar[str] = "v1/incident_types"
    response_field: ClassVar[str] = "incident_types"


class IncidentRoles(IncrementalStream):
    name: ClassVar[str] = "incident_roles"
    path: ClassVar[str] = "v1/incident_roles"
    response_field: ClassVar[str] = "incident_roles"


class Severities(IncrementalStream):
    name: ClassVar[str] = "severities"
    path: ClassVar[str] = "v1/severities"
    response_field: ClassVar[str] = "severities"


# CatalogEntries is a child stream of CatalogTypes
class CatalogEntries(IncrementalStream, PaginatedMixin):
    name: ClassVar[str] = "catalog_entries"
    path: ClassVar[str] = "v3/catalog_entries"
    response_field: ClassVar[str] = "catalog_entries"
    page_size: ClassVar[int] = 250


class Incidents(IncrementalStream, PaginatedMixin):
    name: ClassVar[str] = "incidents"
    path: ClassVar[str] = "v2/incidents"
    response_field: ClassVar[str] = "incidents"
    page_size: ClassVar[int] = 500


FULL_REFRESH_RESOURCES: list[type[FullRefreshStream]] = [
    CatalogResources,
    IncidentAttachments,
    IncidentTimestamps,
    Users,
]

INCREMENTAL_RESOURCES: list[type[IncrementalStream]] = [
    CatalogTypes,
    CustomFields,
    IncidentStatuses,
    IncidentTypes,
    IncidentRoles,
    Severities,
    Incidents,
    CatalogEntries,
]
