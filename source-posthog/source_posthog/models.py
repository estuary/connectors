"""
Pydantic models for PostHog connector configuration and data types.
"""

import functools
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, ClassVar, override
from urllib.parse import urljoin

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    ValidationInfo,
    model_validator,
)

# Use CDK's generic ConnectorState with ResourceState
ConnectorState = GenericConnectorState[ResourceState]

# Primary key type constraint
type PostHogPrimaryKey = int | str


@dataclass(frozen=True, slots=True)
class ProjectIdValidationContext:
    """Validation context carrying the project_id for document construction."""

    project_id: int


# Re-exported from estuary_cdk for convenience
__all__ = [
    "Annotation",
    "BasePostHogEntity",
    "Cohort",
    "ConnectorState",
    "EndpointConfig",
    "Event",
    "FeatureFlag",
    "HogQLEntity",
    "Organization",
    "Person",
    "PersonalApiKeyInfo",
    "PostHogEntity",
    "Project",
    "ProjectEntity",
    "ProjectIdValidationContext",
    "ResourceConfig",
    "ResourceState",
    "RestResponseMeta",
    "default_start_date",
]


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Personal API Key",
        description="PostHog Personal API Key (starts with phx_). ",
        json_schema_extra={"order": 0},
    )
    organization_id: str = Field(
        title="Organization ID",
        description="PostHog Organization UUID. "
        + "The connector captures data from all projects in this organization.",
        json_schema_extra={"order": 1},
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. "
        + "Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        default_factory=default_start_date,
        ge=EPOCH,
        json_schema_extra={"order": 2},
    )

    class Advanced(BaseModel):
        base_url: str = Field(
            title="Base URL",
            description="PostHog API base URL. Use https://eu.posthog.com for EU cloud.",
            default="https://app.posthog.com",
        )

    advanced: Annotated[
        Advanced,
        Field(
            title="Advanced Config",
            description="Advanced settings for the connector.",
            default_factory=Advanced,
            json_schema_extra={"advanced": True, "order": 3},
        ),
    ]


# =============================================================================
# Entity Base Classes
# =============================================================================


class BasePostHogEntity[T: PostHogPrimaryKey](
    BaseDocument, extra="allow", metaclass=ABCMeta
):
    """Base class for all PostHog entities."""

    resource_name: ClassVar[str]

    id: T


class PostHogEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched from plain REST API endpoint."""

    api_path: ClassVar[str]

    @classmethod
    @abstractmethod
    def get_api_endpoint_url(cls, config: EndpointConfig) -> str: ...


class ProjectEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched from project-scoped REST API endpoint."""

    api_path: ClassVar[str]

    @classmethod
    def get_api_endpoint_url(cls, base_url: str, project_id: int) -> str:
        return functools.reduce(
            urljoin,
            [
                base_url,
                "api/",
                "projects/",
                f"{project_id}/",
                f"{cls.api_path}/",
                "?order=-updated_at",
            ],
        )


class HogQLEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched via HogQL Query API."""

    table_name: ClassVar[str]
    cursor_column: ClassVar[str]

    @classmethod
    def get_api_endpoint_url(cls, base_url: str, project_id: int) -> str:
        return functools.reduce(
            urljoin, [base_url, "api/", "projects/", f"{project_id}/", "query"]
        )

    @abstractmethod
    def get_cursor(self) -> AwareDatetime: ...


# =============================================================================
# REST API Entities
# =============================================================================


class Organization(PostHogEntity[str]):
    """PostHog organization."""

    resource_name: ClassVar[str] = "Organizations"
    api_path: ClassVar[str] = "organizations"

    name: str

    @classmethod
    @override
    def get_api_endpoint_url(cls, config: EndpointConfig) -> str:
        return functools.reduce(
            urljoin, [config.advanced.base_url, "api/", cls.api_path + "/"]
        )

    @property
    def display_name(self) -> str:
        return f"{self.name} ({self.id})"


class Project(PostHogEntity[int]):
    """PostHog project."""

    resource_name: ClassVar[str] = "Projects"
    api_path: ClassVar[str] = "projects"

    @classmethod
    @override
    def get_api_endpoint_url(cls, config: EndpointConfig) -> str:
        """Build API endpoint URL for this entity type."""
        return functools.reduce(
            urljoin,
            [
                config.advanced.base_url,
                "api/",
                "organizations/",
                f"{config.organization_id}/",
                f"{cls.api_path}/",
            ],
        )


# =============================================================================
# Project-Scoped REST API Entities
# =============================================================================


class Cohort(ProjectEntity[int]):
    """PostHog cohort."""

    resource_name: ClassVar[str] = "Cohorts"
    api_path: ClassVar[str] = "cohorts"


class FeatureFlag(ProjectEntity[int]):
    """PostHog feature flag."""

    resource_name: ClassVar[str] = "FeatureFlags"
    api_path: ClassVar[str] = "feature_flags"

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)
        project_id: int | None = Field(
            default=None,
            description="The PostHog project this document belongs to",
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=lambda: FeatureFlag.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )

    updated_at: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def _inject_project_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, ProjectIdValidationContext):
            meta = data.get("_meta") or {}
            meta["project_id"] = info.context.project_id
            data["_meta"] = meta
        return data

    def get_cursor(self) -> AwareDatetime:
        return self.updated_at


class Annotation(ProjectEntity[int]):
    """PostHog annotation."""

    resource_name: ClassVar[str] = "Annotations"
    api_path: ClassVar[str] = "annotations"


# =============================================================================
# HogQL Entities
# =============================================================================


class Event(HogQLEntity[str]):
    """PostHog event."""

    resource_name: ClassVar[str] = "Events"
    table_name: ClassVar[str] = "events"
    cursor_column: ClassVar[str] = "timestamp"

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)
        project_id: int | None = Field(
            default=None,
            description="The PostHog project this document belongs to",
        )

    meta_: Meta = Field(
        default_factory=lambda: Event.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )

    id: str = Field(alias="uuid")
    timestamp: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def _inject_project_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, ProjectIdValidationContext):
            meta = data.get("_meta") or {}
            meta["project_id"] = info.context.project_id
            data["_meta"] = meta
        return data

    @override
    def get_cursor(self) -> AwareDatetime:
        return self.timestamp


class Person(HogQLEntity[str]):
    """PostHog person."""

    resource_name: ClassVar[str] = "Persons"
    table_name: ClassVar[str] = "persons"
    cursor_column: ClassVar[str] = "created_at"

    created_at: AwareDatetime

    @override
    def get_cursor(self) -> AwareDatetime:
        return self.created_at


# =============================================================================
# API Response Metadata
# =============================================================================


class PersonalApiKeyInfo(BaseModel):
    scopes: set[str]


class RestResponseMeta(BaseModel):
    next: str | None = None


class HogQLResponseMeta(BaseModel):
    columns: list[str]


HogQLRow = RootModel[list[JsonValue]]
