"""
Pydantic models for PostHog connector configuration and data types.

Uses a generic hierarchy to minimize boilerplate:
- PostHogEntity[T]: Plain REST API entities (Organization, Project)
- ProjectEntity[T]: Project-scoped REST API entities (Cohort, FeatureFlag, Annotation)
- HogQLEntity[T]: HogQL-queried entities (Event, Person)
"""

import functools
from abc import ABCMeta
from datetime import UTC, datetime, timedelta
from typing import Annotated, ClassVar
from urllib.parse import urljoin

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field

# Use CDK's generic ConnectorState with ResourceState
ConnectorState = GenericConnectorState[ResourceState]

# Primary key type constraint
type PostHogPrimaryKey = int | str

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
    "PostHogEntity",
    "Project",
    "ProjectEntity",
    "ResourceConfig",
    "ResourceState",
    "RestResponseMeta",
    "default_start_date",
]


class EndpointConfig(BaseModel):
    """Configuration for connecting to PostHog."""

    personal_api_key: Annotated[
        str,
        Field(
            description="PostHog Personal API Key (starts with phx_). "
            "Required for reading data from the API.",
            title="Personal API Key",
            json_schema_extra={"secret": True},
        ),
    ]

    organization_id: Annotated[
        str,
        Field(
            description="PostHog Organization ID. "
            "The connector captures data from all projects in this organization.",
            title="Organization ID",
        ),
    ]

    class Advanced(BaseModel):
        """Advanced configuration options."""

        base_url: Annotated[
            str,
            Field(
                default="https://app.posthog.com",
                description="PostHog API base URL. Use https://eu.posthog.com for EU cloud.",
                title="Base URL",
            ),
        ]

        start_date: Annotated[
            str | None,
            Field(
                default=None,
                description="Start date for historical data capture (ISO 8601 format). "
                "If not specified, captures from 30 days ago.",
                title="Start Date",
            ),
        ]

        page_size: Annotated[
            int,
            Field(
                default=10000,
                description="Number of records to fetch per API request (1000-50000). "
                "Higher values are faster but use more memory.",
                title="Page Size",
                ge=1000,
                le=50000,
            ),
        ]

    advanced: Annotated[
        Advanced,
        Field(
            default_factory=Advanced,
            title="Advanced",
            json_schema_extra={"advanced": True},
        ),
    ]


# =============================================================================
# Entity Base Classes
# =============================================================================


class BasePostHogEntity[T: PostHogPrimaryKey](BaseDocument, extra="allow", metaclass=ABCMeta):
    """Base class for all PostHog entities."""

    resource_name: ClassVar[str]

    id: T


class PostHogEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched from plain REST API endpoint."""

    api_path: ClassVar[str]

    @classmethod
    def get_api_endpoint_url(cls, base_url: str) -> str:
        """Build API endpoint URL for this entity type."""
        return functools.reduce(urljoin, [base_url, "api/", cls.api_path + "/"])


class ProjectEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched from project-scoped REST API endpoint."""

    api_path: ClassVar[str]

    @classmethod
    def get_api_endpoint_url(cls, base_url: str, project_id: int) -> str:
        """Build API endpoint URL for this entity type within a project."""
        return functools.reduce(
            urljoin,
            [base_url, "api/", "projects/", f"{project_id}/", cls.api_path + "/"],
        )


class HogQLEntity[T: PostHogPrimaryKey](BasePostHogEntity[T], metaclass=ABCMeta):
    """Entity fetched via HogQL Query API."""

    table_name: ClassVar[str]


# =============================================================================
# REST API Entities
# =============================================================================


class Organization(PostHogEntity[str]):
    """PostHog organization."""

    resource_name: ClassVar[str] = "Organizations"
    api_path: ClassVar[str] = "organizations"


class Project(PostHogEntity[int]):
    """PostHog project."""

    resource_name: ClassVar[str] = "Projects"
    api_path: ClassVar[str] = "projects"


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

    timestamp: AwareDatetime  # Used for incremental cursor


class Person(HogQLEntity[str]):
    """PostHog person."""

    resource_name: ClassVar[str] = "Persons"
    table_name: ClassVar[str] = "persons"

    created_at: AwareDatetime | None = None  # Used for pagination


# =============================================================================
# API Response Metadata
# =============================================================================


class RestResponseMeta(BaseModel):
    """Metadata from paginated REST API responses."""

    next: str | None = None
    previous: str | None = None
    count: int | None = None


# =============================================================================
# Utilities
# =============================================================================


def default_start_date() -> datetime:
    """Return default start date (30 days ago)."""
    return datetime.now(tz=UTC) - timedelta(days=30)
