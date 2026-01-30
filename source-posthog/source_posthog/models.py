"""
Pydantic models for PostHog connector configuration and data types.
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

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

# Re-exported from estuary_cdk for convenience
__all__ = [
    "Annotation",
    "Cohort",
    "ConnectorState",
    "EndpointConfig",
    "Event",
    "FeatureFlag",
    "INCREMENTAL_RESOURCES",
    "Organization",
    "Person",
    "Project",
    "ResourceConfig",
    "ResourceState",
    "SNAPSHOT_RESOURCES",
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

    advanced: Annotated[
        Advanced,
        Field(
            default_factory=Advanced,
            title="Advanced",
            json_schema_extra={"advanced": True},
        ),
    ]


# Data models for PostHog resources
# PostHog includes system defined properties prefixed with a $ e.g. $ip,
# $geoip_time_zone according to how the PostHog project is configured. The set
# of included fields can vary widely between projects. User defined custom
# properties lack the $ prefix e.g. country, plan. Therefor we set extra="allow"
# to let schema inference do its thing.
class Project(BaseDocument, extra="allow"):
    """PostHog project."""

    id: int
    name: str
    organization_id: str | None = Field(alias="organization", default=None)
    api_token: str | None = None
    timezone: str = "UTC"
    created_at: AwareDatetime | None = None


class Event(BaseDocument, extra="allow"):
    """PostHog event."""

    id: str
    project_id: int
    event: str
    distinct_id: str
    properties: dict[str, Any] = Field(default_factory=dict)
    timestamp: AwareDatetime
    elements: list[dict[str, Any]] = Field(default_factory=list)


class Person(BaseDocument, extra="allow"):
    """PostHog person."""

    id: str
    project_id: int
    distinct_ids: list[str] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)
    created_at: AwareDatetime | None = None


class Cohort(BaseDocument, extra="allow"):
    """PostHog cohort."""

    id: int
    project_id: int
    name: str
    description: str = ""
    groups: list[dict[str, Any]] = Field(default_factory=list)
    is_static: bool = False
    count: int | None = None
    created_at: AwareDatetime | None = None


class FeatureFlag(BaseDocument, extra="allow"):
    """PostHog feature flag."""

    id: int
    project_id: int
    key: str
    name: str = ""
    active: bool = True
    filters: dict[str, Any] = Field(default_factory=dict)
    rollout_percentage: int | None = None
    created_at: AwareDatetime | None = None


class Annotation(BaseDocument, extra="allow"):
    """PostHog annotation."""

    id: int
    project_id: int
    content: str
    date_marker: AwareDatetime
    scope: str = "project"
    creation_type: str = "USR"
    created_at: AwareDatetime | None = None


class Organization(BaseDocument, extra="allow"):
    """PostHog organization."""

    id: str
    name: str
    slug: str | None = None
    created_at: AwareDatetime | None = None
    updated_at: AwareDatetime | None = None
    membership_level: int | None = None


def default_start_date() -> datetime:
    """Return default start date (30 days ago)."""
    return datetime.now(tz=UTC) - timedelta(days=30)


# Resource type definitions: (resource_type, model_class, key_field, name, tombstone)
# Tombstones use project_id=0 for project-scoped resources.
SNAPSHOT_RESOURCES: list[tuple[str, type[BaseDocument], str, str, BaseDocument]] = [
    ("organizations", Organization, "/id", "Organizations", Organization(id="", name="")),
    ("projects", Project, "/id", "Projects", Project(id=0, name="")),
    ("persons", Person, "/id", "Persons", Person(id="", project_id=0)),
    ("cohorts", Cohort, "/id", "Cohorts", Cohort(id=0, project_id=0, name="")),
    ("feature_flags", FeatureFlag, "/id", "FeatureFlags", FeatureFlag(id=0, project_id=0, key="")),
    (
        "annotations",
        Annotation,
        "/id",
        "Annotations",
        # datetime(1, 1, 1, 0, 0, tzinfo=UTC)
        Annotation(id=0, project_id=0, content="", date_marker=datetime.min.replace(tzinfo=UTC)),
    ),
]

# Incremental resource definitions: (resource_type, model_class, key_field, name)
INCREMENTAL_RESOURCES: list[tuple[str, type[BaseDocument], str, str]] = [
    ("events", Event, "/id", "Events"),
]
