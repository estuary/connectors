"""Pydantic models for Asana source connector."""

from abc import ABCMeta
from datetime import UTC, datetime, timedelta
from typing import Annotated, ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field

ConnectorState = GenericConnectorState[ResourceState]


class EndpointConfig(BaseModel):
    """Configuration for connecting to Asana."""

    api_key: Annotated[
        str,
        Field(
            description="Personal Access Token for Asana.",
            title="API Key",
            json_schema_extra={"secret": True},
        ),
    ]

    class Advanced(BaseModel):
        base_url: Annotated[
            str,
            Field(
                default="https://app.asana.com/api/1.0",
                description="API base URL.",
                title="Base URL",
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


# --- Response envelope ---

class NextPage(BaseModel, extra="allow"):
    """Pagination cursor from Asana API responses."""
    offset: str | None = None
    path: str | None = None
    uri: str | None = None


class AsanaPageMeta(BaseModel, extra="allow"):
    """Top-level pagination metadata wrapping next_page."""
    next_page: NextPage | None = None


# --- Base entity ---

class BaseEntity(BaseDocument, extra="allow", metaclass=ABCMeta):
    """Base for all Asana entities. Uses gid as primary key."""
    gid: str


# --- Workspace-scoped resources ---
# Fetched by iterating over all workspaces the authenticated user belongs to.

class Workspace(BaseEntity):
    pass


class User(BaseEntity):
    pass


class Team(BaseEntity):
    pass


class Project(BaseEntity):
    pass


class Tag(BaseEntity):
    pass


class Portfolio(BaseEntity):
    pass


class Goal(BaseEntity):
    pass


class CustomField(BaseEntity):
    pass


class TimePeriod(BaseEntity):
    pass


class ProjectTemplate(BaseEntity):
    pass


# --- Project-scoped resources ---
# Fetched by iterating over all projects.

class Task(BaseEntity):
    pass


class Section(BaseEntity):
    pass


class StatusUpdate(BaseEntity):
    pass


class Attachment(BaseEntity):
    pass


# --- Task-scoped resources ---

class Story(BaseEntity):
    pass


# --- Membership resources ---

class Membership(BaseEntity):
    pass


class TeamMembership(BaseEntity):
    pass


# --- Sync-token models ---
# For Asana's Events API: change notifications with opaque sync tokens.

class ChangeEvent(BaseModel, extra="allow"):
    """A change notification from GET /events."""
    resource: dict  # {"gid": str, "resource_type": str}
    action: str     # "created", "changed", "deleted", "added", "removed"


class SyncTokenResponse(BaseModel, extra="allow"):
    """Response envelope from GET /events."""
    data: list[ChangeEvent] = []
    sync: str
    has_more: bool = False


# --- Tombstone ---

class _Tombstone(BaseDocument, extra="allow"):
    gid: str = ""


TOMBSTONE = _Tombstone(_meta=_Tombstone.Meta(op="d"))
