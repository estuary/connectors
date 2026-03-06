"""Pydantic models for Asana source connector."""

from abc import ABCMeta, abstractmethod
from typing import Annotated, ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from pydantic import BaseModel, Field

ConnectorState = GenericConnectorState[ResourceState]

API_PAGE_LIMIT = 100


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


# =============================================================================
# Entity Base Classes
# =============================================================================

class BaseEntity(BaseDocument, extra="allow", metaclass=ABCMeta):
    """Base for all Asana entities. Uses gid as primary key."""
    resource_name: ClassVar[str]
    api_path: ClassVar[str]
    gid: str

    @classmethod
    def get_detail_url(cls, base_url: str, gid: str) -> str:
        return f"{base_url}/{cls.api_path}/{gid}"


class TopLevelEntity(BaseEntity, metaclass=ABCMeta):
    """Entity fetched from a top-level API endpoint (no parent scope)."""

    @classmethod
    def get_url(cls, base_url: str) -> str:
        return f"{base_url}/{cls.api_path}?limit={API_PAGE_LIMIT}"


class WorkspaceScopedEntity(BaseEntity, metaclass=ABCMeta):
    """Entity fetched by iterating over workspaces."""
    # HTTP status codes to silently skip per-parent scope. Asana returns different
    # errors depending on workspace/project plan tier or type — e.g. 402 for premium
    # features (portfolios, goals, custom fields) and 403/404 for endpoints that only
    # work on organization-type workspaces (teams). Rather than failing the entire
    # capture, we log and skip the inaccessible scope.
    tolerated_errors: ClassVar[frozenset[int]] = frozenset()
    deduplicate: ClassVar[bool] = False

    @classmethod
    def get_url(cls, base_url: str, ws_gid: str) -> str:
        return f"{base_url}/{cls.api_path}?workspace={ws_gid}&limit={API_PAGE_LIMIT}"


class ProjectScopedEntity(BaseEntity, metaclass=ABCMeta):
    """Entity fetched by iterating over projects."""
    tolerated_errors: ClassVar[frozenset[int]] = frozenset()  # see WorkspaceScopedEntity

    @classmethod
    def get_url(cls, base_url: str, project_gid: str) -> str:
        return f"{base_url}/{cls.api_path}?parent={project_gid}&limit={API_PAGE_LIMIT}"


# =============================================================================
# Top-Level Entities
# =============================================================================

class Workspace(TopLevelEntity):
    resource_name: ClassVar[str] = "Workspaces"
    api_path: ClassVar[str] = "workspaces"


# =============================================================================
# Workspace-Scoped Entities
# =============================================================================

class User(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Users"
    api_path: ClassVar[str] = "users"
    deduplicate: ClassVar[bool] = True


class Team(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Teams"
    api_path: ClassVar[str] = "teams"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({403, 404})

    @classmethod
    def get_url(cls, base_url: str, ws_gid: str) -> str:
        return f"{base_url}/organizations/{ws_gid}/teams?limit={API_PAGE_LIMIT}"


class Project(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Projects"
    api_path: ClassVar[str] = "projects"

    @classmethod
    def get_url(cls, base_url: str, ws_gid: str) -> str:
        return f"{base_url}/projects?workspace={ws_gid}&limit={API_PAGE_LIMIT}&archived=false"

    @classmethod
    def get_events_url(cls, base_url: str, project_gid: str, sync: str | None = None) -> str:
        url = f"{base_url}/projects/{project_gid}/events"
        if sync:
            url += f"?sync={sync}"
        return url


class Tag(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Tags"
    api_path: ClassVar[str] = "tags"


class Portfolio(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Portfolios"
    api_path: ClassVar[str] = "portfolios"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({402})

    @classmethod
    def get_url(cls, base_url: str, ws_gid: str) -> str:
        return f"{base_url}/portfolios?workspace={ws_gid}&owner=me&limit={API_PAGE_LIMIT}"


class Goal(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "Goals"
    api_path: ClassVar[str] = "goals"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({402})


class CustomField(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "CustomFields"
    api_path: ClassVar[str] = "custom_fields"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({402})

    @classmethod
    def get_url(cls, base_url: str, ws_gid: str) -> str:
        return f"{base_url}/workspaces/{ws_gid}/custom_fields?limit={API_PAGE_LIMIT}"


class TimePeriod(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "TimePeriods"
    api_path: ClassVar[str] = "time_periods"


class ProjectTemplate(WorkspaceScopedEntity):
    resource_name: ClassVar[str] = "ProjectTemplates"
    api_path: ClassVar[str] = "project_templates"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({400, 402})


# =============================================================================
# Project-Scoped Entities
# =============================================================================

class Task(ProjectScopedEntity):
    resource_name: ClassVar[str] = "Tasks"
    api_path: ClassVar[str] = "tasks"
    event_type: ClassVar[str] = "task"

    @classmethod
    def get_url(cls, base_url: str, project_gid: str) -> str:
        return f"{base_url}/tasks?project={project_gid}&limit={API_PAGE_LIMIT}"


class Section(ProjectScopedEntity):
    resource_name: ClassVar[str] = "Sections"
    api_path: ClassVar[str] = "sections"
    event_type: ClassVar[str] = "section"

    @classmethod
    def get_url(cls, base_url: str, project_gid: str) -> str:
        return f"{base_url}/projects/{project_gid}/sections?limit={API_PAGE_LIMIT}"


class StatusUpdate(ProjectScopedEntity):
    resource_name: ClassVar[str] = "StatusUpdates"
    api_path: ClassVar[str] = "status_updates"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({403, 404})


class Attachment(ProjectScopedEntity):
    resource_name: ClassVar[str] = "Attachments"
    api_path: ClassVar[str] = "attachments"
    event_type: ClassVar[str] = "attachment"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({403, 404})


class Story(ProjectScopedEntity):
    """Stories are task-scoped but modeled as project-scoped with custom fetch logic."""
    resource_name: ClassVar[str] = "Stories"
    api_path: ClassVar[str] = "stories"
    event_type: ClassVar[str] = "story"

    @classmethod
    def get_url(cls, base_url: str, task_gid: str) -> str:
        return f"{base_url}/tasks/{task_gid}/stories?limit={API_PAGE_LIMIT}"


class Membership(ProjectScopedEntity):
    resource_name: ClassVar[str] = "Memberships"
    api_path: ClassVar[str] = "memberships"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({403, 404})


class TeamMembership(WorkspaceScopedEntity):
    """Team memberships require iterating workspace → teams → memberships."""
    resource_name: ClassVar[str] = "TeamMemberships"
    api_path: ClassVar[str] = "team_memberships"
    tolerated_errors: ClassVar[frozenset[int]] = frozenset({403, 404})

    @classmethod
    def get_url(cls, base_url: str, team_gid: str) -> str:
        return f"{base_url}/team_memberships?team={team_gid}&limit={API_PAGE_LIMIT}"


# --- API response envelopes ---

class AsanaDetailResponse(BaseModel, extra="allow"):
    """Envelope for single-object Asana API responses: {"data": {...}}."""
    data: dict


# --- Sync-token models ---
# Asana Events API: https://developers.asana.com/reference/events
# Uses opaque sync tokens per project. Tokens expire after 24h (or sooner under load).
# Expired tokens return 412 with a new token.

class ChangeEvent(BaseModel, extra="allow"):
    """A change notification from GET /events."""
    resource: dict
    action: str


class SyncTokenResponse(BaseModel, extra="allow"):
    """Response envelope from GET /events."""
    data: list[ChangeEvent] = []
    sync: str
    has_more: bool = False


# --- Tombstone ---

class _Tombstone(BaseDocument, extra="allow"):
    gid: str = ""


TOMBSTONE = _Tombstone(_meta=_Tombstone.Meta(op="d"))
