"""Unit tests for Asana connector models.

No live API credentials needed — these test model metadata and URL construction.
"""

import pytest

from source_asana_native.models import (
    API_PAGE_LIMIT,
    Attachment,
    CustomField,
    Goal,
    Membership,
    Portfolio,
    Project,
    ProjectScopedEntity,
    ProjectTemplate,
    Section,
    StatusUpdate,
    Story,
    Tag,
    Task,
    Team,
    TeamMembership,
    TimePeriod,
    TopLevelEntity,
    User,
    Workspace,
    WorkspaceScopedEntity,
)

BASE = "https://app.asana.com/api/1.0"


# =============================================================================
# All concrete entity models
# =============================================================================

ALL_MODELS = [
    Workspace,
    User,
    Team,
    Project,
    Tag,
    Portfolio,
    Goal,
    CustomField,
    TimePeriod,
    ProjectTemplate,
    Task,
    Section,
    StatusUpdate,
    Attachment,
    Story,
    Membership,
    TeamMembership,
]


class TestClassVars:
    """Every entity must declare resource_name and api_path."""

    @pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
    def test_resource_name_set(self, model):
        assert hasattr(model, "resource_name")
        assert isinstance(model.resource_name, str)
        assert len(model.resource_name) > 0

    @pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
    def test_api_path_set(self, model):
        assert hasattr(model, "api_path")
        assert isinstance(model.api_path, str)
        assert len(model.api_path) > 0


class TestEventType:
    """Incremental models must declare event_type."""

    @pytest.mark.parametrize("model,expected", [
        (Task, "task"),
        (Section, "section"),
        (Attachment, "attachment"),
        (Story, "story"),
    ])
    def test_event_type(self, model, expected):
        assert model.event_type == expected

    def test_snapshot_models_have_no_event_type(self):
        snapshot_only = [Workspace, User, Team, Project, Tag, Portfolio,
                         Goal, CustomField, TimePeriod, ProjectTemplate,
                         StatusUpdate, Membership, TeamMembership]
        for model in snapshot_only:
            assert not hasattr(model, "event_type"), (
                f"{model.__name__} should not have event_type"
            )


class TestToleratedErrors:

    def test_defaults_to_empty(self):
        for model in [User, Project, Tag, TimePeriod, Task, Section]:
            assert model.tolerated_errors == frozenset(), model.__name__

    @pytest.mark.parametrize("model,expected", [
        (Team, frozenset({403, 404})),
        (Portfolio, frozenset({402})),
        (Goal, frozenset({402})),
        (CustomField, frozenset({402})),
        (ProjectTemplate, frozenset({400, 402})),
        (StatusUpdate, frozenset({403, 404})),
        (Attachment, frozenset({403, 404})),
        (Membership, frozenset({403, 404})),
        (TeamMembership, frozenset({403, 404})),
    ])
    def test_tolerated_errors(self, model, expected):
        assert model.tolerated_errors == expected


class TestDeduplicate:

    def test_user_deduplicates(self):
        assert User.deduplicate is True

    def test_others_do_not_deduplicate(self):
        for model in [Team, Project, Tag, Portfolio, Goal, CustomField,
                       TimePeriod, ProjectTemplate, TeamMembership]:
            assert model.deduplicate is False, model.__name__


# =============================================================================
# URL construction
# =============================================================================

class TestGetDetailUrl:
    """get_detail_url is inherited from BaseEntity and uses api_path."""

    @pytest.mark.parametrize("model,expected_path", [
        (Task, "tasks"),
        (Story, "stories"),
        (Section, "sections"),
        (Attachment, "attachments"),
        (User, "users"),
        (Workspace, "workspaces"),
    ])
    def test_detail_url(self, model, expected_path):
        url = model.get_detail_url(BASE, "12345")
        assert url == f"{BASE}/{expected_path}/12345"


class TestTopLevelGetUrl:

    def test_workspace_url(self):
        url = Workspace.get_url(BASE)
        assert url == f"{BASE}/workspaces?limit={API_PAGE_LIMIT}"


class TestWorkspaceScopedGetUrl:
    """Workspace-scoped models build URLs with workspace gid."""

    def test_default_pattern(self):
        """Models using the default get_url: /{api_path}?workspace={gid}"""
        for model in [User, Tag, Goal, TimePeriod]:
            url = model.get_url(BASE, "ws123")
            assert f"?workspace=ws123" in url, model.__name__
            assert f"limit={API_PAGE_LIMIT}" in url, model.__name__

    def test_team_uses_organizations_path(self):
        url = Team.get_url(BASE, "ws123")
        assert url == f"{BASE}/organizations/ws123/teams?limit={API_PAGE_LIMIT}"

    def test_project_includes_archived_false(self):
        url = Project.get_url(BASE, "ws123")
        assert "archived=false" in url
        assert "workspace=ws123" in url

    def test_portfolio_includes_owner_me(self):
        url = Portfolio.get_url(BASE, "ws123")
        assert "owner=me" in url
        assert "workspace=ws123" in url

    def test_custom_field_uses_workspaces_path(self):
        url = CustomField.get_url(BASE, "ws123")
        assert url == f"{BASE}/workspaces/ws123/custom_fields?limit={API_PAGE_LIMIT}"

    def test_team_membership_uses_team_param(self):
        url = TeamMembership.get_url(BASE, "team123")
        assert url == f"{BASE}/team_memberships?team=team123&limit={API_PAGE_LIMIT}"


class TestProjectScopedGetUrl:
    """Project-scoped models build URLs with project/parent gid."""

    def test_default_pattern_uses_parent(self):
        """Models using the default get_url: /{api_path}?parent={gid}"""
        for model in [StatusUpdate, Attachment, Membership]:
            url = model.get_url(BASE, "proj123")
            assert f"?parent=proj123" in url, model.__name__
            assert f"limit={API_PAGE_LIMIT}" in url, model.__name__

    def test_task_uses_project_param(self):
        url = Task.get_url(BASE, "proj123")
        assert url == f"{BASE}/tasks?project=proj123&limit={API_PAGE_LIMIT}"

    def test_section_uses_projects_path(self):
        url = Section.get_url(BASE, "proj123")
        assert url == f"{BASE}/projects/proj123/sections?limit={API_PAGE_LIMIT}"

    def test_story_uses_tasks_path(self):
        url = Story.get_url(BASE, "task123")
        assert url == f"{BASE}/tasks/task123/stories?limit={API_PAGE_LIMIT}"


class TestProjectEventsUrl:

    def test_without_sync_token(self):
        url = Project.get_events_url(BASE, "proj123")
        assert url == f"{BASE}/projects/proj123/events"

    def test_with_sync_token(self):
        url = Project.get_events_url(BASE, "proj123", sync="abc123")
        assert url == f"{BASE}/projects/proj123/events?sync=abc123"

    def test_empty_sync_token_omitted(self):
        url = Project.get_events_url(BASE, "proj123", sync="")
        assert "?sync=" not in url


# =============================================================================
# Scope hierarchy
# =============================================================================

class TestScopeHierarchy:
    """Verify each model inherits from the correct scope base class."""

    def test_top_level(self):
        assert issubclass(Workspace, TopLevelEntity)

    @pytest.mark.parametrize("model", [
        User, Team, Project, Tag, Portfolio, Goal,
        CustomField, TimePeriod, ProjectTemplate, TeamMembership,
    ])
    def test_workspace_scoped(self, model):
        assert issubclass(model, WorkspaceScopedEntity)

    @pytest.mark.parametrize("model", [
        Task, Section, StatusUpdate, Attachment, Story, Membership,
    ])
    def test_project_scoped(self, model):
        assert issubclass(model, ProjectScopedEntity)
