"""Tests for PostHog API connectivity and data access."""

import pytest

from .data_injection import PostHogClient


class TestAPIConnectivity:
    """Tests for basic API connectivity."""

    def test_api_authentication(self, posthog_client: PostHogClient):
        """Test that the API key authenticates successfully."""
        # Get organizations as a simple auth check
        orgs = posthog_client.get_organizations()
        assert len(orgs) > 0

    def test_organization_access(self, posthog_client: PostHogClient, organization_id: str):
        """Test that we can access the configured organization."""
        org = posthog_client.get_organization(organization_id)
        assert org["id"] == organization_id
        assert "name" in org

    def test_project_access(self, posthog_client: PostHogClient, project_id: int):
        """Test that we can access the configured project."""
        project = posthog_client.get_project(project_id)
        assert project["id"] == project_id
        assert "name" in project


class TestOrganizationResources:
    """Tests for organization-level resources."""

    def test_get_organizations(self, posthog_client: PostHogClient):
        """Test fetching all organizations."""
        orgs = posthog_client.get_organizations()

        assert isinstance(orgs, list)
        assert len(orgs) > 0

        # Verify structure
        org = orgs[0]
        assert "id" in org
        assert "name" in org

    def test_get_projects_for_organization(
        self, posthog_client: PostHogClient, organization_id: str
    ):
        """Test fetching projects for an organization."""
        projects = posthog_client.get_projects_for_organization(organization_id)

        assert isinstance(projects, list)
        assert len(projects) > 0

        # Verify structure
        project = projects[0]
        assert "id" in project
        assert "name" in project


class TestProjectResources:
    """Tests for project-level resources (REST-only endpoints)."""

    def test_feature_flags_endpoint(self, posthog_client: PostHogClient, project_id: int):
        """Test feature flags endpoint returns data with expected structure."""
        flags = posthog_client.get_feature_flags(project_id)

        assert isinstance(flags, list)

        if flags:
            flag = flags[0]
            assert "id" in flag
            assert "key" in flag
            assert "active" in flag

    def test_cohorts_endpoint(self, posthog_client: PostHogClient, project_id: int):
        """Test cohorts endpoint returns data with expected structure."""
        cohorts = posthog_client.get_cohorts(project_id)

        assert isinstance(cohorts, list)

        if cohorts:
            cohort = cohorts[0]
            assert "id" in cohort
            assert "name" in cohort

    def test_annotations_endpoint(self, posthog_client: PostHogClient, project_id: int):
        """Test annotations endpoint returns data with expected structure."""
        annotations = posthog_client.get_annotations(project_id)

        assert isinstance(annotations, list)

        if annotations:
            ann = annotations[0]
            assert "id" in ann
            assert "content" in ann
            assert "date_marker" in ann


class TestHogQLQueryAPI:
    """Tests for HogQL Query API functionality."""

    def test_events_query_returns_data(self, posthog_client: PostHogClient, project_id: int):
        """Test that HogQL events query returns data with expected structure."""
        query = """
            SELECT uuid, event, timestamp, distinct_id, properties
            FROM events
            ORDER BY timestamp DESC
            LIMIT 5
        """
        result = posthog_client.query_hogql(project_id, query)

        assert "columns" in result
        assert "results" in result
        assert isinstance(result["results"], list)

        # Verify column names
        columns = result["columns"]
        assert "uuid" in columns
        assert "event" in columns
        assert "timestamp" in columns
        assert "distinct_id" in columns

    def test_persons_query_returns_data(self, posthog_client: PostHogClient, project_id: int):
        """Test that HogQL persons query returns data with expected structure."""
        query = """
            SELECT id, created_at, properties, is_identified
            FROM persons
            ORDER BY created_at DESC
            LIMIT 5
        """
        result = posthog_client.query_hogql(project_id, query)

        assert "columns" in result
        assert "results" in result
        assert isinstance(result["results"], list)

        # Verify column names
        columns = result["columns"]
        assert "id" in columns
        assert "created_at" in columns
        assert "properties" in columns

    def test_query_with_timestamp_filter(self, posthog_client: PostHogClient, project_id: int):
        """Test that HogQL timestamp filtering works correctly."""
        query = """
            SELECT uuid, event, timestamp
            FROM events
            WHERE timestamp > now() - INTERVAL 7 DAY
            ORDER BY timestamp DESC
            LIMIT 10
        """
        result = posthog_client.query_hogql(project_id, query)

        assert "results" in result
        # Query should execute successfully (may return 0 rows if no recent data)


class TestCaptureIntegration:
    """Integration tests for event capture (requires POSTHOG_PROJECT_API_KEY)."""

    @pytest.fixture
    def client_with_capture(self, posthog_client: PostHogClient) -> PostHogClient:
        """Get client with capture capability, skip if not available."""
        if not posthog_client.project_api_key:
            pytest.skip("POSTHOG_PROJECT_API_KEY required for capture tests")
        return posthog_client

    def test_capture_single_event(self, client_with_capture: PostHogClient, project_id: int):
        """Test capturing a single event."""
        import uuid
        from datetime import UTC, datetime

        # Use a unique distinct_id to avoid conflicts
        test_distinct_id = f"test_capture_{uuid.uuid4().hex[:8]}"

        event = {
            "event": "test_capture_event",
            "properties": {
                "distinct_id": test_distinct_id,
                "$set": {"test_property": "test_value"},
            },
            "timestamp": datetime.now(UTC).isoformat(),
        }

        # Should not raise
        client_with_capture.capture_events([event])

        # Note: Events may take a few seconds to appear in the API
        # We just verify the capture call succeeded
