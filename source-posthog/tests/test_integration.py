"""Integration tests for connector capture using flowctl preview."""

import json
import subprocess


class TestCaptureIntegration:
    """Integration tests for connector capture using flowctl preview."""

    def test_capture_organizations(self, request):
        """Test that Organizations capture returns data."""
        result = subprocess.run(
            [
                "flowctl",
                "preview",
                "--source",
                request.fspath.dirname + "/../test.flow.local.yaml",
                "--sessions",
                "1",
                "--delay",
                "5s",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

        # Parse output to find Organizations documents
        # Format: ["collection/name", {document}]
        orgs_found = False
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                # flowctl preview outputs [collection, document] arrays
                if isinstance(row, list) and len(row) == 2:
                    collection, doc = row
                    if "Organizations" in collection:
                        orgs_found = True
                        assert "id" in doc, "Organization missing id"
                        assert "name" in doc, "Organization missing name"
                        break
            except json.JSONDecodeError:
                continue

        assert orgs_found, "No Organizations documents found in capture output"

    def test_capture_projects(self, request):
        """Test that Projects capture returns data with project_id."""
        result = subprocess.run(
            [
                "flowctl",
                "preview",
                "--source",
                request.fspath.dirname + "/../test.flow.local.yaml",
                "--sessions",
                "1",
                "--delay",
                "5s",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

        # Parse output to find Projects documents
        # Format: ["collection/name", {document}]
        projects_found = False
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if isinstance(row, list) and len(row) == 2:
                    collection, doc = row
                    if "Projects" in collection:
                        projects_found = True
                        assert "id" in doc, "Project missing id"
                        assert "name" in doc, "Project missing name"
                        break
            except json.JSONDecodeError:
                continue

        assert projects_found, "No Projects documents found in capture output"

    def test_capture_events(self, request):
        """Test that Events capture returns data with project_id field."""
        result = subprocess.run(
            [
                "flowctl",
                "preview",
                "--source",
                request.fspath.dirname + "/../test.flow.local.yaml",
                "--sessions",
                "1",
                "--delay",
                "10s",  # Events may take longer
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

        # Parse output to find Events documents
        # Format: ["collection/name", {document}]
        events_found = False
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if isinstance(row, list) and len(row) == 2:
                    collection, doc = row
                    if "Events" in collection:
                        events_found = True
                        # Verify project_id is present and is an integer
                        assert "project_id" in doc, "Event missing project_id"
                        assert isinstance(doc["project_id"], int), "project_id should be an integer"
                        assert "id" in doc, "Event missing id"
                        assert "event" in doc, "Event missing event name"
                        break
            except json.JSONDecodeError:
                continue

        assert events_found, "No Events documents found in capture output (run scripts/inject_mock_data.py to populate test data)"

    def test_capture_all_resources(self, request):
        """Test full capture workflow with all bindings."""
        result = subprocess.run(
            [
                "flowctl",
                "preview",
                "--source",
                request.fspath.dirname + "/../test.flow.local.yaml",
                "--sessions",
                "1",
                "--delay",
                "15s",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        assert result.returncode == 0, f"flowctl preview failed: {result.stderr}"

        # Count documents by collection name
        # Format: ["collection/name", {document}]
        doc_counts = {
            "Organizations": 0,
            "Projects": 0,
            "Events": 0,
            "Persons": 0,
            "Cohorts": 0,
            "FeatureFlags": 0,
            "Annotations": 0,
        }

        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if isinstance(row, list) and len(row) == 2:
                    collection, _ = row
                    # Collection format: "acmeCo/ResourceName"
                    for resource_name in doc_counts:
                        if resource_name in collection:
                            doc_counts[resource_name] += 1
                            break
            except json.JSONDecodeError:
                continue

        # At minimum, we should have organizations and projects
        assert doc_counts["Organizations"] > 0, f"Expected Organizations, got counts: {doc_counts}"
        assert doc_counts["Projects"] > 0, f"Expected Projects, got counts: {doc_counts}"
