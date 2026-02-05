"""
Test data injection for PostHog connector development.

Why: The connector reads from PostHog but can't create test data itself.
This script populates a PostHog project with deterministic test data for
smoke testing with `flowctl preview` and validating capture behavior.

Usage:
    poetry run python tests/data_injection.py inject   # Add test data
    poetry run python tests/data_injection.py cleanup  # Remove test data
    poetry run python tests/data_injection.py reset    # Cleanup then inject
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)


@dataclass
class PostHogClient:
    """PostHog API client for testing and data injection."""

    personal_api_key: str
    base_url: str = "https://app.posthog.com"
    project_api_key: str = ""  # For event capture (phc_...)

    _session: requests.Session = field(default_factory=requests.Session, repr=False)

    def __post_init__(self):
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.personal_api_key}",
                "Content-Type": "application/json",
            }
        )

    @staticmethod
    def _is_forbidden(e: requests.exceptions.RequestException) -> bool:
        """Check if an exception is a 403 Forbidden error."""
        return hasattr(e, "response") and e.response is not None and e.response.status_code == 403

    @staticmethod
    def _is_already_exists(e: requests.exceptions.RequestException) -> bool:
        """Check if an exception is a 400 Bad Request (typically 'already exists')."""
        return hasattr(e, "response") and e.response is not None and e.response.status_code == 400

    # -------------------------------------------------------------------------
    # Organization & Project methods (REST API - not deprecated)
    # -------------------------------------------------------------------------

    def get_organizations(self) -> list[dict[str, Any]]:
        """Fetch all organizations the user has access to."""
        url = f"{self.base_url}/api/organizations/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_organization(self, org_id: str) -> dict[str, Any]:
        """Fetch a specific organization by ID."""
        url = f"{self.base_url}/api/organizations/{org_id}/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()

    def get_projects_for_organization(self, org_id: str) -> list[dict[str, Any]]:
        """Fetch all projects for an organization."""
        url = f"{self.base_url}/api/organizations/{org_id}/projects/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_project(self, project_id: int) -> dict[str, Any]:
        """Fetch a specific project by ID."""
        url = f"{self.base_url}/api/projects/{project_id}/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json()

    # -------------------------------------------------------------------------
    # HogQL Query API
    # -------------------------------------------------------------------------

    def query_hogql(
        self,
        project_id: int,
        query: str,
        name: str = "test_query",
    ) -> dict[str, Any]:
        """Execute a HogQL query via the Query API."""
        url = f"{self.base_url}/api/projects/{project_id}/query/"
        payload = {
            "query": {
                "kind": "HogQLQuery",
                "query": query,
            },
            "name": name,
        }

        resp = self._session.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    # -------------------------------------------------------------------------
    # Event capture (write API - not deprecated)
    # -------------------------------------------------------------------------

    def capture_events(self, events: list[dict[str, Any]]) -> None:
        """Send events to PostHog via the capture/batch endpoint."""
        if not self.project_api_key:
            raise ValueError("project_api_key is required for capturing events")

        url = f"{self.base_url}/batch/"
        payload = {
            "api_key": self.project_api_key,
            "batch": events,
        }

        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        resp.raise_for_status()
        log.info("Captured batch of %d events", len(events))

    # -------------------------------------------------------------------------
    # Deletion methods (REST API - required for cleanup)
    # -------------------------------------------------------------------------

    def delete_person(
        self,
        project_id: int,
        person_id: str,
        delete_events: bool = True,
    ) -> None:
        """Delete a person by ID, optionally deleting their events."""
        url = f"{self.base_url}/api/projects/{project_id}/persons/{person_id}/"
        params = {"delete_events": "true"} if delete_events else {}
        resp = self._session.delete(url, params=params)
        resp.raise_for_status()

    def delete_test_persons(
        self,
        project_id: int,
        distinct_id_prefix: str = "test_",
        delete_events: bool = True,
    ) -> dict[str, Any]:
        """Delete all persons with distinct_ids matching the prefix."""
        url = f"{self.base_url}/api/projects/{project_id}/persons/"
        deleted_count = 0
        errors = []

        while url:
            resp = self._session.get(url)
            resp.raise_for_status()
            data = resp.json()

            for person in data.get("results", []):
                distinct_ids = person.get("distinct_ids", [])
                if any(did.startswith(distinct_id_prefix) for did in distinct_ids):
                    try:
                        self.delete_person(project_id, person["id"], delete_events)
                        deleted_count += 1
                        log.debug(
                            "Deleted person: %s", distinct_ids[0] if distinct_ids else person["id"]
                        )
                    except requests.exceptions.RequestException as e:
                        if self._is_forbidden(e):
                            log.warning(
                                "No permission to delete persons (403). Check API key scopes."
                            )
                            return {"deleted_count": deleted_count, "errors": errors}
                        errors.append({"person_id": person["id"], "error": str(e)})
                        log.debug("Failed to delete person %s: %s", person["id"], e)

            url = data.get("next")

        log.info("Deleted %d persons (prefix: %s)", deleted_count, distinct_id_prefix)
        return {"deleted_count": deleted_count, "errors": errors}

    # -------------------------------------------------------------------------
    # Feature Flags (REST API - not deprecated)
    # -------------------------------------------------------------------------

    def get_feature_flags(self, project_id: int) -> list[dict[str, Any]]:
        """Fetch all feature flags for a project."""
        url = f"{self.base_url}/api/projects/{project_id}/feature_flags/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_feature_flag(
        self,
        project_id: int,
        key: str,
        name: str,
        rollout_percentage: int = 100,
        active: bool = True,
    ) -> dict[str, Any]:
        """Create a feature flag."""
        url = f"{self.base_url}/api/projects/{project_id}/feature_flags/"
        data = {
            "key": key,
            "name": name,
            "active": active,
            "filters": {"groups": [{"properties": [], "rollout_percentage": rollout_percentage}]},
        }

        resp = self._session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def delete_feature_flag(self, project_id: int, flag_id: int) -> None:
        """Delete a feature flag (soft delete via PATCH)."""
        url = f"{self.base_url}/api/projects/{project_id}/feature_flags/{flag_id}/"
        resp = self._session.patch(url, json={"deleted": True})
        resp.raise_for_status()

    def delete_test_feature_flags(
        self,
        project_id: int,
        key_prefix: str = "test-",
    ) -> dict[str, Any]:
        """Delete all feature flags with keys matching the prefix."""
        flags = self.get_feature_flags(project_id)
        deleted_count = 0
        errors = []

        for flag in flags:
            if flag.get("key", "").startswith(key_prefix):
                try:
                    self.delete_feature_flag(project_id, flag["id"])
                    deleted_count += 1
                    log.debug("Deleted feature flag: %s", flag["key"])
                except requests.exceptions.RequestException as e:
                    if self._is_forbidden(e):
                        log.warning(
                            "No permission to delete feature flags (403). Check API key scopes."
                        )
                        return {"deleted_count": deleted_count, "errors": errors}
                    errors.append({"flag_id": flag["id"], "key": flag["key"], "error": str(e)})

        log.info("Deleted %d feature flags (prefix: %s)", deleted_count, key_prefix)
        return {"deleted_count": deleted_count, "errors": errors}

    # -------------------------------------------------------------------------
    # Cohorts (REST API - not deprecated)
    # -------------------------------------------------------------------------

    def get_cohorts(self, project_id: int) -> list[dict[str, Any]]:
        """Fetch all cohorts for a project."""
        url = f"{self.base_url}/api/projects/{project_id}/cohorts/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_cohort(
        self,
        project_id: int,
        name: str,
        description: str = "",
        groups: list[dict[str, Any]] | None = None,
        is_static: bool = False,
    ) -> dict[str, Any]:
        """Create a cohort."""
        url = f"{self.base_url}/api/projects/{project_id}/cohorts/"
        data = {
            "name": name,
            "description": description,
            "groups": groups or [],
            "is_static": is_static,
        }

        resp = self._session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def delete_cohort(self, project_id: int, cohort_id: int) -> None:
        """Delete a cohort (soft delete via PATCH)."""
        url = f"{self.base_url}/api/projects/{project_id}/cohorts/{cohort_id}/"
        resp = self._session.patch(url, json={"deleted": True})
        resp.raise_for_status()

    def delete_test_cohorts(
        self,
        project_id: int,
        name_prefix: str = "Test:",
    ) -> dict[str, Any]:
        """Delete all cohorts with names matching the prefix."""
        cohorts = self.get_cohorts(project_id)
        deleted_count = 0
        errors = []

        for cohort in cohorts:
            if cohort.get("name", "").startswith(name_prefix):
                try:
                    self.delete_cohort(project_id, cohort["id"])
                    deleted_count += 1
                    log.debug("Deleted cohort: %s", cohort["name"])
                except requests.exceptions.RequestException as e:
                    if self._is_forbidden(e):
                        log.warning("No permission to delete cohorts (403). Check API key scopes.")
                        return {"deleted_count": deleted_count, "errors": errors}
                    errors.append(
                        {"cohort_id": cohort["id"], "name": cohort["name"], "error": str(e)}
                    )

        log.info("Deleted %d cohorts (prefix: %s)", deleted_count, name_prefix)
        return {"deleted_count": deleted_count, "errors": errors}

    # -------------------------------------------------------------------------
    # Annotations (REST API - not deprecated)
    # -------------------------------------------------------------------------

    def get_annotations(self, project_id: int) -> list[dict[str, Any]]:
        """Fetch all annotations for a project."""
        url = f"{self.base_url}/api/projects/{project_id}/annotations/"
        resp = self._session.get(url)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_annotation(
        self,
        project_id: int,
        content: str,
        date_marker: datetime,
        scope: str = "project",
    ) -> dict[str, Any]:
        """Create an annotation."""
        url = f"{self.base_url}/api/projects/{project_id}/annotations/"
        data = {
            "content": content,
            "date_marker": date_marker.isoformat(),
            "scope": scope,
        }

        resp = self._session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def delete_annotation(self, project_id: int, annotation_id: int) -> None:
        """Delete an annotation (soft delete via PATCH)."""
        url = f"{self.base_url}/api/projects/{project_id}/annotations/{annotation_id}/"
        resp = self._session.patch(url, json={"deleted": True})
        resp.raise_for_status()

    def delete_test_annotations(
        self,
        project_id: int,
        content_prefix: str = "Test:",
    ) -> dict[str, Any]:
        """Delete all annotations with content matching the prefix."""
        annotations = self.get_annotations(project_id)
        deleted_count = 0
        errors = []

        for annotation in annotations:
            if annotation.get("content", "").startswith(content_prefix):
                try:
                    self.delete_annotation(project_id, annotation["id"])
                    deleted_count += 1
                    log.debug("Deleted annotation: %s", annotation["content"][:50])
                except requests.exceptions.RequestException as e:
                    if self._is_forbidden(e):
                        log.warning(
                            "No permission to delete annotations (403). Check API key scopes."
                        )
                        return {"deleted_count": deleted_count, "errors": errors}
                    errors.append({"annotation_id": annotation["id"], "error": str(e)})

        log.info("Deleted %d annotations (prefix: %s)", deleted_count, content_prefix)
        return {"deleted_count": deleted_count, "errors": errors}

    # -------------------------------------------------------------------------
    # Bulk operations
    # -------------------------------------------------------------------------

    def delete_all_test_data(
        self,
        project_id: int,
        distinct_id_prefix: str = "test_",
        flag_key_prefix: str = "test-",
        cohort_name_prefix: str = "Test:",
        annotation_content_prefix: str = "Test:",
    ) -> dict[str, Any]:
        """Delete all test data from a project."""
        results = {}

        log.info("Cleaning up test data from project %d...", project_id)

        results["persons"] = self.delete_test_persons(
            project_id, distinct_id_prefix=distinct_id_prefix
        )
        results["feature_flags"] = self.delete_test_feature_flags(
            project_id, key_prefix=flag_key_prefix
        )
        results["cohorts"] = self.delete_test_cohorts(project_id, name_prefix=cohort_name_prefix)
        results["annotations"] = self.delete_test_annotations(
            project_id, content_prefix=annotation_content_prefix
        )

        total_deleted = sum(r.get("deleted_count", 0) for r in results.values())
        total_errors = sum(len(r.get("errors", [])) for r in results.values())

        if total_errors > 0:
            log.warning(
                "Cleanup complete: %d items deleted, %d errors", total_deleted, total_errors
            )
            # Log first few errors for debugging
            for resource_type, result in results.items():
                errors = result.get("errors", [])
                if errors:
                    log.warning("  %s errors (%d): %s", resource_type, len(errors), errors[0])
        else:
            log.info("Cleanup complete: %d items deleted", total_deleted)
        return results


@dataclass
class MockDataGenerator:
    """Generate deterministic mock data for testing."""

    seed: int = 42
    num_users: int = 10
    events_per_user: int = 20
    base_timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    def generate_users(self) -> list[dict[str, Any]]:
        """Generate deterministic mock user data."""
        plans = ["free", "starter", "pro", "enterprise"]
        countries = ["US", "UK", "DE", "FR", "CA", "AU"]

        users = []
        for i in range(self.num_users):
            user_id = f"test_user_{i:04d}"
            plan = plans[i % len(plans)]
            country = countries[i % len(countries)]
            created_at = self.base_timestamp - timedelta(days=30 - i)

            user = {
                "distinct_id": user_id,
                "email": f"{user_id}@test.example.com",
                "name": f"Test User {i + 1}",
                "plan": plan,
                "country": country,
                "created_at": created_at.isoformat(),
            }
            users.append(user)

        return users

    def generate_events(self, users: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Generate deterministic events for users."""
        event_types = [
            ("$pageview", {"$current_url": "/home"}),
            ("$pageview", {"$current_url": "/pricing"}),
            ("$pageview", {"$current_url": "/docs"}),
            ("$pageview", {"$current_url": "/features"}),
            ("signup_started", {}),
            ("signup_completed", {}),
            ("feature_used", {"feature": "dashboard"}),
            ("feature_used", {"feature": "reports"}),
            ("feature_used", {"feature": "analytics"}),
            ("button_clicked", {"button": "upgrade"}),
            ("button_clicked", {"button": "settings"}),
            ("purchase", {"amount": 99}),
            ("purchase", {"amount": 199}),
            ("purchase", {"amount": 499}),
        ]

        events = []
        total_events = self.num_users * self.events_per_user
        minutes_per_event = (14 * 24 * 60) // total_events

        for user_idx, user in enumerate(users):
            for event_idx in range(self.events_per_user):
                event_type_idx = (user_idx + event_idx) % len(event_types)
                event_type, base_props = event_types[event_type_idx]

                event_props = dict(base_props)
                if event_type == "purchase":
                    amounts = [49, 99, 149, 199, 299, 499]
                    event_props["amount"] = amounts[(user_idx + event_idx) % len(amounts)]

                event_number = user_idx * self.events_per_user + event_idx
                offset_minutes = event_number * minutes_per_event
                timestamp = self.base_timestamp - timedelta(minutes=offset_minutes)

                props = {
                    "distinct_id": user["distinct_id"],
                    "$set": {
                        "email": user["email"],
                        "name": user["name"],
                        "plan": user["plan"],
                        "country": user["country"],
                    },
                    **event_props,
                }

                event = {
                    "event": event_type,
                    "properties": props,
                    "timestamp": timestamp.isoformat(),
                }
                events.append(event)

        return events

    def generate_feature_flags(self) -> list[dict[str, Any]]:
        """Generate deterministic feature flag configurations."""
        return [
            {
                "key": "test-new-dashboard",
                "name": "Test: New Dashboard Experience",
                "rollout_percentage": 50,
            },
            {
                "key": "test-dark-mode",
                "name": "Test: Dark Mode",
                "rollout_percentage": 100,
            },
            {
                "key": "test-beta-features",
                "name": "Test: Beta Features",
                "rollout_percentage": 10,
            },
        ]

    def generate_cohorts(self) -> list[dict[str, Any]]:
        """Generate deterministic cohort configurations."""
        return [
            {
                "name": "Test: Pro Users",
                "description": "Test cohort: Users on the pro plan",
                "groups": [{"properties": [{"key": "plan", "value": "pro", "type": "person"}]}],
            },
            {
                "name": "Test: Enterprise Users",
                "description": "Test cohort: Users on the enterprise plan",
                "groups": [
                    {"properties": [{"key": "plan", "value": "enterprise", "type": "person"}]}
                ],
            },
        ]

    def generate_annotations(self) -> list[dict[str, Any]]:
        """Generate deterministic annotation configurations."""
        return [
            {"content": "Test: Product launch v2.0", "days_before": 14},
            {"content": "Test: Marketing campaign started", "days_before": 10},
            {"content": "Test: Server migration completed", "days_before": 7},
            {"content": "Test: New pricing introduced", "days_before": 5},
            {"content": "Test: Bug fix release", "days_before": 2},
        ]


def inject_test_data(client: PostHogClient, project_id: int) -> None:
    """Inject test data into PostHog."""
    generator = MockDataGenerator()

    # Generate and inject events
    users = generator.generate_users()
    events = generator.generate_events(users)
    log.info("Injecting %d events for %d users...", len(events), len(users))
    client.capture_events(events)

    # Create feature flags
    for flag in generator.generate_feature_flags():
        try:
            client.create_feature_flag(
                project_id,
                key=flag["key"],
                name=flag["name"],
                rollout_percentage=flag["rollout_percentage"],
            )
            log.info("Created feature flag: %s", flag["key"])
        except requests.exceptions.HTTPError as e:
            if PostHogClient._is_already_exists(e):
                log.warning("Feature flag %s already exists", flag["key"])
            elif PostHogClient._is_forbidden(e):
                log.warning("No permission to create feature flags (403). Check API key scopes.")
                break
            else:
                raise

    # Create cohorts
    for cohort in generator.generate_cohorts():
        try:
            client.create_cohort(
                project_id,
                name=cohort["name"],
                description=cohort["description"],
                groups=cohort["groups"],
            )
            log.info("Created cohort: %s", cohort["name"])
        except requests.exceptions.HTTPError as e:
            if PostHogClient._is_already_exists(e):
                log.warning("Cohort %s already exists", cohort["name"])
            elif PostHogClient._is_forbidden(e):
                log.warning("No permission to create cohorts (403). Check API key scopes.")
                break
            else:
                raise

    # Create annotations
    base_timestamp = generator.base_timestamp
    for ann in generator.generate_annotations():
        date_marker = base_timestamp - timedelta(days=ann["days_before"])
        try:
            client.create_annotation(project_id, content=ann["content"], date_marker=date_marker)
            log.info("Created annotation: %s", ann["content"])
        except requests.exceptions.HTTPError as e:
            if PostHogClient._is_forbidden(e):
                log.warning("No permission to create annotations (403). Check API key scopes.")
                break
            else:
                log.warning("Failed to create annotation: %s", e)

    log.info("Test data injection complete!")


def main():
    parser = argparse.ArgumentParser(description="PostHog test data injection utility")
    parser.add_argument(
        "action",
        choices=["inject", "cleanup", "reset"],
        help="Action to perform: inject (add test data), cleanup (remove test data), reset (cleanup then inject)",
    )
    parser.add_argument(
        "--project-id",
        type=int,
        help="PostHog project ID (defaults to POSTHOG_PROJECT_ID env var)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Load environment
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)

    personal_api_key = os.environ.get("POSTHOG_PERSONAL_API_KEY")
    project_api_key = os.environ.get("POSTHOG_PROJECT_API_KEY")
    base_url = os.environ.get("POSTHOG_BASE_URL", "https://app.posthog.com")
    project_id = args.project_id or int(os.environ.get("POSTHOG_PROJECT_ID", "0"))

    if not personal_api_key:
        log.error("POSTHOG_PERSONAL_API_KEY environment variable required")
        sys.exit(1)

    if not project_id:
        log.error("Project ID required (--project-id or POSTHOG_PROJECT_ID)")
        sys.exit(1)

    if args.action == "inject" and not project_api_key:
        log.error("POSTHOG_PROJECT_API_KEY required for inject action")
        sys.exit(1)

    client = PostHogClient(
        personal_api_key=personal_api_key,
        base_url=base_url,
        project_api_key=project_api_key or "",
    )

    if args.action == "cleanup":
        client.delete_all_test_data(project_id)
    elif args.action == "inject":
        inject_test_data(client, project_id)
    elif args.action == "reset":
        client.delete_all_test_data(project_id)
        inject_test_data(client, project_id)


if __name__ == "__main__":
    main()
