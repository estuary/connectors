"""Pytest fixtures for source-posthog tests."""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from source_posthog.models import EndpointConfig

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


@pytest.fixture
def endpoint_config() -> EndpointConfig:
    """Create EndpointConfig from environment variables."""
    personal_api_key = os.environ.get("POSTHOG_PERSONAL_API_KEY", "")
    organization_id = os.environ.get("POSTHOG_ORGANIZATION_ID", "")
    base_url = os.environ.get("POSTHOG_BASE_URL", "https://app.posthog.com")

    if not personal_api_key or not organization_id:
        pytest.skip("POSTHOG_PERSONAL_API_KEY and POSTHOG_ORGANIZATION_ID required")

    return EndpointConfig(
        personal_api_key=personal_api_key,
        organization_id=organization_id,
        advanced=EndpointConfig.Advanced(base_url=base_url),
    )


@pytest.fixture
def base_url() -> str:
    """Get PostHog base URL."""
    return os.environ.get("POSTHOG_BASE_URL", "https://app.posthog.com")


@pytest.fixture
def personal_api_key() -> str:
    """Get PostHog personal API key."""
    key = os.environ.get("POSTHOG_PERSONAL_API_KEY", "")
    if not key:
        pytest.skip("POSTHOG_PERSONAL_API_KEY required")
    return key


@pytest.fixture
def organization_id() -> str:
    """Get PostHog organization ID."""
    org_id = os.environ.get("POSTHOG_ORGANIZATION_ID", "")
    if not org_id:
        pytest.skip("POSTHOG_ORGANIZATION_ID required")
    return org_id


@pytest.fixture
def project_id() -> int:
    """Get PostHog project ID (for resource-level tests)."""
    pid = os.environ.get("POSTHOG_PROJECT_ID", "")
    if not pid:
        pytest.skip("POSTHOG_PROJECT_ID required")
    return int(pid)
