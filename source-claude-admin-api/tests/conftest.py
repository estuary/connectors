"""Pytest fixtures for source-claude-admin-api tests."""

from pathlib import Path

import pytest
import yaml

from source_claude_admin_api.models import EndpointConfig

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture
def endpoint_config() -> EndpointConfig:
    """Build EndpointConfig from config.yaml (skips live tests when absent)."""
    if not CONFIG_PATH.exists():
        pytest.skip("config.yaml not found — copy config.yaml.template and fill in the admin key")

    with open(CONFIG_PATH) as f:
        raw = yaml.safe_load(f)

    return EndpointConfig.model_validate(raw)
