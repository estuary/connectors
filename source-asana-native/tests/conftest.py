"""Pytest fixtures for source-asana-native tests."""

from pathlib import Path

import pytest
import yaml

from source_asana_native.models import EndpointConfig

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture
def endpoint_config() -> EndpointConfig:
    """Create EndpointConfig from config.yaml."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"{CONFIG_PATH} not found — create it with your credentials")

    with open(CONFIG_PATH) as f:
        raw = yaml.safe_load(f)

    return EndpointConfig.model_validate(raw)
