"""Shared pytest fixtures for all tests."""

import asyncio
import logging
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from source_facebook_marketing_native.enums import ApiLevel
from source_facebook_marketing_native.insights import (
    FacebookInsightsJobManager,
    TimeRange,
)

from tests.factories.http import MockHTTPSession


@pytest.fixture
def mock_http() -> MockHTTPSession:
    """Create a fresh MockHTTPSession for each test."""
    return MockHTTPSession()


@pytest.fixture
def mock_log(request: pytest.FixtureRequest) -> logging.Logger:
    """Create a logger that outputs when running with verbose flag.

    Usage:
        pytest -v --log-cli-level=DEBUG  # See all log output
        pytest -v                         # Normal run, logs hidden
    """
    logger = logging.getLogger(f"test.{request.node.name}")
    logger.setLevel(logging.DEBUG)

    # Check if verbose mode requested via pytest -v flag
    if request.config.getoption("verbose", 0) > 0:
        # Add handler if not already present
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%H:%M:%S",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    return logger


@pytest.fixture
def job_manager(
    mock_http: MockHTTPSession,
    mock_log: logging.Logger,
) -> FacebookInsightsJobManager:
    """Create a job manager with mock HTTP session."""
    return FacebookInsightsJobManager(
        http=mock_http,  # type: ignore[arg-type]
        base_url="https://graph.facebook.com/v21.0",
        log=mock_log,
        account_id="test_account",
    )


@pytest.fixture
def mock_model() -> MagicMock:
    """Create mock insights model with minimal required attributes."""
    model = MagicMock()
    model.level = ApiLevel.AD
    model.fields = ["impressions", "clicks"]
    model.breakdowns = None
    model.action_breakdowns = None
    model.action_attribution_windows = None
    return model


@pytest.fixture
def time_range() -> TimeRange:
    """Create a standard time range for tests."""
    return {"since": "2024-01-01", "until": "2024-01-01"}


@pytest.fixture
def mock_sleep() -> Generator[MagicMock, None, None]:
    """Mock asyncio.sleep to avoid real delays in tests.

    Yields:
        MagicMock that tracks call count for assertions
    """

    async def instant_sleep(delay: float) -> None:
        """Instant sleep - no actual waiting.

        The delay parameter is kept for signature compatibility with asyncio.sleep.
        """
        pass

    with patch("asyncio.sleep", side_effect=instant_sleep) as mock:
        yield mock
