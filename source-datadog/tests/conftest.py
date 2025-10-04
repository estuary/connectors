import pytest


@pytest.fixture
def anyio_backend():
    """Configure anyio to only use asyncio backend for all tests."""
    return "asyncio"
