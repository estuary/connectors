import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, Mock
from logging import Logger

from source_datadog.models import EndpointConfig, ApiKey, LogResource
from source_datadog.api import fetch_events_changes


@pytest.mark.anyio
@pytest.mark.parametrize(
    "query,expected_query",
    [
        ("hello", "hello"),  # Custom query
        (None, "*"),  # Default query
    ],
)
async def test_query_from_config_used_in_api_request(query, expected_query):
    """Integration test: verify that EndpointConfig.query is included in API requests."""

    # Create a config with or without a specific query
    config_params = {
        "credentials": ApiKey(
            access_token="test_key",
            application_key="test_app_key"
        ),
        "site": "datadoghq.com",
        "start_date": datetime(2021, 1, 1, tzinfo=UTC),
    }
    if query is not None:
        config_params["query"] = query

    config = EndpointConfig(**config_params)

    # Mock HTTP session to capture the request
    http_mock = AsyncMock()
    captured_request_body = None

    async def capture_request(log, url, method, headers, json):
        nonlocal captured_request_body
        captured_request_body = json
        # Return a minimal valid response
        return '{"data": [], "meta": {"status": "done"}}'

    http_mock.request = capture_request

    log = Mock(spec=Logger)

    # Call fetch_events_changes which should use the query from config
    log_cursor = datetime(2021, 1, 15, tzinfo=UTC)

    result = [item async for item in fetch_events_changes(
        http=http_mock,
        base_url=config.base_url,
        common_headers=config.common_headers,
        endpoint="/logs/events/search",
        resource_type=LogResource,
        window_size=config.advanced.window_size,
        log=log,
        log_cursor=log_cursor,
        query=config.query,  # Pass query from config
    )]

    # Verify the request body includes the query
    assert captured_request_body is not None
    assert "filter" in captured_request_body
    assert "query" in captured_request_body["filter"]
    assert captured_request_body["filter"]["query"] == expected_query
