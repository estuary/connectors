"""Mock HTTP session for testing Facebook API interactions."""

import asyncio
import json
import logging
import re
from collections import deque
from typing import Any, Callable


class MockHTTPSession:
    """Mock HTTP session with URL-pattern routing and concurrency tracking.

    Features:
    - URL-pattern routing: Register handlers with regex patterns
    - Method filtering: Optionally filter by HTTP method
    - Concurrency tracking: Track concurrent requests for semaphore testing
    - Fallback FIFO queue: Simple tests can queue responses without patterns

    Example:
        mock_http = MockHTTPSession()

        # Pattern-based handler
        def handle_submit(request):
            return {"report_run_id": "job_123"}

        mock_http.add_handler(r"/act_[^/]+/insights$", handle_submit, method="POST")

        # Simple FIFO response
        mock_http.add_response({"data": [{"impressions": 100}]})
    """

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self._handlers: list[
            tuple[
                re.Pattern[str],
                str | None,
                Callable[[dict[str, Any]], dict[str, Any] | bytes | Exception],
            ]
        ] = []
        self._fallback_responses: deque[bytes | Exception] = deque()
        self.concurrent_requests = 0
        self.max_concurrent_seen = 0
        self._lock = asyncio.Lock()

    def add_handler(
        self,
        url_pattern: str,
        handler: Callable[[dict[str, Any]], dict[str, Any] | bytes | Exception],
        method: str | None = None,
    ) -> None:
        """Add URL pattern handler.

        Args:
            url_pattern: Regex pattern to match URLs
            handler: Function that takes request dict and returns response
            method: Optional HTTP method filter (GET, POST, etc.)

        Note:
            Handler order matters! Register more specific patterns first.
            Use $ anchors to prevent /job_id from matching /job_id/insights.
        """
        self._handlers.append((re.compile(url_pattern), method, handler))

    def add_response(self, response: dict[str, Any] | bytes | Exception) -> None:
        """Queue a fallback response (FIFO) for simple tests.

        Args:
            response: Dict (auto-encoded to JSON bytes), bytes, or Exception
        """
        if isinstance(response, dict):
            response = json.dumps(response).encode()
        self._fallback_responses.append(response)

    def clear_handlers(self) -> None:
        """Clear all handlers and responses."""
        self._handlers.clear()
        self._fallback_responses.clear()

    async def request(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Mock request with URL routing and concurrency tracking.

        Args:
            log: Logger instance (matches real HTTPSession signature)
            url: Request URL
            method: HTTP method
            params: Query parameters
            **kwargs: Additional arguments (ignored)

        Returns:
            Response bytes

        Raises:
            Exception: If handler returns an Exception
            RuntimeError: If no handler matches and fallback queue is empty
        """
        request_info = {"url": url, "method": method, "params": params}
        self.requests.append(request_info)

        async with self._lock:
            self.concurrent_requests += 1
            self.max_concurrent_seen = max(
                self.max_concurrent_seen, self.concurrent_requests
            )

        try:
            # Check URL pattern handlers first
            for pattern, handler_method, handler in self._handlers:
                if pattern.search(url):
                    if handler_method is None or handler_method == method:
                        response = handler(request_info)
                        break
            else:
                # Fall back to FIFO queue
                if self._fallback_responses:
                    response = self._fallback_responses.popleft()
                else:
                    raise RuntimeError(f"No handler for {method} {url}")

            if isinstance(response, Exception):
                raise response
            if isinstance(response, dict):
                return json.dumps(response).encode()
            return response

        finally:
            async with self._lock:
                self.concurrent_requests -= 1
