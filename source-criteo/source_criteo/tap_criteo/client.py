"""REST client handling, including CriteoStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from singer_sdk.streams import RESTStream

from tap_criteo.auth import CriteoAuthenticator

SCHEMAS_DIR = Path(__file__).parent / "./schemas"


class CriteoStream(RESTStream):
    """Criteo stream class."""

    url_base = "https://api.criteo.com"

    records_jsonpath = "$.data[*]"

    primary_keys = ("id",)

    @property
    def authenticator(self) -> CriteoAuthenticator:
        """Return a new authenticator object.

        Returns:
            The authenticator instance for this stream.
        """
        return CriteoAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers


class CriteoSearchStream(CriteoStream):
    """Search stream."""

    rest_method = "post"

    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any,  # noqa: ARG002, ANN401
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream context.
            next_page_token: Next page value.

        Returns:
            Dictionary for the JSON request body.
        """
        return {}
