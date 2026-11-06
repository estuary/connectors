from logging import Logger
from typing import Any, override

import aiohttp

from estuary_cdk.http import HTTPMixin, ShouldRetryProtocol


class StripeHTTPMixin(HTTPMixin):
    """Pins every request to `pinned_api_version` via the `Stripe-Version` header;
    None means run at the account's default.

    The injection hooks `_request_stream`, the single chokepoint that `request`,
    `request_stream`, and `request_lines` all funnel through, so no public entry
    point can escape the pin."""

    pinned_api_version: str | None = None

    def _with_api_version(
        self, headers: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if self.pinned_api_version is None:
            return headers
        return {**(headers or {}), "Stripe-Version": self.pinned_api_version}

    @override
    async def _request_stream(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        with_token: bool,
        headers: dict[str, Any] | None = None,
        should_retry: ShouldRetryProtocol | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ):
        return await super()._request_stream(
            log,
            url,
            method,
            params,
            json,
            form,
            with_token,
            self._with_api_version(headers),
            should_retry,
            timeout,
        )
