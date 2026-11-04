import functools
from logging import Logger
from typing import Any, override

import aiohttp

from estuary_cdk.http import HTTPMixin, ShouldRetryProtocol

from .errors import should_retry as should_retry_fault


def with_developer_token(
    developer_token: str | None, headers: dict[str, Any] | None, with_token: bool
) -> dict[str, Any] | None:
    if not with_token:
        return headers
    if developer_token is None:
        raise RuntimeError("an authenticated request was made before the developer token was set")
    return {**(headers or {}), "DeveloperToken": developer_token}


class BingAdsHTTPMixin(HTTPMixin):
    """Sends the `DeveloperToken` header on every authenticated request, and
    retries its 5xx faults by Microsoft's fault code (see `errors.should_retry`)
    unless the caller passes its own `should_retry`.

    The injection hooks `_request_stream`, the single chokepoint that `request`,
    `request_stream`, and `request_lines` all funnel through. Requests made
    with `with_token=False` (the OAuth token exchange and signed report
    download URLs) are left alone. Every other request needs the developer
    token, so one made before it's set raises rather than reaching Microsoft.

    `CustomerId` and `CustomerAccountId` differ per account, and requests for
    different accounts run concurrently, so callers pass them per request in
    `headers` rather than through this shared mixin."""

    developer_token: str | None = None

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
            with_developer_token(self.developer_token, headers, with_token),
            should_retry or (functools.partial(should_retry_fault, log) if with_token else None),
            timeout,
        )
