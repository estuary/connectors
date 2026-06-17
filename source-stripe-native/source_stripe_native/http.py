from logging import Logger
from typing import Any, override

from estuary_cdk.http import HTTPMixin


class StripeHTTPMixin(HTTPMixin):
    """Pins every request to `pinned_api_version` via the `Stripe-Version` header;
    None means run at the account's default."""

    pinned_api_version: str | None = None

    def _with_api_version(
        self, headers: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if self.pinned_api_version is None:
            return headers
        return {**(headers or {}), "Stripe-Version": self.pinned_api_version}

    @override
    async def request(
        self,
        log: Logger,
        url: str,
        *args: Any,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        return await super().request(
            log, url, *args, headers=self._with_api_version(headers), **kwargs
        )

    @override
    async def request_stream(
        self,
        log: Logger,
        url: str,
        *args: Any,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        return await super().request_stream(
            log, url, *args, headers=self._with_api_version(headers), **kwargs
        )
