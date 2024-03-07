from dataclasses import dataclass
from logging import Logger
from pydantic import BaseModel
from typing import AsyncGenerator, Any
import abc
import aiohttp
import asyncio
import base64
import time

from . import Mixin
from .flow import BaseOAuth2Credentials, AccessToken, OAuth2Spec, BasicAuth


class HTTPSession(abc.ABC):
    """
    HTTPSession is an abstract base class for an HTTP client implementation.
    Implementations should manage retries, authorization, and other details.
    Only "success" responses are returned: failures throw an Exception if
    they cannot be retried.

    HTTPSession is implemented by HTTPMixin.

    Common parameters of request methods:
     * `url` to request.
     * `method` to use (GET, POST, DELETE, etc)
     * `params` are encoded as URL parameters of the query
     * `json` is a JSON-encoded request body (if set, `form` cannot be)
     * `form` is a form URL-encoded request body (if set, `json` cannot be)
    """

    async def request(
        self,
        log: Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        form: dict[str, Any] | None = None,
        _with_token: bool = True,  # Unstable internal API.
    ) -> bytes:
        """Request a url and return its body as bytes"""

        chunks: list[bytes] = []
        async for chunk in self._request_stream(
            log, url, method, params, json, form, _with_token
        ):
            chunks.append(chunk)

        if len(chunks) == 0:
            return b""
        elif len(chunks) == 1:
            return chunks[0]
        else:
            return b"".join(chunks)

    async def request_lines(
        self,
        log: Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        form: dict[str, Any] | None = None,
        delim: bytes = b"\n",
    ) -> AsyncGenerator[bytes, None]:
        """Request a url and return its response as streaming lines, as they arrive"""

        buffer = b""
        async for chunk in self._request_stream(
            log, url, method, params, json, form, True
        ):
            buffer += chunk
            while delim in buffer:
                line, buffer = buffer.split(delim, 1)
                yield line

        if buffer:
            yield buffer

        return

    @abc.abstractmethod
    def _request_stream(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        _with_token: bool,
    ) -> AsyncGenerator[bytes, None]: ...

    # TODO(johnny): This is an unstable API.
    # It may need to accept request headers, or surface response headers,
    # or we may refactor TokenSource, etc.


@dataclass
class TokenSource:

    class AccessTokenResponse(BaseModel):
        access_token: str
        token_type: str
        expires_in: int = 0
        refresh_token: str = ""
        scope: str = ""

    oauth_spec: OAuth2Spec | None
    credentials: BaseOAuth2Credentials | AccessToken | BasicAuth
    _access_token: AccessTokenResponse | None = None
    _fetched_at: int = 0

    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        if isinstance(self.credentials, AccessToken):
            return ("Bearer", self.credentials.access_token)
        elif isinstance(self.credentials, BasicAuth):
            return (
                "Basic",
                base64.b64encode(
                    f"{self.credentials.username}:{self.credentials.password}".encode()
                ).decode(),
            )

        assert isinstance(self.credentials, BaseOAuth2Credentials)
        current_time = time.time()

        if self._access_token is not None:
            horizon = self._fetched_at + self._access_token.expires_in * 0.75

            if current_time < horizon:
                return ("Bearer", self._access_token.access_token)

        self._fetched_at = int(current_time)
        self._access_token = await self._fetch_oauth2_token(
            log, session, self.credentials
        )

        log.debug(
            "fetched OAuth2 access token",
            {"at": self._fetched_at, "expires_in": self._access_token.expires_in},
        )
        return ("Bearer", self._access_token.access_token)

    async def _fetch_oauth2_token(
        self, log: Logger, session: HTTPSession, credentials: BaseOAuth2Credentials
    ) -> AccessTokenResponse:
        assert self.oauth_spec

        response = await session.request(
            log,
            self.oauth_spec.accessTokenUrlTemplate,
            method="POST",
            form={
                "grant_type": "refresh_token",
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
                "refresh_token": credentials.refresh_token,
            },
            _with_token=False,
        )
        return self.AccessTokenResponse.model_validate_json(response)


class RateLimiter:
    """
    RateLimiter maintains a `delay` parameter, which is the number of seconds
    to wait before issuing an HTTP request. It attempts to achieve a low rate
    of HTTP 429 (Rate Limit Exceeded) errors (under 5%) while fully utilizing
    the available rate limit without excessively long delays due to more
    traditional exponential back-off strategies.

    As requests are made, RateLimiter.update() is called to dynamically adjust
    the `delay` parameter, decreasing it as successful request occur and
    increasing it as HTTP 429 (Rate Limit Exceeded) failures are reported.

    It initially uses quadratic decrease of `delay` until a first failure is
    encountered. Additional failures result in quadratic increase, while
    successes apply a linear decay.
    """

    delay: float = 1.0
    gain: float = 0.01

    failed: int = 0
    total: int = 0

    def update(self, cur_delay: float, failed: bool):
        self.total += 1
        update: float

        if failed:
            update = max(cur_delay * 4.0, 0.1)
            self.failed += 1
        elif self.failed == 0:
            update = cur_delay / 2.0
        else:
            update = cur_delay * (1 - self.gain)

        self.delay = (1 - self.gain) * self.delay + self.gain * update

    @property
    def error_ratio(self) -> float:
        return self.failed / self.total


# HTTPMixin is an opinionated implementation of HTTPSession.
class HTTPMixin(Mixin, HTTPSession):

    inner: aiohttp.ClientSession
    rate_limiter: RateLimiter
    token_source: TokenSource | None = None

    async def _mixin_enter(self, _: Logger):
        self.inner = aiohttp.ClientSession()
        self.rate_limiter = RateLimiter()
        return self

    async def _mixin_exit(self, _: Logger):
        await self.inner.close()
        return self

    async def _request_stream(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        _with_token: bool,
    ) -> AsyncGenerator[bytes, None]:
        while True:

            cur_delay = self.rate_limiter.delay
            await asyncio.sleep(cur_delay)

            headers = {}
            if _with_token and self.token_source is not None:
                token_type, token = await self.token_source.fetch_token(log, self)
                headers["Authorization"] = f"{token_type} {token}"

            async with self.inner.request(
                headers=headers,
                json=json,
                data=form,
                method=method,
                params=params,
                url=url,
            ) as resp:

                self.rate_limiter.update(cur_delay, resp.status == 429)

                if resp.status == 429:
                    if self.rate_limiter.failed / self.rate_limiter.total > 0.05:
                        log.warning(
                            "rate limit errors are elevated",
                            {
                                "delay": self.rate_limiter.delay,
                                "failed": self.rate_limiter.failed,
                                "total": self.rate_limiter.total,
                            },
                        )

                elif resp.status >= 500 and resp.status < 600:
                    body = await resp.read()
                    log.warning(
                        "server internal error (will retry)",
                        {"body": body.decode("utf-8")},
                    )
                elif resp.status >= 400 and resp.status < 500:
                    body = await resp.read()
                    raise RuntimeError(
                        f"Encountered HTTP error status {resp.status} which cannot be retried.\nURL: {url}\nResponse:\n{body.decode('utf-8')}"
                    )
                else:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_any():
                        yield chunk

                    return
