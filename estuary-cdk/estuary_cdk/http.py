from dataclasses import dataclass
from logging import Logger
from pydantic import BaseModel
from typing import AsyncGenerator, Any, TypeVar, Callable, Awaitable
import abc
import aiohttp
import asyncio
import base64
import json
import time


from google.auth.credentials import TokenState as GoogleTokenState
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.service_account import Credentials as GoogleServiceAccountCredentials

from . import Mixin
from .flow import (
    AccessToken,
    BasicAuth,
    BaseOAuth2Credentials,
    AuthorizationCodeFlowOAuth2Credentials,
    ClientCredentialsOAuth2Credentials,
    OAuth2TokenFlowSpec,
    LongLivedClientCredentialsOAuth2Credentials,
    ResourceOwnerPasswordOAuth2Credentials,
    RotatingOAuth2Credentials,
    OAuth2Spec,
    OAuth2RotatingTokenSpec,
    GoogleServiceAccount,
    GoogleServiceAccountSpec,
)
from .utils import format_error_message


DEFAULT_AUTHORIZATION_HEADER = "Authorization"
DEFAULT_AUTHORIZATION_TOKEN_TYPE = "Bearer"

T = TypeVar("T")

class Headers(dict[str, Any]):
    pass


BodyGeneratorFunction = Callable[[], AsyncGenerator[bytes, None]]
HeadersAndBodyGenerator = tuple[Headers, BodyGeneratorFunction]


class HTTPError(RuntimeError):
    """
    HTTPError is an custom error class that provides the HTTP status code
    as a distinct attribute.
    """

    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.code = code
        self.message = message


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
        headers: dict[str, Any] = {},
    ) -> bytes:
        """Request a url and return its body as bytes"""

        chunks: list[bytes] = []
        _, body_generator = await self._request_stream(
            log, url, method, params, json, form, _with_token, headers
        )

        async for chunk in body_generator():
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
        headers: dict[str, Any] = {}
    ) -> tuple[Headers, BodyGeneratorFunction]:
        """Request a url and return its response as streaming lines, as they arrive"""

        resp_headers, body = await self._request_stream(
            log, url, method, params, json, form, True, headers
        )

        async def gen() -> AsyncGenerator[bytes, None]:
            buffer = b""
            async for chunk in body():
                buffer += chunk
                while delim in buffer:
                    line, buffer = buffer.split(delim, 1)
                    yield line

            if buffer:
                yield buffer

        return (resp_headers, gen)

    async def request_stream(
        self,
        log: Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        form: dict[str, Any] | None = None,
        _with_token: bool = True,  # Unstable internal API.
        headers: dict[str, Any] = {},
    ) -> tuple[Headers, BodyGeneratorFunction]:
        """Request a url and and return the raw response as a stream of bytes"""

        return await self._request_stream(log, url, method, params, json, form, _with_token, headers)


    @abc.abstractmethod
    async def _request_stream(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        _with_token: bool,
        headers: dict[str, Any] = {},
    ) -> HeadersAndBodyGenerator: ...

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

    oauth_spec: OAuth2Spec | OAuth2TokenFlowSpec | OAuth2RotatingTokenSpec | None
    credentials: (
        BaseOAuth2Credentials
        | RotatingOAuth2Credentials
        | ResourceOwnerPasswordOAuth2Credentials
        | ClientCredentialsOAuth2Credentials
        | AuthorizationCodeFlowOAuth2Credentials
        | LongLivedClientCredentialsOAuth2Credentials
        | AccessToken
        | BasicAuth
        | GoogleServiceAccount
    )
    authorization_header: str = DEFAULT_AUTHORIZATION_HEADER
    authorization_token_type: str = DEFAULT_AUTHORIZATION_TOKEN_TYPE
    google_spec: GoogleServiceAccountSpec | None = None
    _access_token: AccessTokenResponse | GoogleServiceAccountCredentials | None = None
    _fetched_at: int = 0

    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        if isinstance(self.credentials, (
                AccessToken,
                LongLivedClientCredentialsOAuth2Credentials,
                # RotatingOAuth2Credentials are refreshed _only_ at connector startup.
                # Expired tokens cause a crash, triggering a restart and token exchange.
                # Mid-run exchanges would complicate token management and make it difficult
                # to keep valid tokens in the endpoint config, so we never attempt to
                # exchange tokens in `fetch_token` for `RotatingOAuth2Credentials`.
                RotatingOAuth2Credentials,
            )
        ):
            return (self.authorization_token_type, self.credentials.access_token)
        elif isinstance(self.credentials, BasicAuth):
            return (
                "Basic",
                base64.b64encode(
                    f"{self.credentials.username}:{self.credentials.password}".encode()
                ).decode(),
            )
        elif isinstance(self.credentials, GoogleServiceAccount):
            assert isinstance(self.google_spec, GoogleServiceAccountSpec)
            if self._access_token is None:
                self._access_token = GoogleServiceAccountCredentials.from_service_account_info(
                    json.loads(self.credentials.service_account),
                    scopes=self.google_spec.scopes,
                )

            assert isinstance(self._access_token, GoogleServiceAccountCredentials)

            match self._access_token.token_state:
                case GoogleTokenState.FRESH:
                    pass
                case GoogleTokenState.STALE | GoogleTokenState.INVALID:
                    self._access_token.refresh(GoogleAuthRequest())
                case _:
                    raise RuntimeError(f"Unknown GoogleTokenState: {self._access_token.token_state}")

            return (self.authorization_token_type, self._access_token.token)

        assert (
            isinstance(self.credentials, BaseOAuth2Credentials)
            or isinstance(self.credentials, ClientCredentialsOAuth2Credentials)
            or isinstance(self.credentials, AuthorizationCodeFlowOAuth2Credentials)
            or isinstance(self.credentials, ResourceOwnerPasswordOAuth2Credentials)
        )
        current_time = time.time()

        if self._access_token is not None:
            assert isinstance(self._access_token, self.AccessTokenResponse)
            horizon = self._fetched_at + self._access_token.expires_in * 0.75

            if current_time < horizon:
                return (self.authorization_token_type, self._access_token.access_token)

        self._fetched_at = int(current_time)
        self._access_token = await self._fetch_oauth2_token(
            log, session, self.credentials
        )

        log.debug(
            "fetched OAuth2 access token",
            {"at": self._fetched_at, "expires_in": self._access_token.expires_in},
        )
        return (self.authorization_token_type, self._access_token.access_token)

    async def initialize_oauth2_tokens(
        self,
        log: Logger,
        session: HTTPSession,
    ) -> AccessTokenResponse:
        assert (
            isinstance(self.credentials, BaseOAuth2Credentials)
            or isinstance(self.credentials, ClientCredentialsOAuth2Credentials)
            or isinstance(self.credentials, AuthorizationCodeFlowOAuth2Credentials)
        )

        self._fetched_at = int(time.time())
        response = await self._fetch_oauth2_token(
            log, session, self.credentials,
        )
        self._access_token = response
        return response

    async def _fetch_oauth2_token(
        self,
        log: Logger,
        session: HTTPSession,
        credentials: BaseOAuth2Credentials
        | ResourceOwnerPasswordOAuth2Credentials
        | ClientCredentialsOAuth2Credentials
        | AuthorizationCodeFlowOAuth2Credentials
        | RotatingOAuth2Credentials,
    ) -> AccessTokenResponse:
        assert self.oauth_spec

        headers = {}
        form = {}

        match credentials:
            case RotatingOAuth2Credentials():
                assert isinstance(self.oauth_spec, OAuth2RotatingTokenSpec)
                form: dict[str, str | int] = {
                    "grant_type": "refresh_token",
                    "client_id": credentials.client_id,
                    "client_secret": credentials.client_secret,
                    "refresh_token": credentials.refresh_token,
                }

                # Some providers require additional parameters within the form body, like
                # an `expires_in` to configure how long the access token remains valid.
                if self.oauth_spec.additionalTokenExchangeBody:
                    form.update(self.oauth_spec.additionalTokenExchangeBody)

            case BaseOAuth2Credentials():
                form = {
                    "grant_type": "refresh_token",
                    "client_id": credentials.client_id,
                    "client_secret": credentials.client_secret,
                    "refresh_token": credentials.refresh_token,
                }
            case ClientCredentialsOAuth2Credentials():
                form = {
                    "grant_type": "client_credentials",
                }
                headers = {
                    "Authorization": "Basic "
                    + base64.b64encode(
                        f"{credentials.client_id}:{credentials.client_secret}".encode()
                    ).decode()
                }
            case AuthorizationCodeFlowOAuth2Credentials():
                form = {
                    "grant_type": "authorization_code",
                    "client_id": credentials.client_id,
                    "client_secret": credentials.client_secret,
                }
            case ResourceOwnerPasswordOAuth2Credentials():
                form = {
                    "grant_type": "password",
                    "client_id": credentials.client_id,
                    "client_secret": credentials.client_secret,
                }
            case _:
                raise TypeError(f"Unsupported credentials type: {type(credentials)}.")

        response = await session.request(
            log,
            self.oauth_spec.accessTokenUrlTemplate,
            method="POST",
            headers=headers,
            form=form,
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

    To avoid excessively long delays, `delay` cannot grow larger than `MAX_DELAY`.
    """

    delay: float = 1.0
    MAX_DELAY: float = 300.0 # 5 minutes
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
        self.delay = min(self.delay, self.MAX_DELAY)

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

    async def _establish_connection_and_get_response(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        _with_token: bool,
        headers: dict[str, Any],
    ):
        if _with_token and self.token_source is not None:
            token_type, token = await self.token_source.fetch_token(log, self)
            header_value = (
                f"{token_type} {token}"
                if self.token_source.authorization_header
                == DEFAULT_AUTHORIZATION_HEADER
                else f"{token}"
            )
            headers[self.token_source.authorization_header] = header_value

        resp = await self.inner.request(
            headers=headers,
            json=json,
            data=form,
            method=method,
            params=params,
            url=url,
        )

        return resp

    async def _retry_on_connection_error(
        self,
        log: Logger,
        url: str,
        method: str,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        max_attempts = 3
        attempt = 1

        while True:
            try:
                return await operation()
            except (
                asyncio.TimeoutError,                # Connection timeouts
                aiohttp.ClientConnectorError,        # DNS, SSL handshake, connection refused errors
                aiohttp.ClientConnectorDNSError,     # DNS resolution failures
                aiohttp.ConnectionTimeoutError,      # aiohttp connection timeouts (sock_connect, connect)
                ConnectionResetError,                # TCP connection reset
                aiohttp.ClientOSError,               # OS errors (like BrokenPipeError) during request sending
                aiohttp.ClientConnectionResetError,  # Connection reset errors
            ) as e:
                if attempt <= max_attempts:
                    log.warning(
                        f"Connection error occurred while establishing connection (will retry)",
                        {"url": url, "method": method, "attempt": attempt, "error": format_error_message(e)}
                    )
                    attempt += 1
                else:
                    raise

    async def _request_stream(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        _with_token: bool,
        headers: dict[str, Any] = {},
    ) -> HeadersAndBodyGenerator:
        while True:
            cur_delay = self.rate_limiter.delay
            await asyncio.sleep(cur_delay)

            resp = await self._retry_on_connection_error(
                log, url, method,
                lambda: self._establish_connection_and_get_response(
                    log, url, method, params, json, form, _with_token, headers,
                )
            )

            should_release_response = True
            try:
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
                    raise HTTPError(
                        f"Encountered HTTP error status {resp.status} which cannot be retried.\nURL: {url}\nResponse:\n{body.decode('utf-8')}",
                        resp.status,
                    )
                else:
                    resp.raise_for_status()

                    async def body_generator() -> AsyncGenerator[bytes, None]:
                        try:
                            async for chunk in resp.content.iter_any():
                                yield chunk
                        finally:
                            await resp.release()

                    response_headers = Headers({k: v for k, v in resp.headers.items()})
                    should_release_response = False
                    return (response_headers, body_generator)

            finally:
                if should_release_response:
                    await resp.release()


