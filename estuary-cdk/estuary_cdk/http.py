from pydantic import BaseModel
from dataclasses import dataclass
import abc
import aiohttp
import asyncio
import logging
import time

from . import Mixin, logger
from .flow import BaseOAuth2Credentials, AccessToken, OAuth2Spec


# HTTPSession is an abstract base class for HTTP clients.
# Connectors should use this type for typing constraints.
class HTTPSession(abc.ABC):
    @abc.abstractmethod
    async def request(
        self,
        url: str,
        method: str = "GET",
        params=None,
        json=None,
        data=None,
        with_token=True,
    ) -> bytes: ...


@dataclass
class TokenSource:

    class AccessTokenResponse(BaseModel):
        access_token: str
        token_type: str
        expires_in: int = 0
        refresh_token: str = ""
        scope: str = ""

    spec: OAuth2Spec
    credentials: BaseOAuth2Credentials | AccessToken
    _access_token: AccessTokenResponse | None = None
    _fetched_at: int = 0

    async def fetch_token(self, session: HTTPSession) -> str:
        if isinstance(self.credentials, AccessToken):
            return self.credentials.access_token

        assert isinstance(self.credentials, BaseOAuth2Credentials)
        current_time = time.time()

        if self._access_token is not None:
            horizon = self._fetched_at + self._access_token.expires_in * 0.75

            if current_time < horizon:
                return self._access_token.access_token

        self._fetched_at = int(current_time)
        self._access_token = await self._fetch_oauth2_token(session, self.credentials)

        logger.debug(
            "fetched OAuth2 access token",
            {"at": self._fetched_at, "expires_in": self._access_token.expires_in},
        )
        return self._access_token.access_token

    async def _fetch_oauth2_token(
        self, session: HTTPSession, credentials: BaseOAuth2Credentials
    ) -> AccessTokenResponse:
        response = await session.request(
            self.spec.accessTokenUrlTemplate,
            method="POST",
            data={
                "grant_type": "refresh_token",
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
                "refresh_token": credentials.refresh_token,
            },
            with_token=False,
        )
        return self.AccessTokenResponse.model_validate_json(response)


class RateLimiter:
    gain: float = 0.01
    delay: float = 1.0

    failed: int = 0
    total: int = 0

    def update(self, cur_delay: float, failed: bool):
        self.total += 1
        update: float

        if failed:
            update = max(cur_delay * 4.0, 0.1)
            self.failed += 1

            if self.failed / self.total > 0.05:
                logger.warning(
                    "rate limit exceeded",
                    {
                        "total": self.total,
                        "failed": self.failed,
                        "delay": self.delay,
                        "update": update,
                    },
                )

        elif self.failed == 0:
            update = cur_delay / 2.0

            # logger.debug(
            #    "rate limit fast-start",
            #    {
            #        "total": self.total,
            #        "failed": self.failed,
            #        "delay": self.delay,
            #        "update": update,
            #    },
            # )

        else:
            update = cur_delay * (1 - self.gain)

            # logger.debug(
            #     "rate limit decay",
            #    {
            #        "total": self.total,
            #        "failed": self.failed,
            #        "delay": self.delay,
            #        "update": update,
            #    },
            # )

        self.delay = (1 - self.gain) * self.delay + self.gain * update


# HTTPMixin is an opinionated implementation of HTTPSession.
class HTTPMixin(Mixin, HTTPSession):

    inner: aiohttp.ClientSession
    token_source: TokenSource | None = None
    rate_limiter: RateLimiter

    async def _mixin_enter(self, logger: logging.Logger):
        self.inner = aiohttp.ClientSession()
        self.rate_limiter = RateLimiter()
        return self

    async def _mixin_exit(self, logger: logging.Logger):
        await self.inner.close()
        return self

    async def _send_raw_request(
        self, url: str, method: str, params, json, data, with_token
    ) -> tuple[aiohttp.ClientResponse, bytes]:

        headers = {}
        if with_token and self.token_source is not None:
            token = await self.token_source.fetch_token(self)
            headers["Authorization"] = f"Bearer {token}"

        async with self.inner.request(
            headers=headers,
            json=json,
            data=data,
            method=method,
            params=params,
            url=url,
        ) as resp:

            return (resp, await resp.read())

    async def request(
        self,
        url: str,
        method: str = "GET",
        params=None,
        json=None,
        data=None,
        with_token=True,
    ) -> bytes:
        while True:

            cur_delay = self.rate_limiter.delay
            await asyncio.sleep(cur_delay)

            resp, body = await self._send_raw_request(
                url, method, params, json, data, with_token
            )
            self.rate_limiter.update(cur_delay, resp.status == 429)

            if resp.status == 429:
                pass

            elif resp.status >= 500 and resp.status < 600:
                logger.warning(
                    "server internal error (will retry)",
                    {"body": body.decode("utf-8")},
                )
            elif resp.status >= 400 and resp.status < 500:
                raise RuntimeError(
                    f"Encountered HTTP error status {resp.status} which cannot be retried.\nURL: {url}\nResponse:\n{body.decode('utf-8')}"
                )
            else:
                resp.raise_for_status()
                return body
