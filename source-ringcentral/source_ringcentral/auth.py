import base64
import time
from logging import Logger

from pydantic import BaseModel
from estuary_cdk.http import HTTPSession, TokenSource

from .constants import OAUTH_BASE_URL
from .models import JWTCredentials


class TokenResponse(BaseModel, extra="allow"):
    """Response from RingCentral OAuth token endpoint."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int = 3600
    scope: str = ""

    def get_scopes(self) -> set[str]:
        """Return the set of scopes granted by this token."""
        if not self.scope:
            return set()
        return set(self.scope.split())


class RingCentralJWTTokenSource(TokenSource):
    """TokenSource for RingCentral JWT auth flow.

    Exchanges JWT credential for access token and caches until near expiry.

    Important notes:
        - RingCentral JWT tokens require an HTTP POST to the token endpoint
          with Basic auth (client_id:client_secret) and the JWT as an assertion.
        - Tokens are refreshed 5 minutes before expiry to reduce risk of
          unanticipated 401 errors.
        - The generated access token is provided in the request's authorization
          header as a bearer token by setting the credentials `access_token`.
    """

    _TOKEN_REFRESH_BUFFER = 5 * 60  # 5 minutes before expiry

    def __init__(self, credentials: JWTCredentials):
        super().__init__(oauth_spec=None, credentials=credentials)
        self._jwt_credentials = credentials
        self._token_expiry: float = 0

    async def fetch_token(
        self, log: Logger, session: HTTPSession
    ) -> tuple[str, str]:
        assert isinstance(self.credentials, JWTCredentials), (
            "Invalid credentials type. Please provide a JWTCredentials object."
        )
        self._jwt_credentials = self.credentials

        if not self._is_token_valid():
            await self._exchange_jwt_for_token(log, session)

        return await super().fetch_token(log, session)

    def _is_token_valid(self) -> bool:
        if not self._jwt_credentials.access_token:
            return False

        return time.time() < (self._token_expiry - self._TOKEN_REFRESH_BUFFER)

    async def _exchange_jwt_for_token(
        self, log: Logger, session: HTTPSession
    ) -> None:
        if not self._jwt_credentials.client_id or not self._jwt_credentials.client_secret:
            raise ValueError(
                "RingCentral client_id and client_secret are required. "
                "Create a JWT Auth app at https://developers.ringcentral.com/console/apps "
                "and provide the app credentials."
            )

        url = f"{OAUTH_BASE_URL}/token"

        auth = base64.b64encode(
            f"{self._jwt_credentials.client_id}:{self._jwt_credentials.client_secret}".encode()
        ).decode()

        response = TokenResponse.model_validate_json(
            await session.request(
                log,
                url,
                method="POST",
                headers={"Authorization": f"Basic {auth}"},
                form={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": self._jwt_credentials.jwt,
                },
                with_token=False,
            )
        )

        self._jwt_credentials.access_token = response.access_token
        self._jwt_credentials.set_scopes(response.get_scopes())
        self._token_expiry = time.time() + response.expires_in
