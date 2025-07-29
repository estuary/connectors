import time
from datetime import UTC, datetime
from logging import Logger

import jwt
from estuary_cdk.http import HTTPSession, TokenSource

from .models import AppleCredentials


class AppleJWTTokenSource(TokenSource):
    """
    TokenSource implementation for Apple App Store Connect API JWT authentication.
    Integrates with Estuary CDK's HTTPSession to automatically generate JWT tokens
    and provide these as Bearer access tokens in the HTTP Authorization header.

    Important notes:
        - Apple requires that JWT tokens be generated using the ES256 algorithm and have
        an expiration time less than or equal to 20 minutes.
        - Tokens are generated using a 20 minute lifetime, but refreshed 5 minutes before
        expiry to reduce risk of unanticipated 401 errors.
        - The generated JWT token is provided in the request's authorization header as a bearer token
        by setting the credentials `access_token` to the JWT token. This is done automatically by the
        TokenSource implementation in the CDK when `fetch_token` is called.
    """

    _TOKEN_LIFETIME = 20 * 60  # 20-minutes (1200 seconds)
    _TOKEN_REFRESH_BUFFER = 5 * 60  # 5-minutes (300 seconds)
    _ALGORITHM = "ES256"

    def __init__(self, credentials: AppleCredentials):
        super().__init__(oauth_spec=None, credentials=credentials)
        self._apple_credentials = credentials
        self._token_expiry_seconds: int = 0

    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        assert isinstance(self.credentials, AppleCredentials), (
            "Invalid credentials type. Please provide a AppleCredentials object."
        )
        self._apple_credentials = self.credentials

        if not self._is_token_valid():
            self._generate_jwt_token()

        return await super().fetch_token(log, session)

    def _is_token_valid(self) -> bool:
        if (
            not self._apple_credentials.access_token
            or self._apple_credentials.access_token == ""
        ):
            return False

        seconds_since_epoch = int(datetime.now(tz=UTC).timestamp())
        expiry = self._token_expiry_seconds - self._TOKEN_REFRESH_BUFFER

        return seconds_since_epoch < expiry

    def _generate_jwt_token(self):
        """
        Generate a new JWT token signed with ES256.

        Creates a JWT with the required claims for Apple App Store Connect API:
        - iss: Issuer (Team ID)
        - iat: Issued At timestamp
        - exp: Expiration timestamp (20 minutes max)
        - aud: Audience (appstoreconnect-v1)
        """
        now = int(time.time())
        payload = {
            "iss": self._apple_credentials.issuer_id,
            "iat": now,
            "exp": now + self._TOKEN_LIFETIME,
            "aud": "appstoreconnect-v1",
        }
        headers = {
            "alg": self._ALGORITHM,
            "kid": self._apple_credentials.key_id,
            "typ": "JWT",
        }

        token = jwt.encode(
            payload,
            self._apple_credentials.private_key,
            algorithm=self._ALGORITHM,
            headers=headers,
        )

        self._apple_credentials.access_token = token
        self._token_expiry_seconds = now + self._TOKEN_LIFETIME
