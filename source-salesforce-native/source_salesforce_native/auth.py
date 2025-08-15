import dataclasses
from logging import Logger
import time
from typing import Literal, TYPE_CHECKING

from pydantic import (
    BaseModel,
    Field,
)
from estuary_cdk.capture.common import (
    BaseOAuth2Credentials,
    OAuth2Spec,
)
from estuary_cdk.http import (
    HTTPSession,
    TokenSource,
)
from simple_salesforce.login import SalesforceLogin


from .shared import VERSION


OAUTH2_SPEC = OAuth2Spec(
    provider="salesforce",
    authUrlTemplate=(
        "https://"
        r"{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}"
        ".salesforce.com/services/oauth2/authorize?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&response_type=code"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate=(
        "https://"
        r"{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}"
        ".salesforce.com/services/oauth2/token"
    ),
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
        "instance_url": "/instance_url",
    },
)

# Mustache templates in accessTokenUrlTemplate are not interpolated within the connector, so update_oauth_spec reassigns it for use within the connector.
def update_oauth_spec(is_sandbox: bool):
    OAUTH2_SPEC.accessTokenUrlTemplate = f"https://{'test' if is_sandbox else 'login'}.salesforce.com/services/oauth2/token"


class UserPass(BaseModel):
    credentials_title: Literal["Username, Password, & Security Token"] = Field(
        default="Username, Password, & Security Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    username: str = Field(
        title="Username",
        json_schema_extra={"order": 1},
    )
    password: str = Field(
        title="Password",
        json_schema_extra={"secret": True, "order": 2},
    )
    security_token: str = Field(
        title="Security Token",
        json_schema_extra={"secret": True, "order": 3},
    )

    def fetch_access_token_and_instance_url(self, is_sandbox: bool) -> tuple[str, str]:
        access_token, instance_url = SalesforceLogin(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain="test" if is_sandbox else "login",
            sf_version=VERSION,
        )

        if not instance_url.startswith("https://"):
            instance_url = "https://" + instance_url

        return (access_token, instance_url)


class SalesforceOAuth2Credentials(BaseOAuth2Credentials):
    instance_url: str = Field(
        title="Instance URL",
    )

    @staticmethod
    def for_provider(provider: str) -> type["SalesforceOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(SalesforceOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials


if TYPE_CHECKING:
    OAuth2Credentials = SalesforceOAuth2Credentials
else:
    OAuth2Credentials = SalesforceOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


# The access token response does not contain any field indicating if/when the access token we receive expires.
# The default TokenSource.AccessTokenResponse.expires_in is 0, causing every request to fetch a new access token.
# The actual expires_in value depends on the session settings in the user's Salesforce account. The default seems
# to be 2 hours. More importantly, each time the access token is used, it's validity is extended. Meaning that as long
# as the access token is actively used, it shouldn't expire. Setting an expires_in of 1 hour should be a 
# safe fallback in case a user changes all enabled bindings' intervals to an hour or greater.
@dataclasses.dataclass
class SalesforceTokenSource(TokenSource):
    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        expires_in: int = 1 * 60 * 60

    # These fields must be keyword-only because the parent TokenSource class has fields with default values.
    # In Python dataclasses, you cannot define fields without defaults after fields with defaults
    # unless the new fields are marked as keyword-only using dataclasses.field(kw_only=True).
    credentials: UserPass | OAuth2Credentials = dataclasses.field(kw_only=True)
    is_sandbox: bool = dataclasses.field(kw_only=True)

    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        if isinstance(self.credentials, UserPass):
            current_time = time.time()

            if self._access_token is not None:
                assert isinstance(self._access_token, self.AccessTokenResponse)

                horizon = self._fetched_at + self._access_token.expires_in * 0.75

                if current_time < horizon:
                    return (self.authorization_token_type, self._access_token.access_token)

            self._fetched_at = int(current_time)
            access_token, _ = self.credentials.fetch_access_token_and_instance_url(is_sandbox=self.is_sandbox)
            self._access_token = SalesforceTokenSource.AccessTokenResponse(
                access_token=access_token,
                token_type=self.authorization_token_type,
            )
            return (self.authorization_token_type, self._access_token.access_token)
        else:
            return await super().fetch_token(log, session)
