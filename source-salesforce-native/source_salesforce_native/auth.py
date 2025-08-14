
from typing import TYPE_CHECKING

from pydantic import (
    Field,
)
from estuary_cdk.capture.common import (
    BaseOAuth2Credentials,
    OAuth2Spec,
)
from estuary_cdk.http import (
    TokenSource,
)



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
class SalesforceTokenSource(TokenSource):
    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        expires_in: int = 1 * 60 * 60
