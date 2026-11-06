import dataclasses
from logging import Logger
import time
from typing import Literal, TYPE_CHECKING

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
from estuary_cdk.capture.common import (
    BaseOAuth2Credentials,
    ClientCredentialsOAuth2Credentials,
    OAuth2Spec,
)
from estuary_cdk.http import (
    HTTPSession,
    TokenSource,
)
from simple_salesforce.login import SalesforceLogin


from .shared import VERSION


# _OAUTH_LOGIN_ORIGIN is a mustache fragment rendered by the control plane that selects the OAuth login origin.
# The org's My Domain host is used when provided, otherwise the standard login/test host is used.
_OAUTH_LOGIN_ORIGIN = (
    "https://"
    r"{{#config.my_domain}}{{{config.my_domain}}}{{/config.my_domain}}"
    r"{{^config.my_domain}}{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}.salesforce.com{{/config.my_domain}}"
)


OAUTH2_SPEC = OAuth2Spec(
    provider="salesforce",
    authUrlTemplate=(
        _OAUTH_LOGIN_ORIGIN + (
            "/services/oauth2/authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            r"&response_type=code"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
        )
    ),
    accessTokenUrlTemplate=_OAUTH_LOGIN_ORIGIN + "/services/oauth2/token",
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
def update_oauth_spec(is_sandbox: bool, my_domain: str):
    # Refresh-token requests must hit the same host the tokens were issued from. When a My Domain
    # is configured it's already a full host (e.g. mycompany.my.salesforce.com); otherwise use the
    # standard login/test host.
    host = my_domain or f"{'test' if is_sandbox else 'login'}.salesforce.com"
    OAUTH2_SPEC.accessTokenUrlTemplate = f"https://{host}/services/oauth2/token"


def _login_domain_from_my_domain(my_domain: str) -> str:
    # simple_salesforce builds the SOAP login URL as https://{domain}.salesforce.com/..., so it
    # wants just the subdomain. my_domain is validated by the EndpointConfig model as a full host ending in
    # ".my.salesforce.com", so here we strip the ".salesforce.com" it re-appends.
    return my_domain.removesuffix(".salesforce.com")


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

    def fetch_access_token_and_instance_url(self, is_sandbox: bool, my_domain: str) -> tuple[str, str]:
        # Log in through the org's My Domain when configured, otherwise use the standard login/test host.
        domain = _login_domain_from_my_domain(my_domain) or ("test" if is_sandbox else "login")

        access_token, instance_url = SalesforceLogin(
            username=self.username,
            password=self.password,
            security_token=self.security_token,
            domain=domain,
            sf_version=VERSION,
        )

        if not instance_url.startswith("https://"):
            instance_url = "https://" + instance_url

        return (access_token, instance_url)


class SalesforceClientCredentials(ClientCredentialsOAuth2Credentials):
    # Salesforce's OAuth 2.0 Client Credentials flow is a server-to-server flow: the connector
    # exchanges a Salesforce Connected App's / External Client App's Consumer Key/Secret for an
    # access token minted as the app's configured "Run As" user. Both app types expose a Consumer
    # Key/Secret and support this flow, and the connector treats them identically.
    #
    # The CDK's TokenSource performs the grant_type=client_credentials exchange.
    # This model just supplies Salesforce-specific titles and discriminator.
    #
    # The flow requires posting to the org's My Domain token endpoint, which is why EndpointConfig
    # requires my_domain when SalesforceClientCredentials is selected.
    model_config = ConfigDict(title="Client Credentials")

    credentials_title: Literal["Client Credentials"] = Field(
        default="Client Credentials",
        json_schema_extra={"type": "string", "order": 0},
    )
    client_id: str = Field(
        title="Consumer Key",
        description="The Consumer Key of the Connected App or External Client App.",
        json_schema_extra={"secret": True, "order": 1},
    )
    client_secret: str = Field(
        title="Consumer Secret",
        description="The Consumer Secret of the Connected App or External Client App.",
        json_schema_extra={"secret": True, "order": 2},
    )


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
        # The UserPass and Client Credentials flows learn the instance URL from their token exchange
        # (the SOAP login and the client_credentials token response, respectively) and stash it here.
        # OAuth reads its instance URL from the persisted credentials and never uses this field.
        instance_url: str

    # These fields must be keyword-only because the parent TokenSource class has fields with default values.
    # In Python dataclasses, you cannot define fields without defaults after fields with defaults
    # unless the new fields are marked as keyword-only using dataclasses.field(kw_only=True).
    credentials: UserPass | OAuth2Credentials | SalesforceClientCredentials = dataclasses.field(kw_only=True)
    is_sandbox: bool = dataclasses.field(kw_only=True)
    my_domain: str = dataclasses.field(kw_only=True)

    async def fetch_instance_url(self, log: Logger, session: HTTPSession) -> str:
        # Used by the UserPass and Client Credentials flows, whose instance URL is only known after
        # the token exchange that fetch_token performs and caches on self._access_token.
        await self.fetch_token(log, session)
        assert isinstance(self._access_token, self.AccessTokenResponse)
        return self._access_token.instance_url

    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        if isinstance(self.credentials, UserPass):
            current_time = time.time()

            if self._access_token is not None:
                assert isinstance(self._access_token, self.AccessTokenResponse)

                horizon = self._fetched_at + self._access_token.expires_in * 0.75

                if current_time < horizon:
                    return (self.authorization_token_type, self._access_token.access_token)

            self._fetched_at = int(current_time)
            access_token, instance_url = self.credentials.fetch_access_token_and_instance_url(is_sandbox=self.is_sandbox, my_domain=self.my_domain)
            self._access_token = SalesforceTokenSource.AccessTokenResponse(
                access_token=access_token,
                token_type=self.authorization_token_type,
                instance_url=instance_url,
            )
            return (self.authorization_token_type, self._access_token.access_token)
        else:
            return await super().fetch_token(log, session)
