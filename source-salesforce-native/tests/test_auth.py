import logging
from typing import cast

import pytest
from pydantic import ValidationError

from estuary_cdk.http import HTTPSession

from source_salesforce_native.auth import (
    OAUTH2_SPEC,
    SalesforceClientCredentials,
    SalesforceTokenSource,
    _login_domain_from_my_domain,
)
from source_salesforce_native.models import EndpointConfig

_LOG = logging.getLogger(__name__)


def _config_with_my_domain(my_domain: str) -> dict:
    # Minimal valid endpoint config; the OAuth/My Domain template only cares about my_domain.
    return {
        "my_domain": my_domain,
        "credentials": {
            "credentials_title": "Username, Password, & Security Token",
            "username": "svc@example.com",
            "password": "pw",
            "security_token": "tok",
        },
    }


@pytest.mark.parametrize(
    "my_domain, expected",
    [
        # A validated My Domain host is reduced to the subdomain that
        # simple_salesforce re-appends ".salesforce.com" to.
        ("mycompany.my.salesforce.com", "mycompany.my"),
        ("acme--uat.sandbox.my.salesforce.com", "acme--uat.sandbox.my"),
    ],
)
def test_login_domain_from_my_domain(my_domain: str, expected: str) -> None:
    assert _login_domain_from_my_domain(my_domain) == expected


@pytest.mark.parametrize(
    "my_domain",
    [
        "",  # optional — empty is the default and must validate
        "mycompany.my.salesforce.com",
        "acme--uat.sandbox.my.salesforce.com",
    ],
)
def test_endpoint_config_accepts_valid_my_domain(my_domain: str) -> None:
    # The control plane interpolates this value verbatim into the OAuth URLs, so it must be a
    # clean host; these forms are safe.
    assert EndpointConfig.model_validate(_config_with_my_domain(my_domain)).my_domain == my_domain


@pytest.mark.parametrize(
    "my_domain",
    [
        "https://mycompany.my.salesforce.com",  # scheme would double up in the URL
        "mycompany.my.salesforce.com/",  # trailing path
        "mycompany",  # bare My Domain name -> https://mycompany/... (broken)
        "mycompany.my",  # missing .salesforce.com
        "na123.salesforce.com",  # instanced URL, not a My Domain
        "login.salesforce.com",  # standard host, not a My Domain
    ],
)
def test_endpoint_config_rejects_malformed_my_domain(my_domain: str) -> None:
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(_config_with_my_domain(my_domain))


def _client_credentials_config(my_domain: str) -> dict:
    return {
        "my_domain": my_domain,
        "credentials": {
            "credentials_title": "Client Credentials",
            "client_id": "KEY",
            "client_secret": "SECRET",
        },
    }


def test_client_credentials_requires_my_domain() -> None:
    # The Client Credentials flow must hit the org's My Domain token endpoint, so My Domain is
    # required for this auth method even though it's optional for the others.
    with pytest.raises(ValidationError, match="My Domain"):
        EndpointConfig.model_validate(_client_credentials_config(""))


def test_client_credentials_with_my_domain_resolves() -> None:
    cfg = EndpointConfig.model_validate(_client_credentials_config("mycompany.my.salesforce.com"))
    assert isinstance(cfg.credentials, SalesforceClientCredentials)


def _client_credentials_token_source() -> SalesforceTokenSource:
    return SalesforceTokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=SalesforceClientCredentials(client_id="KEY", client_secret="SECRET"),
        is_sandbox=False,
        my_domain="mycompany.my.salesforce.com",
    )


class _FakeTokenEndpoint:
    # Minimal HTTPSession stand-in: its `request` returns the given token response serialized to JSON,
    # so the real Client Credentials token exchange and AccessTokenResponse parsing run without a
    # network call.
    def __init__(self, token_response: SalesforceTokenSource.AccessTokenResponse) -> None:
        self._body = token_response.model_dump_json().encode()

    async def request(self, _log: logging.Logger, _url: str, **_kwargs: object) -> bytes:
        return self._body


@pytest.mark.asyncio
async def test_fetch_instance_url_returns_value_from_token_response() -> None:
    ts = _client_credentials_token_source()
    session = _FakeTokenEndpoint(
        SalesforceTokenSource.AccessTokenResponse(
            access_token="tok",
            token_type="Bearer",
            instance_url="https://mycompany.my.salesforce.com",
        )
    )
    assert await ts.fetch_instance_url(_LOG, cast(HTTPSession, session)) == "https://mycompany.my.salesforce.com"

