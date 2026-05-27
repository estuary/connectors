import pytest
from pydantic import ValidationError

from source_salesforce_native.auth import _login_domain_from_my_domain
from source_salesforce_native.models import EndpointConfig


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
