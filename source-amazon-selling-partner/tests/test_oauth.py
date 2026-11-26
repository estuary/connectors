import logging
import re
from typing import TypedDict, cast
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from estuary_cdk.capture import request
from estuary_cdk.flow import BaseOAuth2Credentials, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin
from pydantic import ValidationError as ModelValidationError

from source_amazon_selling_partner import Connector
from source_amazon_selling_partner.models import (
    APPLICATION_ID,
    LWA_TOKEN_URL,
    EndpointConfig,
    LoginWithAmazonCredentials,
    Region,
    build_config_and_oauth,
    lwa_credentials,
)
from source_amazon_selling_partner.resources import validate_credentials

from .fake_amazon import FakeAmazon
from .test_orders import NOW

APP_ID = "test-application-id"


def render(template: str, values: dict[str, str]) -> str:
    """Render only the urlencode sections and triple variables these templates use."""
    def encoded(match: re.Match[str]) -> str:
        content = re.sub(r"\{\{\{\s*(\w+)\s*\}\}\}", lambda var: values[var[1]], match[1])
        assert "{{" not in content
        return quote(content, safe="")

    result = re.sub(r"\{\{#urlencode\}\}(.*?)\{\{/urlencode\}\}", encoded, template)
    assert "{{" not in result
    return result


def test_oauth_templates():
    _, oauth = build_config_and_oauth(APP_ID)
    assert oauth is not None
    values = {
        "state": "state +/&=?雪",
        "code": "code +/&=?雪",
        "redirect_uri": "https://example.com/oauth?x=1&y=two words",
        "client_id": "client +/&=?雪",
        "client_secret": "secret +/&=?雪",
    }
    assert oauth.accessTokenUrlTemplate == LWA_TOKEN_URL
    assert oauth.accessTokenHeaders == {"content-type": "application/x-www-form-urlencoded"}
    assert oauth.accessTokenResponseMap == {"refresh_token": "/refresh_token"}
    consent = urlsplit(render(oauth.authUrlTemplate, values))
    assert (consent.scheme, consent.netloc, consent.path) == (
        "https", "sellercentral.amazon.com", "/apps/authorize/consent",
    )
    assert parse_qs(consent.query) == {
        "application_id": [APP_ID],
        "state": [values["state"]],
        "redirect_uri": [values["redirect_uri"]],
        "version": ["beta"],
    }
    assert parse_qs(render(oauth.accessTokenBody, values)) == {
        "grant_type": ["authorization_code"],
        **{key: [values[key]] for key in ("code", "redirect_uri", "client_id", "client_secret")},
    }


class ConfigInput(TypedDict):
    credentials: dict[str, str]
    region: str
    marketplace_ids: list[str]


def config_input(title: str = "Login with Amazon", region: str = "North America") -> ConfigInput:
    return {
        "credentials": {
            "credentials_title": title,
            "client_id": "test-client",
            "client_secret": "test-secret",
            "refresh_token": "test-refresh",
        },
        "region": region,
        "marketplace_ids": ["ATVPDKIKX0DER"],
    }


@pytest.mark.parametrize("app_id", [None, APP_ID])
def test_existing_lwa_config_and_refresh(app_id: str | None):
    model, _ = build_config_and_oauth(app_id)
    data = config_input()
    # Existing configs may omit the LWA discriminator.
    del data["credentials"]["credentials_title"]
    config = model.model_validate(data)
    assert isinstance(config.credentials, LoginWithAmazonCredentials)
    refresh = lwa_credentials(config.credentials)
    assert isinstance(refresh, BaseOAuth2Credentials)
    assert refresh.grant_type == "refresh_token"
    for key in ("client_id", "client_secret", "refresh_token"):
        assert getattr(refresh, key) == data["credentials"][key]


def test_enabled_union_and_identical_refresh_credentials():
    model, oauth = build_config_and_oauth(APP_ID)
    assert oauth is not None
    manual = model.model_validate(config_input())
    managed = model.model_validate(config_input("OAuth Credentials"))
    assert lwa_credentials(managed.credentials) is managed.credentials
    assert lwa_credentials(manual.credentials).model_dump() == lwa_credentials(managed.credentials).model_dump()
    schema = model.model_json_schema()
    credentials = schema["properties"]["credentials"]
    mapping = credentials["discriminator"]["mapping"]
    assert credentials["oneOf"][0]["$ref"] == mapping["Login with Amazon"]
    arm = schema["$defs"][mapping["OAuth Credentials"].rsplit("/", 1)[-1]]
    assert arm["x-oauth2-provider"] == oauth.provider == "amazon-selling-partner"
    assert "North America" in arm["title"]
    assert "North America" in arm["description"]


@pytest.mark.asyncio
async def test_default_spec_has_only_lwa():
    assert APPLICATION_ID is None
    model, oauth = build_config_and_oauth(None)
    assert oauth is None
    spec = await Connector().spec(logging.getLogger("test"), request.Spec())
    assert spec.oauth2 is None
    assert spec.configSchema == model.model_json_schema() == EndpointConfig.model_json_schema()
    assert "OAuth2Credentials" not in str(spec.configSchema)
    with pytest.raises(ModelValidationError):
        model.model_validate(config_input("OAuth Credentials"))


@pytest.mark.asyncio
@pytest.mark.parametrize("region", [Region.EUROPE, Region.FAR_EAST])
async def test_managed_oauth_rejects_other_regions_before_network(region: str):
    model, _ = build_config_and_oauth(APP_ID)
    config = model.model_validate(config_input("OAuth Credentials", region))
    with pytest.raises(ValidationError, match="use Login with Amazon credentials for this region"):
        await validate_credentials(logging.getLogger("test"), None, config)  # pyright: ignore[reportArgumentType]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "title, region",
    [("OAuth Credentials", Region.NORTH_AMERICA), ("Login with Amazon", Region.EUROPE)],
)
async def test_region_guard_allows_supported_credentials(title: str, region: str):
    model, _ = build_config_and_oauth(APP_ID)
    config = model.model_validate(config_input(title, region))
    fake = FakeAmazon(now=NOW, orders=[])

    class Http:
        token_source = None
        request_stream = fake.request_stream

    await validate_credentials(logging.getLogger("test"), cast(HTTPMixin, Http()), config)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "title, guidance",
    [
        (
            "OAuth Credentials",
            "Please re-authenticate with Amazon and confirm the selling account "
            "authorized the Estuary application in the selected region.",
        ),
        (
            "Login with Amazon",
            "Please confirm the client ID, client secret and refresh token are correct, "
            "and that the selling account authorized this application in the selected region.",
        ),
    ],
)
async def test_credential_error_guidance_per_arm(title: str, guidance: str):
    model, _ = build_config_and_oauth(APP_ID)
    config = model.model_validate(config_input(title))

    class Http:
        token_source = None

        async def request_stream(self, *args: object, **kwargs: object):
            raise HTTPError("Amazon rejected authorization", 403)

    with pytest.raises(ValidationError) as exc:
        await validate_credentials(logging.getLogger("test"), cast(HTTPMixin, Http()), config)
    assert exc.value.errors == [f"Invalid credentials. {guidance}\n\nAmazon rejected authorization"]
    if title == "OAuth Credentials":
        assert "client ID" not in str(exc.value)
        assert "client secret" not in str(exc.value)
        assert "refresh token" not in str(exc.value)
