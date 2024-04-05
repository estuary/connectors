import estuary_cdk.pydantic_polyfill  # Must be first. # noqa

import asyncio
from estuary_cdk import shim_airbyte_cdk, flow
from source_linkedin_pages import SourceLinkedinPages
import urllib.parse


def wrap_with_braces(body: str, count: int) -> str:
    opening = "{" * count
    closing = "}" * count
    return f"{opening}{body}{closing}"


def urlencode_field(field: str) -> str:
    return (
        f"{wrap_with_braces('#urlencode',2)}"
        f"{wrap_with_braces(field,3)}"
        f"{wrap_with_braces('/urlencode',2)}"
    )


def build_auth_url_template() -> str:
    scopes = " ".join(
        [
            "r_organization_followers",
            "r_organization_social",
            "rw_organization_admin",
            "r_organization_social_feed",
            "w_member_social",
            "w_organization_social",
            "r_basicprofile",
            "w_organization_social_feed",
            "w_member_social_feed",
            "r_1st_connections_size",
        ]
    )
    scopes = urllib.parse.quote(scopes)
    params = {
        "client_id": wrap_with_braces("client_id", 3),
        "redirect_uri": urlencode_field("redirect_uri"),
        "response_type": "code",
        "state": urlencode_field("state"),
        "scope": "{" + scopes + "}",
    }

    query = "&".join([f"{k}={v}" for k, v in params.items()])
    url = urllib.parse.urlunsplit(
        [
            "https",  # scheme
            "www.linkedin.com",  # netloc
            "/oauth/v2/authorization",  # path
            query,  # query
            "",  # fragment
        ]
    )
    return url


def build_access_token_url_template() -> str:
    params = {
        "grant_type": "authorization_code",
        "code": urlencode_field("code"),
        "client_id": wrap_with_braces("client_id", 3),
        "client_secret": wrap_with_braces("client_secret", 3),
        "redirect_uri": urlencode_field("redirect_uri"),
    }
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    url = urllib.parse.urlunsplit(
        [
            "https",  # scheme
            "www.linkedin.com",  # netloc
            "/oauth/v2/accessToken",  # path
            query,  # query
            "",  # fragment
        ]
    )
    return url


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceLinkedinPages(),
        oauth2=flow.OAuth2Spec(
            provider="linkedin",
            authUrlTemplate=build_auth_url_template(),
            accessTokenUrlTemplate=build_access_token_url_template(),
            accessTokenResponseMap={
                "access_token": "/access_token",
                "refresh_token": "/refresh_token",
            },
            accessTokenBody="",  # Uses query arguments.
            accessTokenHeaders={},
        ),
        schema_inference=True,
    ).serve()
)
