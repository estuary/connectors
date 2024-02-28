import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
from estuary_cdk import shim_airbyte_cdk, flow
from source_asana import SourceAsana


def wrap_with_braces(body: str, count: int):
    opening = "{" * count
    closing = "}" * count
    return f"{opening}{body}{closing}"


def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceAsana(),
        oauth2=flow.OAuth2Spec(
            provider="asana",
            authUrlTemplate=(
                f"https://app.asana.com/-/oauth_authorize?"
                f"client_id={wrap_with_braces('client_id',3)}&"
                f"redirect_uri={urlencode_field('redirect_uri')}&"
                f"response_type=code&"
                f"state={urlencode_field('state')}&"
                f"scope=default"
            ),
            accessTokenUrlTemplate=(
                f"https://app.asana.com/-/oauth_token?"
                f"grant_type=authorization_code&"
                f"client_id={wrap_with_braces('client_id',3)}&"
                f"client_secret={wrap_with_braces('client_secret',3)}&"
                f"redirect_uri={urlencode_field('redirect_uri')}&"
                f"code={urlencode_field('code')}"
            ),
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
