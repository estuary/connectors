import estuary_cdk.pydantic_polyfill # Must be first.

import asyncio
import json

from estuary_cdk import flow, shim_airbyte_cdk

from source_zendesk_support import SourceZendeskSupport

def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"

accessTokenBody = {
    "grant_type": "authorization_code",
    "code": "{{{ code }}}",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
    "scope": "read"
}

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceZendeskSupport(),
        oauth2=flow.OAuth2Spec(
            provider="zendesk",
            accessTokenBody=json.dumps(accessTokenBody),
            authUrlTemplate=(
                "https://{{{ config.subdomain }}}.zendesk.com/oauth/authorizations/new?"
                f"response_type=code&"
                f"client_id={urlencode_field('client_id')}&"
                f"redirect_uri={urlencode_field('redirect_uri')}&"
                f"scope=read&"
                f"state={urlencode_field('state')}"
            ),
            accessTokenUrlTemplate=("https://{{{ config.subdomain }}}.zendesk.com/oauth/tokens"),
            accessTokenResponseMap={
                "access_token": "/access_token",
            },
            accessTokenHeaders={
                "Content-Type": "application/json",
            },
        ),
        schema_inference=False,
    ).serve()
)
