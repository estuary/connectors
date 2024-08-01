import estuary_cdk.pydantic_polyfill # Must be first.

import asyncio

from estuary_cdk import flow, shim_airbyte_cdk

from source_zendesk_support import SourceZendeskSupport

def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"

accessTokenBody = (
    f"grant_type=authorization_code&"
    f"client_id={urlencode_field('client_id')}&"
    f"client_secret={urlencode_field('client_secret')}&"
    f"redirect_uri={urlencode_field('redirect_uri')}&"
    f"code={urlencode_field('code')}"
)

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceZendeskSupport(),
        oauth2=flow.OAuth2Spec(
            provider="zendesk",
            accessTokenBody=accessTokenBody,
            authUrlTemplate=(
                f"https://estuarysupport.zendesk.com/oauth/authorizations/new?"
                f"response_type=code&"
                f"client_id={urlencode_field('client_id')}&"
                f"redirect_uri={urlencode_field('redirect_uri')}&"
                f"scope=read&"
                f"state={urlencode_field('state')}"
            ),
            accessTokenUrlTemplate=(f"https://estuarysupport.zendesk.com/oauth/tokens"),
            accessTokenResponseMap={
                "access_token": "/access_token",
            },
            accessTokenHeaders={
                "content-type": "application/x-www-form-urlencoded",
            },
        ),
        schema_inference=False,
    ).serve()
)
