import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_mailchimp import SourceMailchimp

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceMailchimp(),
        oauth2=flow.OAuth2Spec(
            provider="mailchimp",
            authUrlTemplate=(
                "https://login.mailchimp.com/oauth2/authorize?response_type=code"
                r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            ),
            accessTokenUrlTemplate="https://login.mailchimp.com/oauth2/token",
            accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
            accessTokenBody=(
                "grant_type=authorization_code"
                r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            ),
            accessTokenResponseMap={
                "access_token": "/access_token"
            },
        ),
        schema_inference=False,
    ).serve()
)