import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_linkedin_ads_v2 import SourceLinkedinAds



asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceLinkedinAds(),
        oauth2=flow.OAuth2Spec(
            provider="linkedin",
            authUrlTemplate=(
                "https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&scope=r_emailaddress%20r_liteprofile%20r_ads%20r_ads_reporting%20r_organization_social&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            ),
            accessTokenUrlTemplate="https://www.linkedin.com/oauth/v2/accessToken",
            accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
            accessTokenBody=(
                "grant_type=authorization_code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            ),
            accessTokenResponseMap={
                "refresh_token": "/refresh_token",
            },
        ),
        schema_inference=False,
    ).serve()
)


