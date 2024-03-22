import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_google_analytics_data_api import SourceGoogleAnalyticsDataApi

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceGoogleAnalyticsDataApi(),
        oauth2=flow.OAuth2Spec(
            provider="google",
            authUrlTemplate=(
                "https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent"
                r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&response_type=code"
                r"&scope=https://www.googleapis.com/auth/analytics.readonly"
                r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            ),
            accessTokenUrlTemplate="https://oauth2.googleapis.com/token",
            accessTokenHeaders={},
            accessTokenBody=(
                "grant_type=authorization_code"
                r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            ),
            accessTokenResponseMap={
                "refresh_token": "/refresh_token",
            },
        ),
        schema_inference=False,
    ).serve()
)
