import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_hubspot import SourceHubspot

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceSalesforce(),
        oauth2={
        "provider": "salesforce",
        "authUrlTemplate": (
        r"https://{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}.salesforce.com/services/oauth2/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
        ),
        "accessTokenUrlTemplate": r"https://{{#config.is_sandbox}}test{{/config.is_sandbox}}{{^config.is_sandbox}}login{{/config.is_sandbox}}.salesforce.com/services/oauth2/token",
        "accessTokenBody": (
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
        ),
        "accessTokenHeaders": {"content-type": "application/x-www-form-urlencoded"},
        "accessTokenResponseMap": {"refresh_token": "/refresh_token"}
    },
        usesSchemaInference=False,
).serve()
)