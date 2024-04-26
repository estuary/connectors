import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_github import SourceGithub


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceGithub(),
        oauth2=flow.OAuth2Spec(
            provider="github",
            authUrlTemplate=(
                "https://github.com/login/oauth/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&scope=repo%20read:org%20read:repo_hook%20read:user%20read:discussion%20workflow"
                r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            ),
            accessTokenUrlTemplate="https://github.com/login/oauth/access_token",
            accessTokenHeaders={"Accept": "application/json"},
            accessTokenBody=(
                '{"client_id": "{{{ client_id }}}", "client_secret": "{{{ client_secret }}}", "redirect_uri": "{{{ redirect_uri }}}", "code": "{{{ code }}}"}'
            ),
            accessTokenResponseMap={"access_token": "/access_token"},
        ),
        schema_inference=False,
    ).serve()
)
