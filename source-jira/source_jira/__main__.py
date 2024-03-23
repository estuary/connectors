import estuary_cdk.pydantic_polyfill  # Must be first. # noqa

import asyncio
from estuary_cdk import shim_airbyte_cdk, flow
from source_jira import SourceJira


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceJira(),
        oauth2=flow.OAuth2Spec(
            provider="",
            authUrlTemplate="",
            accessTokenUrlTemplate="",
            accessTokenBody="",
            accessTokenHeaders={},
            accessTokenResponseMap={},
        ),
        schema_inference=True,
    ).serve()
)
