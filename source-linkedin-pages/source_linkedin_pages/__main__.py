import estuary_cdk.pydantic_polyfill  # Must be first. # noqa

import asyncio
from estuary_cdk import shim_airbyte_cdk
from source_linkedin_pages import SourceLinkedinPages

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceLinkedinPages(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
