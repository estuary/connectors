import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
from estuary_cdk import shim_airbyte_cdk
from source_stripe import SourceStripe


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceStripe(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
