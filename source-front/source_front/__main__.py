import estuary_cdk.pydantic_polyfill # Must be first.

import asyncio

from estuary_cdk import flow, shim_airbyte_cdk

from source_front import SourceFront

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceFront(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)