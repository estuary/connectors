import estuary_cdk.pydantic_polyfill # Must be first.

import asyncio

from estuary_cdk import flow, shim_airbyte_cdk

from source_pendo import SourcePendo

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourcePendo(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
