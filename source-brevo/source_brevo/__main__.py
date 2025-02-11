import estuary_cdk.pydantic_polyfill # Must be first.
import estuary_cdk.requests_session_send_patch # Must be second.

import asyncio

from estuary_cdk import flow, shim_airbyte_cdk

from source_brevo import SourceBrevo

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceBrevo(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)