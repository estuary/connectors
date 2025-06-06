import estuary_cdk.pydantic_polyfill  # Must be first.
import estuary_cdk.requests_session_send_patch # Must be second.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_klaviyo import SourceKlaviyo


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceKlaviyo(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)