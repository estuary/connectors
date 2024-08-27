import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_mixpanel import SourceMixpanel


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceMixpanel(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)