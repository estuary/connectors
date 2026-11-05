import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_twilio import SourceTwilio

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceTwilio(),
        oauth2=None,
        schema_inference=False,
    ).serve()
)
