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