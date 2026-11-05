import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_recharge import SourceRecharge


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceRecharge(),
        oauth2=None,
        schema_inference=False,
    ).serve()
)