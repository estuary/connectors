import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_jira_legacy import SourceJira



asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceJira(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
