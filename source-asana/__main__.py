from flow_sdk import shim_airbyte_cdk
from source_asana import SourceAsana

shim_airbyte_cdk.CaptureShim(
    delegate=SourceAsana(),
    oauth2=None
).main()