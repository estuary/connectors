from flow_sdk import shim_airbyte_cdk
from source_zoom import SourceZoom

shim_airbyte_cdk.CaptureShim(
    delegate=SourceZoom(),
    oauth2=None
).main()