from flow_sdk import shim_airbyte_cdk
from source_recurly import SourceRecurly


shim_airbyte_cdk.CaptureShim(
    delegate=SourceRecurly(),
    oauth2=None
).main()