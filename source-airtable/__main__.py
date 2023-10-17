from python import shim_airbyte_cdk
from source_airtable import SourceAirtable

shim_airbyte_cdk.CaptureShim(
    delegate=SourceAirtable(),
    oauth2=None
).main()