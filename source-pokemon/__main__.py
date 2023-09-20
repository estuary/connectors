from python import shim_airbyte_cdk
from .source import SourcePokemon

shim_airbyte_cdk.CaptureShim(
    delegate=SourcePokemon(),
    oauth2=None,
).main()