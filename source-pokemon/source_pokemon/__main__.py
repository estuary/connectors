import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
from estuary_cdk import shim_airbyte_cdk
from .source import SourcePokemon

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourcePokemon(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
