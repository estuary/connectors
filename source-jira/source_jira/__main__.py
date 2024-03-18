import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
from estuary_cdk import shim_airbyte_cdk
from source_jira import SourceJira


def wrap_with_braces(body: str, count: int):
    opening = "{" * count
    closing = "}" * count
    return f"{opening}{body}{closing}"


def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"


asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceJira(),
        oauth2=None,
        schema_inference=True,
    ).serve()
)
