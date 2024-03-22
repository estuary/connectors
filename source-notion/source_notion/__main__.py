import estuary_cdk.pydantic_polyfill  # Must be first.
import json

import asyncio
from estuary_cdk import shim_airbyte_cdk, flow
from source_notion import SourceNotion


def wrap_with_braces(body: str, count: int):
    opening = "{" * count
    closing = "}" * count
    return f"{opening}{body}{closing}"


def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"


accessTokenBody = {
    "grant_type": "authorization_code",
    "redirect_uri": "{{{ redirect_uri }}}",
    "code": "{{{ code }}}",
}

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceNotion(),
        oauth2=flow.OAuth2Spec(
            provider="notion",
            accessTokenBody=json.dumps(accessTokenBody),
            authUrlTemplate=(
                f"https://api.notion.com/v1/oauth/authorize?"
                f"client_id={urlencode_field(' client_id ')}&"
                f"redirect_uri={urlencode_field(' redirect_uri ')}&"
                f"response_type=code&"
                f"owner=user&"
                f"state={urlencode_field(' state ')}"
            ),
            accessTokenUrlTemplate=(f"https://api.notion.com/v1/oauth/token"),
            accessTokenResponseMap={
                "access_token": "/access_token",
            },
            accessTokenHeaders={
                "Authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}",
                "Content-Type": "application/json",
            },
        ),
        schema_inference=True,
    ).serve()
)
