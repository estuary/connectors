from flow_sdk import shim_airbyte_cdk
from source_google_ads import SourceGoogleAds
import json


def wrap_with_braces(body: str, count: int):
    opening = "{" * count
    closing = "}" * count
    return f"{opening}{body}{closing}"


def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"

accessTokenBody = {
    "grant_type": "authorization_code",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
    "code": "{{{ code }}}",
}

shim_airbyte_cdk.CaptureShim(
    delegate=SourceGoogleAds(),
    oauth2={
        "provider": "google",
        "accessTokenBody": json.dumps(accessTokenBody),
        "authUrlTemplate": (
            f"https://accounts.google.com/o/oauth2/auth?"
            f"access_type=offline&"
            f"prompt=consent&"
            f"client_id={urlencode_field('client_id')}&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
            f"response_type=code&"
            f"scope=https://www.googleapis.com/auth/adwords&state={urlencode_field('state')}"
        ),
        "accessTokenResponseMap": {"refresh_token": "/refresh_token"},
        "accessTokenUrlTemplate": "https://oauth2.googleapis.com/token",
    },
).main()
