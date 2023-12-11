from flow_sdk import shim_airbyte_cdk
from source_bing_ads import SourceBingAds

scopes = " ".join([
    "openid",
    "profile",
    "https://ads.microsoft.com/msads.manage",
    "offline_access"
])

def wrap_with_braces(body: str, count: int):
    opening = '{'*count
    closing = '}'*count
    return f"{opening}{body}{closing}"

def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"

shim_airbyte_cdk.CaptureShim(
    delegate=SourceBingAds(),
    oauth2=
    {
        "provider": "microsoft",
        "accessTokenBody": (
            f"client_id={urlencode_field('client_id')}&"
            f"scope={scopes}&"
            f"code={urlencode_field('code')}&"
            f"grant_type=authorization_code&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
            f"client_secret={urlencode_field('client_secret')}"
        ),
        "authUrlTemplate": (
            f"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"
            f"client_id={urlencode_field('client_id')}&"
            f"scope={urlencode_field(scopes)}&"
            f"response_type=code&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
            f"state={urlencode_field('state')}&"
            f"response_type=query&"
            f"prompt=login"
        ),
        "accessTokenHeaders": {
            "content-type": "application/x-www-form-urlencoded"
        },
        "accessTokenResponseMap": {
            "refresh_token": "/refresh_token",
        },
        "accessTokenUrlTemplate": (
            f"https://login.microsoftonline.com/common/oauth2/v2.0/token"
            f"client_id={urlencode_field('client_id')}&"
            f"scope={urlencode_field(scopes)}&"
            f"code={urlencode_field('code')}&"
            f"grant_type=authorization_code&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
        )
    },
).main()