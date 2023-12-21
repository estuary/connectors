from flow_sdk import shim_airbyte_cdk
from source_zoom import SourceZoom
import base64

def wrap_with_braces(body: str, count: int):
    opening = '{'*count
    closing = '}'*count
    return f"{opening}{body}{closing}"

def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"

shim_airbyte_cdk.CaptureShim(
    delegate=SourceZoom(),
    oauth2= {
        "provider": "zoom",
        "authUrlTemplate": (
            f"https://zoom.us/oauth/authorize?"
            r"client_id={{ client_id }}&"
            r"redirect_uri={{#urlencode}}{{{ redirect_uri }}}/{{/urlencode}}&"
            "response_type=code"
        ),
        "accessTokenHeaders": {
            "Authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "accessTokenUrlTemplate": (
            f"https://zoom.us/oauth/token?"
            f"grant_type=authorization_code&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
            f"code={urlencode_field('code')}"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token"
        }
        # "accessTokenBody": r"grant_type=account_credentials&account_id=ZO9F-j1DRwey_9r4TjgA5A",
        # "accessTokenUrlTemplate": "https://zoom.us/oauth/token/",
        # "accessTokenHeaders": {
        #     "content-type": "application/x-www-form-urlencoded",
        #     "authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}"
        # },
        # "accessTokenResponseMap": {
        #     "access_token": "/access_token",
        #     "token_expiry_date": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}"
        # }
    }
).main()