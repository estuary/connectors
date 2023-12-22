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
            f"client_id={wrap_with_braces('client_id', 3)}&"
            r"redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&"
            "response_type=code"
        ),
        "accessTokenHeaders": {
            "Authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "accessTokenBody": (
            r"code={{ code }}&"
            "grant_type=authorization_code&"
            r"redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        ),
        "accessTokenUrlTemplate": "https://zoom.us/oauth/token",
        "accessTokenResponseMap": {
            "access_token":  "/access_token",
            "refresh_token": "/refresh_token"
        }
        # "accessTokenBody": f"grant_type=account_credentials&account_id={wrap_with_braces('account_id', 3)}",
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