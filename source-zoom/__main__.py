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
            f"response_type=code&"
            f"client_id={wrap_with_braces('client_id',3)}&"
            f"redirect_uri={wrap_with_braces('redirect_uri',3)}"
        ),
        "accessTokenHeaders": {
            "Authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "accessTokenUrlTemplate": (
            f"https://zoom.us/oauth/token?"
            f"grant_type=authorization_code&"
            f"redirect_uri={wrap_with_braces('redirect_uri',3)}&"
            f"code={urlencode_field('code')}"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token"
        }
    }
).main()