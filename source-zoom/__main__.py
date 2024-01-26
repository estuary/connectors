from flow_sdk import shim_airbyte_cdk
from .source_zoom.source import BaseSource

shim_airbyte_cdk.CaptureShim(
    delegate = BaseSource(),
    oauth2 = {
        "provider": "zoom",
        "authUrlTemplate": (
            "https://zoom.us/oauth/authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            "&response_type=code"
        ),
        "accessTokenUrlTemplate": "https://zoom.us/oauth/token/",
        "accessTokenHeaders": {
            "content-type": "application/x-www-form-urlencoded",
            "authorization": r"Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}"
        },
        "accessTokenBody": (
            r"code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            "&grant_type=authorization_code"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token",
            "refresh_token": "/refresh_token",
            "token_expiry_date": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}"
        }
    }
).main()