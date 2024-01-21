from flow_sdk import shim_airbyte_cdk
from .source_microsoft_onedrive.source import BaseSource

shim_airbyte_cdk.CaptureShim(
    delegate = BaseSource(),
    oauth2 = {
        "provider": "microsoft",
        "authUrlTemplate": (
            "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            "&scope=offline_access%20user.read%20mail.read%20files.read.all"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            "&response_type=code"
            "&prompt=login"
        ),
        "accessTokenBody": (
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&scope=offline_access%20user.read%20mail.read%20files.read.all"
            r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            "&grant_type=authorization_code"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        ),
        "accessTokenUrlTemplate": "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        "accessTokenHeaders": {
            "content-type": "application/x-www-form-urlencoded",
        },
        "accessTokenResponseMap": {
            "access_token": "/access_token",
            "refresh_token": "/refresh_token",
            "token_expiry_date": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}"
        },
    }
).main()