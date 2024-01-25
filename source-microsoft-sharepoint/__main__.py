from flow_sdk import shim_airbyte_cdk
from .source_microsoft_sharepoint.source import BaseSource

shim_airbyte_cdk.CaptureShim(
    delegate = BaseSource(),
    oauth2 = {
        "provider": "microsoft",
        "authUrlTemplate": (
            r"https://login.microsoftonline.com/{{{ config.tenant_id }}}/oauth2/v2.0/authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            r"&scope=offline_access%20files.read.all%20sites.read.all"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            r"&response_type=code"
            r"&prompt=login"
        ),
        "accessTokenBody": (
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&scope=offline_access%20files.read.all%20sites.read.all"
            r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            r"&grant_type=authorization_code"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        ),
        "accessTokenUrlTemplate": r"https://login.microsoftonline.com/{{{ config.tenant_id }}}/oauth2/v2.0/token",
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