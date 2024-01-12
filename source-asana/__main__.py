from flow_sdk import shim_airbyte_cdk
from python.base_connector.source import BaseSource

shim_airbyte_cdk.CaptureShim(
    delegate = BaseSource("source-asana"),
    oauth2 = {
        "provider": "asana",
        "authUrlTemplate": (
            "https://app.asana.com/-/oauth_authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            "&response_type=code"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            "&scope=default"
        ),
        "accessTokenUrlTemplate": (
            "https://app.asana.com/-/oauth_token?"
            "grant_type=authorization_code"
            r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token",
            "refresh_token": "/refresh_token"
        }
    }
).main()