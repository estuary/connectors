from flow_sdk import shim_airbyte_cdk
from source_facebook_marketing import SourceFacebookMarketing

shim_airbyte_cdk.CaptureShim(
	delegate = SourceFacebookMarketing(),
	oauth2 =
        {
            "provider": "facebook",
            "authUrlTemplate": "https://www.facebook.com/v19.0/dialog/oauth?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{#urlencode}}{{{  state }}}{{/urlencode}}&scope=ads_management,ads_read,read_insights,business_management",
            "accessTokenResponseMap": {
                "access_token": "/access_token"
            },
            "accessTokenUrlTemplate": "https://graph.facebook.com/v19.0/oauth/access_token?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        },
    usesSchemaInference=False
).main()
