from flow_sdk import shim_airbyte_cdk
from source_airtable import SourceAirtable

shim_airbyte_cdk.CaptureShim(
    delegate=SourceAirtable(),
    oauth2={
        "provider": "airtable", 
        "accessTokenBody": r"grant_type=authorization_code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}&code_verifier={{#urlencode}}{{{ code_verifier }}}{{/urlencode}}", "authUrlTemplate": "https://airtable.com/oauth2/v1/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}&scope=data.records:read%20data.recordComments:read%20schema.bases:read&code_challenge={{#urlencode}}{{{ code_challenge }}}{{/urlencode}}&code_challenge_method={{{ code_challenge_method }}}", 
        "accessTokenHeaders": {
            "content-type": "application/x-www-form-urlencoded", 
            "authorization": "Basic {{#basicauth}}{{{ client_id }}}:{{{client_secret }}}{{/basicauth}}"
        }, 
        "accessTokenResponseMap": {
            "access_token": "/access_token", 
            "refresh_token": "/refresh_token", 
            "token_expiry_date": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}"
        }, 
        "accessTokenUrlTemplate": "https://airtable.com/oauth2/v1/token"
    }
).main()