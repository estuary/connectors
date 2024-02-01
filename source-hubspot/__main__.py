from flow_sdk import shim_airbyte_cdk
from source_hubspot import SourceHubspot

shim_airbyte_cdk.CaptureShim(
    delegate = SourceHubspot(),
    oauth2 = {
        "provider": "hubspot",
        "authUrlTemplate": (
            "https://app.hubspot.com/oauth/authorize?"
            r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            r"&scope=content%20crm.schemas.deals.read%20automation%20oauth%20crm.objects.owners.read%20forms%20files%20tickets%20e-commerce%20files.ui_hidden.read%20crm.schemas.custom.read%20crm.objects.custom.read%20sales-email-read%20forms-uploaded-files%20crm.objects.goals.read%20crm.objects.companies.read%20crm.lists.read%20crm.objects.deals.read%20crm.schemas.contacts.read%20crm.objects.contacts.read%20crm.schemas.companies.read"
            r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
        ),
        "accessTokenUrlTemplate": "https://api.hubapi.com/oauth/v1/token",
        "accessTokenHeaders": {
            "content-type": "application/x-www-form-urlencoded"
        },
        "accessTokenBody": (
            r"code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
            r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
            r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            "&grant_type=authorization_code"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token",
            "refresh_token": "/refresh_token",
            "token_expiry_date": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}"
        }
    }
).main()