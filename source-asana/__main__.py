from flow_sdk import shim_airbyte_cdk
from source_asana import SourceAsana

shim_airbyte_cdk.CaptureShim(
    delegate=SourceAsana(),
    oauth2={
        "provider": "asana",
        "authUrlTemplate": (
            f"https://app.asana.com/-/oauth_authorize?"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"response_type=code&"
            f"state={urlencode_field('state')}&"
            f"scope=default"
            
        ),
        "accessTokenUrlTemplate": (
            f"https://app.asana.com/-/oauth_token?"
            f"grant_type=authorzation_code&"
            f"client_id={client_id}&"
            f"client_secret={client_secret}&"
            f"redirect_uri={redirect_uri}&"
            f"code={urlencode_field('code')}"
        ),
        "accessTokenReponseMap": {
            "access_token": "/access_token"
        }
    }
).main()