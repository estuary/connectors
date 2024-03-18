from flow_sdk import shim_airbyte_cdk
from source_linkedin_pages.source_linkedin_pages import SourceLinkedinPages


scopes = " ".join([
    "r_organization_followers",
    "r_organization_social",
    "rw_organization_admin",
    "r_organization_social_feed",
    "w_member_social",
    "w_organization_social",
    "r_basicprofile",
    "w_organization_social_feed",
    "w_member_social_feed",
    "r_1st_connections_size"
])

def wrap_with_braces(body: str, count: int):
    opening = '{'*count
    closing = '}'*count
    return f"{opening}{body}{closing}"

def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"

shim_airbyte_cdk.CaptureShim(
    delegate=SourceLinkedinPages(),
    oauth2={
        "provider": "linkedin",
        "authUrlTemplate": (
            f"https://www.linkedin.com/oauth/v2/authorization?"
            f"client_id={wrap_with_braces('client_id',3)}&"
            f"redirect_uri={urlencode_field('redirect_uri')}&"
            f"response_type=code&"
            f"state={wrap_with_braces('state', 3)}&"
            f"scope={scopes}"
            
        ),
        "accessTokenHeaders": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "accessTokenUrlTemplate": (
            f"https://www.linkedin.com/oauth/v2/accessToken?"
            f"grant_type=authorization_code&"
            f"code={urlencode_field('code')}&"
            f"client_id={wrap_with_braces('client_id',3)}&"
            f"client_secret={wrap_with_braces('client_secret',3)}&"
            f"redirect_uri={urlencode_field('redirect_uri')}"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token",
            "refresh_token": "/refresh_token"
        }
    }
).main()