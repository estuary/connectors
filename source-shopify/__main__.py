from flow_sdk import shim_singer_sdk
from tap_shopify.tap import Tap_Shopify

scopes = ",".join([
    "read_locales",
    "read_products",
    # TODO (jshearer): Switch to `read_all_orders` once our app is granted permission
    "read_orders",
    "read_checkouts",
    "read_locations",
    "read_inventory",
    "read_fulfillments",
    "read_customers"
])

def wrap_with_braces(body: str, count: int):
    opening = '{'*count
    closing = '}'*count
    return f"{opening}{body}{closing}"

def urlencode_field(field: str):
    return f"{wrap_with_braces('#urlencode',2)}{wrap_with_braces(field,3)}{wrap_with_braces('/urlencode',2)}"

shim_singer_sdk.CaptureShim(
    config_schema=Tap_Shopify.config_jsonschema,
    delegate_factory=Tap_Shopify,
    oauth2={
        "provider": "shopify",
        "authUrlTemplate": (
            f"https://{wrap_with_braces('config.store',3)}.myshopify.com/admin/oauth/authorize?"
            f"client_id={wrap_with_braces('client_id',3)}&"
            f"scope={scopes}&"
            f"redirect_uri={wrap_with_braces('redirect_uri',3)}&"
            f"state={urlencode_field('state')}"
        ),
        "accessTokenUrlTemplate": (
            f"https://{wrap_with_braces('config.store',3)}.myshopify.com/admin/oauth/access_token?"
            f"client_id={wrap_with_braces('client_id',3)}&"
            f"client_secret={wrap_with_braces('client_secret',3)}&"
            f"code={urlencode_field('code')}"
        ),
        "accessTokenResponseMap": {
            "access_token": "/access_token"
        }
    }
).main()