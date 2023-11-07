from flow_sdk import shim_singer_sdk
from tap_shopify.tap import Tap_Shopify

shim_singer_sdk.CaptureShim(
    config_schema=Tap_Shopify.config_jsonschema,
    delegate_factory=Tap_Shopify,
).main()