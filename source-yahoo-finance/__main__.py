from python import shim_singer_sdk
from tap_investing.tap import TapInvesting

shim_singer_sdk.CaptureShim(
    config_schema=TapInvesting.config_jsonschema,
    delegate_factory=TapInvesting,
).main()