from python import shim_singer_sdk
from .tap import TapNetSuite

shim_singer_sdk.CaptureShim(
    config_schema=TapNetSuite.config_jsonschema,
    delegate_factory=TapNetSuite,
).main()