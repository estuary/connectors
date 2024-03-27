from flow_sdk import shim_singer_sdk
from tap_criteo.tap import TapCriteo

shim_singer_sdk.CaptureShim(
    config_schema=TapCriteo.config_jsonschema,
    delegate_factory=TapCriteo,
).main()
