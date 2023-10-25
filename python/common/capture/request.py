from .. import flow
import typing as t

class Spec(t.TypedDict):
    connectorType: flow.ConnectorType
    config: dict

class Discover(t.TypedDict):
    connectorType: flow.ConnectorType
    config: dict

class ValidateBinding(t.TypedDict):
    collection: flow.CollectionSpec
    resourceConfig: dict

class Validate(t.TypedDict):
    name: str
    connectorType: flow.ConnectorType
    config: dict
    bindings: t.List[ValidateBinding]

class Apply(t.TypedDict):
    capture: flow.CaptureSpec
    version: str
    dryRun: bool

class Open(t.TypedDict):
    capture: flow.CaptureSpec
    version: str
    range: flow.RangeSpec
    state: dict

class Acknowledge(t.TypedDict):
    checkpoints: int