from .. import flow
import typing as t

class DiscoveredBinding(t.TypedDict):
    recommendedName: str
    resourceConfig: dict
    documentSchema: dict
    key: t.List[str]
    disable: t.NotRequired[bool]

class Discovered(t.TypedDict):
    bindings: t.List[DiscoveredBinding]

class ValidatedBinding(t.TypedDict):
    resourcePath: t.List[str]

class Validated(t.TypedDict):
    bindings: t.List[ValidatedBinding]

class Applied(t.TypedDict, total=False):
    actionDescription: str

class Opened(t.TypedDict):
    explicitAcknowledgements: bool

class Captured(t.TypedDict):
    binding: int
    doc: dict

class Checkpoint(t.TypedDict):
    state: flow.ConnectorState
