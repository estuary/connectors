from .. import flow
import typing as t

class ValidatedTransform(t.TypedDict):
    readOnly: bool

class Validated(t.TypedDict):
    transforms: t.List[ValidatedTransform]
    generatedFiles: dict[str, str]

class Opened(t.TypedDict):
    pass

class Published(t.TypedDict):
    doc: dict

class Flushed(t.TypedDict):
    pass 

class StartedCommit(t.TypedDict):
    state: flow.ConnectorState