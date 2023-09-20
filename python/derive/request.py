from .. import flow
import typing as t

ConnectorType = t.Literal["IMAGE", "LOCAL", "SQLITE", "TYPESCRIPT"]

class Spec(t.TypedDict):
    connectorType: ConnectorType
    config: dict

class ValidateTransform(t.TypedDict):
    name: str
    collection: flow.CollectionSpec
    shuffleLambdaConfig: t.Optional[dict]
    lambdaConfig: dict

class Validate(t.TypedDict):
    connectorType: ConnectorType
    config: dict
    collection: flow.CollectionSpec
    transforms: t.List[ValidateTransform]
    shuffleKeyTypes: t.List[str]
    projectRoot: str
    importMap: dict[str, str]

class Open(t.TypedDict):
    collection: flow.CollectionSpec
    version: str
    range: flow.RangeSpec
    state: dict

class Shuffle(t.TypedDict):
    key: str
    packed: str
    hash: int

class Read(t.TypedDict):
    transform: int
    uuid: flow.UUIDParts
    shuffle: Shuffle
    doc: dict

class Flush(t.TypedDict):
    pass # Always empty.

class StartCommit(t.TypedDict):
    runtimeCheckpoint: flow.Checkpoint

class Reset(t.TypedDict):
    pass # Always empty.
