import typing as t

ConnectorType = t.Literal["IMAGE", "LOCAL"]

class CollectionSpec(t.TypedDict):
    name: str
    writeSchema: dict
    readSchema: dict
    key: t.List[str]

class CaptureBinding(t.TypedDict):
    collection: CollectionSpec
    resourceConfig: dict
    resourcePath: t.List[str]
    backfill: int
    stateKey: str

class CaptureSpec(t.TypedDict):
    name: str
    connectorType: ConnectorType
    config: dict
    intervalSeconds: int
    bindings: t.List[CaptureBinding]

class RangeSpec(t.TypedDict):
    keyBegin: t.Optional[int]
    keyEnd: int
    rClockBegin: t.Optional[int]
    rClockEnd: int

class UUIDParts(t.TypedDict):
    node: str
    clock: str

class CheckpointSource(t.TypedDict):
    readThrough: str
    producers: t.List[t.Any]

class Checkpoint(t.TypedDict):
    sources: dict[str, CheckpointSource]
    ackIntents: dict[str, str]

class OAuth2(t.TypedDict):
    provider: str
    accessTokenBody: str
    authUrlTemplate: str
    accessTokenHeaders: t.Dict[str, str]
    accessTokenResponseMap: t.Dict[str, str]
    accessTokenUrlTemplate: str

class Spec(t.TypedDict):
    protocol: t.NotRequired[int]
    configSchema: dict
    resourceConfigSchema: dict
    documentationUrl: str
    oauth2: t.NotRequired[OAuth2]
    resourcePathPointers: t.List[str]

class ConnectorState(t.TypedDict):
    updated: dict
    mergePatch: bool
