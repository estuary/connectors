from pydantic import BaseModel
from typing import Generic, Any

from ..flow import ResourceConfig, ConnectorState, ConnectorStateUpdate


class DiscoveredBinding(BaseModel, Generic[ResourceConfig]):
    recommendedName: str
    resourceConfig: ResourceConfig
    documentSchema: dict
    key: list[str]
    disable: bool = False


class Discovered(BaseModel, Generic[ResourceConfig]):
    bindings: list[DiscoveredBinding[ResourceConfig]]


class ValidatedBinding(BaseModel):
    resourcePath: list[str]


class Validated(BaseModel):
    bindings: list[ValidatedBinding]


class Applied(BaseModel):
    actionDescription: str


class Opened(BaseModel):
    explicitAcknowledgements: bool


class Captured(BaseModel):
    binding: int
    doc: Any


class Checkpoint(BaseModel, Generic[ConnectorState]):
    state: ConnectorStateUpdate[ConnectorState] | None = None
