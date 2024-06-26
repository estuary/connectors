from typing import Any, Generic

from pydantic import BaseModel, NonNegativeInt

from ..flow import (
    CollectionSpec,
    ConnectorState,
    ConnectorType,
    EndpointConfig,
    MaterializationSpec,
    RangeSpec,
    ResourceConfig,
)


class Spec(BaseModel):
    connectorType: ConnectorType


class ValidateBinding(BaseModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    fieldConfigJsonMap: dict[str, Any] | None = None
    backfill: NonNegativeInt | None = 0


class Validate(BaseModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    config: EndpointConfig
    bindings: list[ValidateBinding[ResourceConfig]] = []
    lastMaterialization: MaterializationSpec[EndpointConfig, ResourceConfig] | None = (
        None
    )
    lastVersion: str | None = None


class Apply(BaseModel, Generic[EndpointConfig, ResourceConfig]):
    materialization: MaterializationSpec[EndpointConfig, ResourceConfig]
    version: str
    lastMaterialization: MaterializationSpec[EndpointConfig, ResourceConfig] | None = (
        None
    )
    lastVersion: str | None = None


class Open(BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    materialization: MaterializationSpec[EndpointConfig, ResourceConfig]
    version: str
    range: RangeSpec
    state: ConnectorState


class Load(BaseModel):
    keyPacked: str
    binding: int | None = 0
    keyJson: tuple[Any, ...] | None = None


class Flush(BaseModel):
    pass


class Store(BaseModel):
    binding: int | None = 0
    keyPacked: str
    valuesPacked: str
    doc: dict[Any, Any] | None = None
    exists: bool | None = False
    delete: bool | None = False
    valuesJson: tuple[Any, ...] | None = None
    keyJson: tuple[Any, ...] | None = None


class StartCommit(BaseModel):
    runtimeCheckpoint: dict[Any, Any]


class Acknowledge(BaseModel):
    pass
