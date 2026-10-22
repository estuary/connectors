from datetime import date

from pydantic import BaseModel, NonNegativeInt, PositiveInt
from typing import Generic, Any

from ..flow import (
    CaptureSpec,
    CollectionSpec,
    ConnectorState,
    ConnectorType,
    EndpointConfig,
    RangeSpec,
    ResourceConfig,
)


class Spec(BaseModel):
    # TODO(johnny): This shouldn't have a default.
    # This is a temporary accommodation while this fix circulates:
    # https://github.com/estuary/flow/pull/1400
    connectorType: ConnectorType = "IMAGE"


class Discover(BaseModel, Generic[EndpointConfig]):
    connectorType: ConnectorType
    config: EndpointConfig
    createdAt: date | None = None


class ValidateBinding(BaseModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    backfill: NonNegativeInt = 0


class ValidateBase(BaseModel, Generic[ResourceConfig]):
    name: str
    connectorType: ConnectorType
    bindings: list[ValidateBinding[ResourceConfig]] = []


class ValidateLastCaptureEndpointConfig(BaseModel):
    config: dict[str, Any]


class ValidateLastCapture(ValidateBase, Generic[EndpointConfig, ResourceConfig]):
    config: ValidateLastCaptureEndpointConfig
    createdAt: date | None = None


class Validate(ValidateBase, Generic[EndpointConfig, ResourceConfig]):
    config: EndpointConfig
    lastCapture: ValidateLastCapture[EndpointConfig, ResourceConfig] | None = None


class Apply(BaseModel, Generic[EndpointConfig, ResourceConfig]):
    capture: CaptureSpec[EndpointConfig, ResourceConfig]
    version: str


class Open(BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    capture: CaptureSpec[EndpointConfig, ResourceConfig]
    version: str
    range: RangeSpec
    state: ConnectorState


class Acknowledge(BaseModel):
    checkpoints: PositiveInt
