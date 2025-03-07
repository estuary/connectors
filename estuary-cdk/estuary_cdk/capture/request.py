from pydantic import BaseModel, NonNegativeInt
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
from ..pydantic_polyfill import GenericModel


class Spec(BaseModel):
    # TODO(johnny): This shouldn't have a default.
    # This is a temporary accommodation while this fix circulates:
    # https://github.com/estuary/flow/pull/1400
    connectorType: ConnectorType = "IMAGE"


class Discover(GenericModel, Generic[EndpointConfig]):
    connectorType: ConnectorType
    config: EndpointConfig


class ValidateBinding(GenericModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    backfill: NonNegativeInt = 0


class ValidateBase(GenericModel, Generic[ResourceConfig]):
    name: str
    connectorType: ConnectorType
    bindings: list[ValidateBinding[ResourceConfig]] = []


class ValidateLastCaptureEndpointConfig(BaseModel):
    config: dict[str, Any]


class ValidateLastCapture(ValidateBase, Generic[EndpointConfig, ResourceConfig]):
    config: ValidateLastCaptureEndpointConfig


class Validate(ValidateBase, Generic[EndpointConfig, ResourceConfig]):
    config: EndpointConfig
    lastCapture: ValidateLastCapture[EndpointConfig, ResourceConfig] | None = None


class Apply(GenericModel, Generic[EndpointConfig, ResourceConfig]):
    capture: CaptureSpec[EndpointConfig, ResourceConfig]
    version: str


class Open(GenericModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    capture: CaptureSpec[EndpointConfig, ResourceConfig]
    version: str
    range: RangeSpec
    state: ConnectorState


class Acknowledge(BaseModel):
    checkpoints: NonNegativeInt
