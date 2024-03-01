from pydantic import BaseModel, NonNegativeInt
from typing import Generic

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


class Validate(GenericModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    bindings: list[ValidateBinding[ResourceConfig]] = []


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
