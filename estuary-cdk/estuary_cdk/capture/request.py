from pydantic import BaseModel, NonNegativeInt, PositiveInt
from typing import Generic, Any

from ..flow import (
    UNLINKED,
    CaptureSpec,
    CollectionSpec,
    ConnectorState,
    ConnectorType,
    EndpointConfig,
    Linked,
    RangeSpec,
    ResourceConfig,
    link_bindings,
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
    collection: CollectionSpec = UNLINKED
    collectionIndex: NonNegativeInt = 0
    resourceConfig: ResourceConfig
    backfill: NonNegativeInt = 0


class ValidateBase(Linked, GenericModel, Generic[ResourceConfig]):
    name: str
    connectorType: ConnectorType
    bindings: list[ValidateBinding[ResourceConfig]] = []
    linkedCollections: list[CollectionSpec] = []

    def _link(self) -> None:
        link_bindings(self.bindings, self.linkedCollections)


class ValidateLastCaptureEndpointConfig(BaseModel):
    config: dict[str, Any]


class ValidateLastCapture(ValidateBase[ResourceConfig], Generic[EndpointConfig, ResourceConfig]):
    config: ValidateLastCaptureEndpointConfig


class Validate(ValidateBase[ResourceConfig], Generic[EndpointConfig, ResourceConfig]):
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
    checkpoints: PositiveInt
