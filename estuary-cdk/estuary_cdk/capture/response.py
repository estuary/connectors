from pydantic import BaseModel, Field
from typing import Generic, Any

from ..flow import ResourceConfig, ConnectorState, ConnectorStateUpdate
from ..pydantic_polyfill import GenericModel, VERSION as _PYDANTIC_VERSION


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


class SourcedSchema(BaseModel):
    binding: int
    # Pydantic's BaseModel already has a schema_json field, and it complains if we overwrite it.
    # We use a serialization alias to avoid Pydantic's complaints and serialize the field name correctly.
    schemaJson: dict = Field(serialization_alias="schema_json")

    if _PYDANTIC_VERSION == "v2":
        model_config = {"validate_by_name": True, "validate_by_alias": True}
    else:
        class Config:
            allow_population_by_field_name = True



class Checkpoint(BaseModel, Generic[ConnectorState]):
    state: ConnectorStateUpdate[ConnectorState] | None = None
