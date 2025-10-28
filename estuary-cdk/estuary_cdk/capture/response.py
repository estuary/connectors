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
    # Pydantic V2 supports serialization_alias in Field, while Pydantic V1 requires an inner Config class to serialize by the alias.
    schemaJson: dict = Field(serialization_alias="schema_json", alias="schema_json")

    if _PYDANTIC_VERSION == "v2":
        model_config = {"validate_by_name": True, "validate_by_alias": True}
    else:
        # Pydantic V1 compatibility is needed here since imported connectors using the `CaptureShim` can
        # use this model to emit sourced schemas. In Pydantic V1, the configuration is done via an inner Config class and
        # requires setting allow_population_by_field_name so that the alias can be used during serialization.
        class Config:
            allow_population_by_field_name = True



class Checkpoint(BaseModel, Generic[ConnectorState]):
    state: ConnectorStateUpdate[ConnectorState] | None = None
