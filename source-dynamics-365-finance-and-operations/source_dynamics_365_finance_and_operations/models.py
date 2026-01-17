from dataclasses import dataclass
from typing import ClassVar, Literal, Annotated

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from pydantic import BaseModel, Field, BeforeValidator


class AzureSASToken(BaseModel):
    credentials_title: Literal["SAS Token"] = Field(
        default="SAS Token",
        json_schema_extra={"type": "string", "order": 0}
    )
    sas_token: Annotated[str, BeforeValidator(lambda v: v.lstrip('?'))] = Field(
        title="SAS Token",
        description="SAS Token",
        json_schema_extra={"secret": True, "order": 1},
    )

class EndpointConfig(BaseModel):
    account_name: str = Field(
        description="The Azure account name.",
        title="Account name",
        json_schema_extra={"order": 0},
    )
    filesystem: str = Field(
        description="The filesystem containing your Dynamics 365 Finance and Operations data.",
        title="Filesystem",
        json_schema_extra={"order": 1},
    )
    credentials: AzureSASToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 2},
    )

ConnectorState = GenericConnectorState[ResourceState]


class ModelDotJson(BaseModel, extra="allow"):
    class Entity(BaseModel, extra="allow"):
        class Attribute(BaseModel, extra="allow"):
            name: str
            dataType: str

        type_: str = Field(alias="$type")
        name: str
        description: str
        attributes: list[Attribute]

    name: str
    description: str
    version: str
    entities: list[Entity]


class BaseTable(BaseCSVRow, extra="allow"):
    """
    Used for schema generation only. The Pydantic model is needed to generate
    JSON schemas for Resources. Not used to validate actual documents - we
    yield raw dicts to avoid Pydantic's serialization/validation overhead
    that's not necessary in this connector.
    """
    name: ClassVar[str]

    Id: str
    IsDelete: bool | None


def model_from_entity(entity: ModelDotJson.Entity) -> type[BaseTable]:
    return type(entity.name, (BaseTable,), {'name': entity.name})


def tables_from_model_dot_json(model_dot_json: ModelDotJson) -> list[type[BaseTable]]:
    tables: list[type[BaseTable]] = []

    for entity in model_dot_json.entities:
        tables.append(
            model_from_entity(entity)
        )

    return tables


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """
    Lightweight metadata container for runtime CSV processing.
    """
    name: str
    field_names: list[str]
    # Pre-computed set of boolean field names for efficient conversion.
    # Avoids iterating all fields per row - only iterate boolean fields.
    boolean_fields: frozenset[str]
