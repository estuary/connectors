from enum import StrEnum
from typing import ClassVar, Literal, Annotated

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from pydantic import BaseModel, Field, model_validator, BeforeValidator


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
    name: ClassVar[str]
    field_names: ClassVar[list[str]]
    field_types: ClassVar[dict[str, str]]

    Id: str
    IsDelete: bool | None

    @model_validator(mode='before')
    @classmethod
    def convert_boolean_fields(cls, values: dict) -> dict:
        if hasattr(cls, 'field_types'):
            for field_name, value in values.items():
                if (cls.field_types.get(field_name) == "boolean") and value is not None:
                    assert isinstance(value, str)
                    values[field_name] = value.lower() == 'true'
        return values

    @model_validator(mode='after')
    def set_meta_op(self) -> 'BaseTable':
        if self.IsDelete:
            self.meta_ = BaseTable.Meta(op='d')
        else:
            self.meta_ = BaseTable.Meta(op='u')
        return self


def model_from_entity(entity: ModelDotJson.Entity) -> type[BaseTable]:
    field_names = [attr.name for attr in entity.attributes]
    field_types = {attr.name: attr.dataType for attr in entity.attributes}

    attrs = {
        'name': entity.name,
        'field_names': field_names,
        'field_types': field_types,
    }

    return type(entity.name, (BaseTable,), attrs)


def tables_from_model_dot_json(model_dot_json: ModelDotJson) -> list[type[BaseTable]]:
    tables: list[type[BaseTable]] = []

    for entity in model_dot_json.entities:
        tables.append(
            model_from_entity(entity)
        )

    return tables
