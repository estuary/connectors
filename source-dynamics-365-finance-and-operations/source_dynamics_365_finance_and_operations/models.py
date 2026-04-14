from dataclasses import dataclass
from enum import StrEnum
from logging import Logger
from typing import Any, ClassVar, Literal, Annotated

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


class AttributeDataType(StrEnum):
    STRING = "string"
    INT64 = "int64"
    DECIMAL = "decimal"
    DATE_TIME = "dateTime"
    DATE_TIME_OFFSET = "dateTimeOffset"
    GUID = "guid"
    BOOLEAN = "boolean"


class ModelDotJson(BaseModel, extra="allow"):
    class Entity(BaseModel, extra="allow"):
        class Attribute(BaseModel, extra="allow"):
            name: str
            dataType: AttributeDataType | str
            # maxLength is only set to a non -1 value for string dataTypes. For
            # other dataTypes, it's either absent or set to -1.
            maxLength: int = -1

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
    attributes: ClassVar[list[ModelDotJson.Entity.Attribute]]

    Id: str
    IsDelete: bool | None
    # Monotonically increasing commit sequence number from the source system.
    # Authoritative ordering primitive for changes within a folder.
    versionnumber: str
    # Timestamp at which the change was committed to the data lake. Used
    # only as a defensive tiebreaker when two rows for the same Id share
    # a versionnumber.
    SinkModifiedOn: str

    @classmethod
    def sourced_schema(cls, log: Logger) -> dict[str, Any]:
        """Build a sourced schema from entity attribute metadata.

        The sourced schema tells the Flow runtime what each field's type is,
        causing materialization columns to be created even before data is seen.
        Length constraints start tight and schema inference widens them as
        actual data arrives.

        All values except booleans arrive as CSV strings, so most types are
        schematized as {"type": "string"} with a format hint.
        """
        schema: dict[str, Any] = {
            "additionalProperties": False,
            "type": "object",
            "properties": {},
            "required": [],
        }

        for attr in cls.attributes:
            match attr.dataType:
                case AttributeDataType.STRING:
                    # model.json provides an authoritative maxLength for every
                    # string field. Set minLength=maxLength so inference widens
                    # minLength down as shorter values are seen. Fall back to 1
                    # if maxLength is absent.
                    if attr.maxLength > 0:
                        field_schema: dict[str, Any] = {
                            "type": "string",
                            "minLength": attr.maxLength,
                            "maxLength": attr.maxLength
                        }
                    else:
                        field_schema = {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 1
                        }
                case AttributeDataType.INT64 | AttributeDataType.DECIMAL:
                    # Actual string lengths vary widely across fields, so start
                    # at 1 and let schema inference discover real bounds.
                    fmt = "integer" if attr.dataType == AttributeDataType.INT64 else "number"
                    field_schema = {
                        "type": "string",
                        "format": fmt,
                        "minLength": 1,
                        "maxLength": 1
                    }
                case AttributeDataType.DATE_TIME:
                    # SinkCreatedOn/SinkModifiedOn are Azure Synapse Link
                    # metadata fields that use a non-ISO format like
                    # "4/14/2026 8:21:50 PM". Lengths range from 19
                    # ("1/1/2026 1:00:00 PM") to 22 ("12/31/2026 12:59:59 PM").
                    # All other dateTime fields are ISO 8601 with 7 fractional
                    # digits: "1900-01-01T00:00:00.0000000Z" (28 chars).
                    if attr.name in ("SinkCreatedOn", "SinkModifiedOn"):
                        field_schema = {
                            "type": "string",
                            "minLength": 19,
                            "maxLength": 22
                        }
                    else:
                        field_schema = {
                            "type": "string",
                            "format": "date-time",
                            "minLength": 28, "maxLength": 28
                        }
                case AttributeDataType.DATE_TIME_OFFSET:
                    # ISO 8601 with offset: "1900-01-01T00:00:00.0000000+00:00" (33 chars).
                    field_schema = {
                        "type": "string",
                        "format": "date-time",
                        "minLength": 33, "maxLength": 33
                    }
                case AttributeDataType.GUID:
                    # Standard UUID with hyphens: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (36 chars).
                    field_schema = {
                        "type": "string",
                        "format": "uuid",
                        "minLength": 36,
                        "maxLength": 36
                    }
                case AttributeDataType.BOOLEAN:
                    field_schema = {"type": "boolean"}
                case _:
                    log.warning(f"Omitting attribute '{attr.name}' from sourced schema for entity '{cls.name}' due to unknown dataType '{attr.dataType}'.")
                    continue

            schema["properties"][attr.name] = field_schema
            # Mark every field as required so the sourced schema starts as
            # tight as possible. Schema inference will widen to permit nulls
            # as actual data arrives.
            schema["required"].append(attr.name)

        # Sort to keep a stable ordering.
        schema["required"].sort()

        return schema


def model_from_entity(entity: ModelDotJson.Entity) -> type[BaseTable]:
    return type(entity.name, (BaseTable,), {'name': entity.name, 'attributes': entity.attributes})


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
