from dataclasses import dataclass
from typing import ClassVar, Literal, Annotated

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from pydantic import BaseModel, Field, BeforeValidator


# Checkpoint every N rows to keep buffer sizes small
CHECKPOINT_EVERY = 10_000

# Cursor format constants
CURSOR_SEPARATOR = "|"
ROW_OFFSET_WIDTH = 10  # Zero-pad to 10 digits (supports up to 9,999,999,999 rows)


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
    Used for schema generation and table metadata. Not used to validate actual
    documents - we yield raw dicts to avoid Pydantic's serialization/validation
    overhead that's not necessary in this connector.
    """
    name: ClassVar[str]
    field_names: ClassVar[list[str]]
    field_types: ClassVar[dict[str, str]]
    # Pre-computed set of boolean field names for efficient conversion.
    # Avoids iterating all fields per row - only iterate boolean fields.
    boolean_fields: ClassVar[frozenset[str]]

    Id: str
    IsDelete: bool | None


def model_from_entity(entity: ModelDotJson.Entity) -> type[BaseTable]:
    field_names = [attr.name for attr in entity.attributes]
    field_types = {attr.name: attr.dataType for attr in entity.attributes}
    boolean_fields = frozenset(
        attr.name for attr in entity.attributes if attr.dataType == "boolean"
    )

    attrs = {
        'name': entity.name,
        'field_names': field_names,
        'field_types': field_types,
        'boolean_fields': boolean_fields,
    }

    return type(entity.name, (BaseTable,), attrs)


def tables_from_model_dot_json(model_dot_json: ModelDotJson) -> list[type[BaseTable]]:
    tables: list[type[BaseTable]] = []

    for entity in model_dot_json.entities:
        tables.append(
            model_from_entity(entity)
        )

    return tables


@dataclass
class ParsedCursor:
    """Structured representation of the composite cursor string."""
    folder_timestamp: str  # ISO 8601 folder timestamp, e.g., "2024-07-15T00:00:00.000Z"
    csv_last_modified: str  # ISO 8601 CSV last-modified, empty if folder complete
    row_offset: int  # Number of rows already emitted from current CSV

    @property
    def is_folder_complete(self) -> bool:
        """True if the folder has been fully processed."""
        return self.csv_last_modified == ""

    @property
    def is_initial(self) -> bool:
        """True if this is the initial cursor (no data processed yet)."""
        return self.folder_timestamp == ""


def parse_cursor(cursor: tuple[str]) -> ParsedCursor:
    """
    Parse a composite cursor string into structured components.

    Args:
        cursor: Tuple containing single composite string

    Returns:
        ParsedCursor with extracted components
    """
    cursor_str = cursor[0]

    if cursor_str == "":
        return ParsedCursor(folder_timestamp="", csv_last_modified="", row_offset=0)

    parts = cursor_str.split(CURSOR_SEPARATOR)
    if len(parts) != 3:
        raise ValueError(f"Invalid cursor format: {cursor_str}")

    folder_timestamp, csv_last_modified, row_offset_str = parts
    row_offset = int(row_offset_str) if row_offset_str else 0

    return ParsedCursor(
        folder_timestamp=folder_timestamp,
        csv_last_modified=csv_last_modified,
        row_offset=row_offset,
    )


def format_cursor(folder_timestamp: str, csv_last_modified: str = "", row_offset: int = 0) -> tuple[str]:
    """
    Create a composite cursor tuple from components.

    Args:
        folder_timestamp: ISO 8601 folder timestamp
        csv_last_modified: ISO 8601 CSV last-modified (empty for folder complete)
        row_offset: Number of rows emitted from current CSV

    Returns:
        Single-element tuple containing composite cursor string
    """
    # Empty folder_timestamp means initial cursor - use empty string
    # which sorts before any real timestamp
    if not folder_timestamp:
        return ("",)

    row_offset_str = str(row_offset).zfill(ROW_OFFSET_WIDTH) if csv_last_modified else ""
    return (f"{folder_timestamp}{CURSOR_SEPARATOR}{csv_last_modified}{CURSOR_SEPARATOR}{row_offset_str}",)
