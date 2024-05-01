from datetime import datetime, timedelta, UTC, date
from decimal import Decimal
from dataclasses import dataclass
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator, BeforeValidator, create_model, StringConstraints, constr, NonNegativeInt, computed_field
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Any
import urllib.parse

import estuary_cdk.capture.common
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BasicAuth,
    BaseDocument,
    ResourceConfig as GenericResourceConfig,
    ResourceState,
)


class Wallet(BaseModel):
    credentials_title: Literal["Wallet"]
    username: str
    password: str = Field(
        title="Password",
        json_schema_extra={"secret": True},
    )
    tnsnames: str = Field(
        title="tnsnames.ora file",
        description="tnsnames.ora file from the wallet zip file",
        json_schema_extra={"multiline": True},
    )
    ewallet: str = Field(
        title="ewallet.pem file",
        description="ewallet.pem file from the wallet zip file",
        json_schema_extra={"secret": True, "multiline": True},
    )
    wallet_password: str = Field(
        title="Password",
        description="Wallet's password",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    address: str = Field(description="Address")
    credentials: BasicAuth | Wallet = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    class Advanced(BaseModel):
        skip_flashback_retention_checks: bool = Field(
            default=False,
            title="Skip Flashback Retention Checks",
            description="Skip Flashback retention checks. Use this cautiously as we cannot guarantee consistency if Flashback retention is not sufficient.",
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


class ResourceConfig(GenericResourceConfig):
    pass


ConnectorState = GenericConnectorState[ResourceState]


class Document(BaseDocument, extra="allow"):
    class Meta(BaseModel):
        op: Literal["c", "u", "d"] = Field(
            description="Operation type (c: Create, u: Update, d: Delete)"
        )

        class Source(BaseModel):
            table: str = Field(
                description="Database table of the event"
            )
            row_id: str | None = Field(
                title='Row ID',
                default=None,
                description="Row ID of the Document, available for backfilled documents",
            )
            scn: int | None = Field(
                title='SCN',
                default=None,
                description="Database System Change Number, available for incremental events"
            )

        source: Source | None = Field(alias="source")

    meta_: Meta = Field(
        default=Meta(op="u", source=None), alias="_meta", description="Document metadata"
    )

    pass


class OracleTable(BaseModel, extra="forbid"):
    """
    Oracle all_tables records. docs: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TABLES.html
    """

    table_name: str = Field(alias="TABLE_NAME")  # "OA_FOOBAR" for system tables, or e.x. "Account"


class OracleColumn(BaseModel, extra="forbid"):
    """
    Oracle all_tab_columns records. docs: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TAB_COLUMNS.html
    """

    class Type(StrEnum):
        CHAR = "CHAR"
        CLOB = "CLOB"
        BLOB = "BLOB"
        NCLOB = "NCLOB"
        BFILE = "BFILE"
        FLOAT = "FLOAT"
        REAL = "REAL"
        DOUBLE = "DOUBLE PRECISION"
        INTEGER = "INTEGER"
        NUMBER = "NUMBER"
        SMALLINT = "SMALLINT"
        TIMESTAMP = "TIMESTAMP"
        VARCHAR = "VARCHAR"
        VARCHAR2 = "VARCHAR2"
        NCHAR = "NCHAR"
        NVARCHAR2 = "NVARCHAR2"
        DATE = "DATE"
        WITH_TIMEZONE = "WITH TIME ZONE"
        WITH_LOCAL_TIMEZONE = "WITH LOCAL TIME ZONE"
        INTERVAL = "INTERVAL"

    table_name: str = Field(alias="TABLE_NAME")  # "CUSTOMRECORD_ABC_PRODUCTION_SERIALS",
    column_name: str = Field(alias="COLUMN_NAME")  # Ex 'recordid'

    # The timestamp type and various other types in Oracle have precision / size as part
    # of their DATA_TYPE, so they are not easily made into an enum, e.g. we have
    # TIMESTAMP(1) to TIMESTAMP(9), and TIMESTAMP(1) WITH TIMEZONE, etc.
    data_type: str = Field(alias="DATA_TYPE")
    data_length: int = Field(alias="DATA_LENGTH", description="The length in bytes of data")
    data_precision: int = Field(default=0, alias="DATA_PRECISION")  # Ex 999
    data_scale: int = Field(
        default=0,
        alias="DATA_SCALE",
        description="Number of digits to the right of the decimal point that are significant.",
    )
    nullable: bool = Field(default=True, alias="NULLABLE")

    @computed_field
    def is_datetime(self) -> bool:
        return self.data_type.startswith(self.Type.TIMESTAMP) or self.data_type == self.Type.DATE

    @computed_field
    def has_timezone(self) -> bool:
        return self.data_type.find(self.Type.WITH_TIMEZONE) > -1 or self.data_type.find(self.Type.WITH_LOCAL_TIMEZONE) > -1

    @computed_field
    def cast_to_string(self) -> bool:
        return self.data_type.startswith(self.Type.INTERVAL)

    is_pk: bool = Field(default=False, alias="COL_IS_PK", description="Calculated by join on all_constraints and all_cons_columns")


@ dataclass
class Table:
    """Table is an extracted representation of a table built from its OracleColumns"""

    table_name: str
    columns: list[OracleColumn]
    primary_key: list[OracleColumn]
    model_fields: dict[str, Any]

    def create_model(self) -> type[Document]:
        return create_model(
            self.table_name,
            __base__=Document,
            __cls_kwargs__={"extra": "forbid"},
            **self.model_fields,
        )


def build_table(
    config: EndpointConfig,
    table_name: str,
    columns: list[OracleColumn],
) -> Table:

    primary_key: list[OracleColumn] = []
    model_fields: dict[str, Any] = {}

    # Order by sequenced primary key then lexicographic column name.
    columns.sort(key=lambda col: (1 if col.is_pk else 1_000, col.column_name))

    for col in columns:

        # First, pick a type for this field.
        field_type: type[int | str | datetime | float | Decimal]
        field_schema_extra: dict | None = None

        if col.data_type == col.Type.NUMBER and col.data_scale == 0:
            field_type = int

        elif col.data_type in (col.Type.DOUBLE, col.Type.NUMBER, col.Type.FLOAT):
            # This field is definitely floating-point (data_scale > 0).
            if col.is_pk:
                # Floats cannot be used as keys, so use {type: string, format: number}.
                field_type = Decimal, Decimal()
                field_schema_extra = {"format": "number"}
            else:
                field_type = float

        elif col.data_type in (col.Type.INTEGER, col.Type.SMALLINT):
            field_type = int
        elif col.data_type in (col.Type.CHAR, col.Type.VARCHAR, col.Type.VARCHAR2, col.Type.CLOB, col.Type.NCHAR, col.Type.NVARCHAR2):
            field_type = str
        elif col.is_datetime and col.has_timezone:
            field_type = datetime
        elif col.is_datetime:
            field_type = str
        elif col.data_type.startswith(col.Type.INTERVAL):
            field_type = str
        else:
            raise NotImplementedError(f"unsupported type {col}")

        if col.is_pk:
            primary_key.append(col)
            description = "Primary Key"
        else:
            description = ""

        field_default = {"default": None} if col.nullable else {}

        # Finally, build the Pydantic model field.
        field_info = Field(
            description=description.strip() or None,
            json_schema_extra=field_schema_extra,
            **field_default,
        )
        field_info.annotation = field_type | None if col.nullable else field_type

        # datetimes must have timezones. Use UTC.
        # TODO: add unit test coverage to confirm this works.
        if field_type is datetime:
            field_info.annotation = Annotated[
                field_info.annotation,
                BeforeValidator(lambda val: val.astimezone(UTC) if isinstance(val, datetime) else val),
            ]

        model_fields[col.column_name] = (field_info.annotation, field_info)

    return Table(
        table_name,
        columns,
        primary_key,
        model_fields,
    )
