from datetime import datetime, timedelta, UTC, date
from decimal import Decimal
from dataclasses import dataclass
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator, BeforeValidator, create_model, StringConstraints, constr, NonNegativeInt
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


class EndpointConfig(BaseModel):
    address: str = Field(description="Address")
    credentials: BasicAuth = Field(
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
    pass


class OracleTable(BaseModel, extra="forbid"):
    """
    Oracle all_tables records. docs: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TABLES.html
    """

    tablespace_name: str = Field(alias="TABLESPACE_NAME")  # "SYSTEM", or "AcmeCo"
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

    _is_ts: bool
    _has_tz: bool

    is_pk: bool = Field(default=False, alias="COL_IS_PK", description="Calculated by join on all_constraints and all_cons_columns")


@ dataclass
class Table:
    """Table is an extracted representation of a table built from its OracleColumns"""

    table_name: str
    columns: list[OracleColumn]
    primary_key: list[OracleColumn]
    model_fields: dict[str, Any]

    def create_model(self) -> type[BaseDocument]:
        return create_model(
            self.table_name,
            __base__=BaseDocument,
            __cls_kwargs__={"extra": "forbid"},
            **self.model_fields,
        )


def build_table(
    config: EndpointConfig.Advanced,
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
        field_zero: Any
        import sys

        if col.data_type == col.Type.NUMBER and col.data_scale == 0:
            field_type, field_zero = int, 0

        elif col.data_type in (col.Type.DOUBLE, col.Type.NUMBER, col.Type.FLOAT):
            # This field is definitely floating-point (data_scale > 0).
            if col.is_pk:
                # Floats cannot be used as keys, so use {type: string, format: number}.
                field_type, field_zero = Decimal, Decimal()
                field_schema_extra = {"format": "number"}
            else:
                field_type, field_zero = float, 0.0

        elif col.data_type in (col.Type.INTEGER, col.Type.SMALLINT):
            field_type, field_zero = int, 0
        elif col.data_type in (col.Type.CHAR, col.Type.VARCHAR, col.Type.VARCHAR2, col.Type.CLOB, col.Type.NCHAR, col.Type.NVARCHAR2):
            field_type, field_zero = str, ""
        elif col.data_type.startswith(col.Type.TIMESTAMP) and col.data_type.find(col.Type.WITH_TIMEZONE) > -1:
            col._is_ts = True
            col._has_tz = True
            field_type, field_zero = datetime, datetime(1, 1, 1, tzinfo=UTC)
        elif col.data_type.startswith(col.Type.TIMESTAMP) and col.data_type.find(col.Type.WITH_LOCAL_TIMEZONE) > -1:
            col._is_ts = True
            col._has_tz = True
            field_type, field_zero = datetime, datetime(1, 1, 1, tzinfo=UTC)
        elif col.data_type.startswith(col.Type.TIMESTAMP):
            col._is_ts = True
            col._has_tz = False
            field_type, field_zero = str, ""
        elif col.data_type.startswith(col.Type.INTERVAL):
            field_type, field_zero = str, ""
        elif col.data_type in (col.Type.DATE,):
            col._is_ts = True
            col._has_tz = False
            field_type, field_zero = str, ""
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
        # TODO(johnny): add unit test coverage to confirm this works.
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
