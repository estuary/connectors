import collections
import itertools
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Iterable, List, Literal, TypeAlias

import netsuite
from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceConfig as BaseResourceConfig
from estuary_cdk.capture.common import ResourceState
from pydantic import BaseModel, BeforeValidator, Field, StringConstraints, create_model


class EndpointConfig(BaseModel):
    address: str = Field(description="Address")
    user: str = Field(description="Username")
    password: str = Field(description="Password", json_schema_extra={"secret": True})


class ResourceConfig(BaseResourceConfig):
    log_cursor: str = Field(
        title="Incremental Cursor",
        description="A date-time column to use for incremental capture of modifications.",
    )
    page_cursor: str = Field(
        title="Backfill Cursor",
        description="An indexed, non-NULL integer column to use for ordered table backfills. Does not need to be unique, but should have high cardinality.",
    )
    query_limit: int = Field(
        title="Query Limit",
        description="Maximum number of rows to fetch in a query. Typically left as the default.",
        default=100_000,
    )
    query_timeout: timedelta = Field(
        title="Query Timeout",
        description="Timeout for queries. Typically left as the default.",
        default=timedelta(minutes=10),
    )


# We use ResourceState directly, without extending it.
ConnectorState: TypeAlias = GenericConnectorState[ResourceState]


class OracleTable(BaseModel, extra="forbid"):
    """
    Oracle all_tables records. docs: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TABLES.html
    """

    tablespace_name: str = Field(alias="TABLESPACE_NAME")  # "SYSTEM", or "AcmeCo"
    table_owner: str = Field(alias="TABLE_OWNER")  # "SYSTEM", or "AcmeCo - A Role".
    table_name: str = Field(alias="TABLE_NAME")  # "OA_FOOBAR" for system tables, or e.x. "Account"


class OracleColumn(BaseModel, extra="forbid"):
    """
    Oracle all_tab_columns records. docs: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TAB_COLUMNS.html
    """

    class Type(StrEnum):
        BIGINT = "BIGINT"
        CHAR = "CHAR"
        CLOB = "CLOB"
        DOUBLE = "DOUBLE"
        INTEGER = "INTEGER"
        NUMBER = "NUMBER"
        SMALLINT = "SMALLINT"
        TIMESTAMP = "TIMESTAMP"
        VARCHAR = "VARCHAR"
        VARCHAR2 = "VARCHAR2"
        WVARCHAR = "WVARCHAR"

    table_owner: str = Field(alias="TABLE_OWNER")  # "SYSTEM", or "AcmeCo - A Role".
    table_name: str = Field(alias="TABLE_NAME")  # "CUSTOMRECORD_ABC_PRODUCTION_SERIALS",
    column_name: str = Field(alias="COLUMN_NAME")  # Ex 'recordid'

    data_type: Type = Field(alias="DATA_TYPE")
    data_length: int = Field(alias="DATA_LENGTH", description="The length in bytes of data")
    data_precision: int = Field(default=0, alias="DATA_PRECISION")  # Ex 999
    data_scale: int = Field(
        default=0,
        alias="OA_SCALE",
        description="Number of digits to the right of the decimal point that are significant.",
    )
    nullable: bool = Field(default=True, alias="NULLABLE")

    is_pk: bool = Field(default=False, alias="COL_IS_PK", description="Calculated by join on all_constraints and all_cons_columns")


class NextPageCursor(BaseModel, extra="forbid"):
    """Used to fetch the next page cursor to backfill from."""

    next_page_cursor: int


@dataclass
class Table:
    """Table is an extracted representation of a table built from its OAColumns"""

    table_name: str
    columns: list[OracleColumn]
    primary_key: list[OracleColumn]
    model_fields: dict[str, Any]
    possible_log_cursors: list[tuple[float, OracleColumn]]
    possible_page_cursors: list[tuple[float, OracleColumn]]

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
    foreign_keys: list[OracleColumn] = []
    model_fields: dict[str, Any] = {}
    possible_log_cursors: list[tuple[float, OracleColumn]] = []
    possible_page_cursors: list[tuple[float, OracleColumn]] = []

    # Order by sequenced primary key, then foreign keys, then lexicographic column name.
    columns.sort(key=lambda col: (1 if col.is_pk else 1_000, col.column_name))

    for col in columns:

        # First, pick a type for this field.
        field_type: type[int | str | datetime | float | Decimal]
        field_schema_extra: dict | None = None
        field_zero: Any

        if col.type_name == col.Type.NUMBER and col.data_scale == 0:
            # This field could be EITHER an integer or a floating-point.
            # NetSuite.com is sneaky and doesn't give us enough information.
            if col.is_pk:
                # We assume keyed fields are always integers. This technically
                # is not guaranteed, especially for the *_ID column match case,
                # but is a pervasive practice.
                field_type, field_zero = int, 0
            else:
                # Use Decimal to represent lossless integers OR floats.
                # format: number has downsides within materializations
                # (it uses extra storage & cannot represent full double range)
                # so we don't use it if we can avoid it.
                field_type, field_zero = Decimal, Decimal()
                field_schema_extra = {"format": "number"}

        elif col.type_name in (col.Type.DOUBLE, col.Type.NUMBER):
            # This field is definitely floating-point (oa_scale > 0).
            if col.is_pk:
                # Floats cannot be used as keys, so use {type: string, format: number}.
                field_type, field_zero = Decimal, Decimal()
                field_schema_extra = {"format": "number"}
            else:
                field_type, field_zero = float, 0.0

        elif col.type_name in (col.Type.BIGINT, col.Type.INTEGER, col.Type.SMALLINT):
            field_type, field_zero = int, 0
        elif col.type_name in (col.Type.CHAR, col.Type.VARCHAR, col.Type.VARCHAR2, col.Type.CLOB):
            field_type, field_zero = str, ""
        elif col.type_name in (col.Type.TIMESTAMP,):
            field_type, field_zero = datetime, datetime(1, 1, 1, tzinfo=UTC)
        else:
            raise NotImplementedError(f"unsupported type {col}")

        if col.is_pk:
            primary_key.append(col)
            description = "(PK)"

        # Is `col` eligible to act as the page cursor?
        if field_type is not int:
            pass
        elif col.is_pk:
            # Primary key columns are the most-desired page cursor.
            possible_page_cursors.append((-0.99, col))

        # Is `col` eligible to act as the log cursor?
        if field_type is datetime:
            try:
                # Prioritize on index within configured `cursor_fields`.
                possible_log_cursors.append(
                    (
                        config.cursor_fields.index(col.column_name.lower()),
                        col,
                    )
                )
            except ValueError:
                # Still allow fields not in the known list, just rank them below any known cursors
                possible_log_cursors.append(
                    (
                        len(config.cursor_fields) + 1,
                        col,
                    )
                )

        # Finally, build the Pydantic model field.
        field_info = Field(
            default=field_default,
            description=description.strip() or None,
            json_schema_extra=field_schema_extra,
        )
        field_info.annotation = field_type if col.pk_seq else field_type | None

        # datetimes must have timezones. Use UTC.
        # TODO(johnny): add unit test coverage to confirm this works.
        if field_type is datetime:
            field_info.annotation = Annotated[
                field_info.annotation,
                BeforeValidator(lambda val: val.astimezone(UTC) if isinstance(val, datetime) else val),
            ]

        model_fields[col.column_name] = (field_info.annotation, field_info)

    # Sort into priority order.
    possible_log_cursors.sort(key=lambda c: c[0])
    possible_page_cursors.sort(key=lambda c: c[0])

    return Table(
        table_name,
        columns,
        primary_key,
        foreign_keys,
        model_fields,
        possible_log_cursors,
        possible_page_cursors,
    )


def build_tables(columns: list[OAColumn], config: EndpointConfig) -> List[Table]:
    return [
        build_table(
            config.advanced,
            table,
            list(columns),
        )
        for table, columns in itertools.groupby(
            sorted(columns, key=lambda c: c.table_name),
            key=lambda c: c.table_name,
        )
    ]


# NOTE these magic numbers are extracted from the `sqlext.h` header file in the ODBC Netsuite Driver
#      `rg TIMEOUT netsuite_odbc_driver/include/sqlext.h`
class NETSUITE_ODBC_CONSTANTS:
    SQL_LOGIN_TIMEOUT = 103
    # SQL_ATTR_QUERY_TIMEOUT is the same in the driver source
    SQL_QUERY_TIMEOUT = 0
    SQL_ATTR_CONNECTION_TIMEOUT = 113

    SQL_OPT_TRACE = 104
    SQL_OPT_TRACEFILE = 105

    SQL_ATTR_DISCONNECT_BEHAVIOR = 114
    SQL_ACCESS_MODE = 101


def fuzzy_diff(this: str, that: str) -> float:
    """
    Calculates one minus the ratio of the number of matched n-grams
    between `this` and `that`, normalized by the number of n-grams in `this`.

    More similar strings have values closer to zero,
    and dissimilar strings have values close to one.
    """

    this_grams = collections.Counter(_n_grams(this, 3))
    that_grams = collections.Counter(_n_grams(that, 3))
    overlap = sum(min(this_grams[k], that_grams[k]) for k in this_grams & that_grams)
    return 1 - (overlap / this_grams.total())


def _n_grams(text, n) -> Iterable[str]:
    # Extend with start ^ and stop $ characters.
    text = "^" * (n - 1) + text + "$"
    return (text[i: i + n] for i in range(len(text) - n + 1))
