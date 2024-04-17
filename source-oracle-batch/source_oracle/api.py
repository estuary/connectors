from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import json
import pytz
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator, Dict
import asyncio
import itertools
from copy import deepcopy
import oracledb
import inspect


from .models import (
    OracleTable,
    OracleColumn,
    EndpointConfig,
    Table,
)

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)


def connect(config: EndpointConfig) -> oracledb.Connection:
    return oracledb.connect(user=config.credentials.username,
                            password=config.credentials.password,
                            dsn=config.address)


async def fetch_tables(
    log: Logger, conn: oracledb.Connection,
) -> list[OracleTable]:
    cursor = conn.cursor()

    sql_columns = ','.join([f.alias for (k, f) in OracleTable.model_fields.items()])

    query = f"SELECT {sql_columns} FROM user_tables"

    tables = []
    for values in cursor.execute(query):
        cols = [col[0] for col in cursor.description]
        row = dict(zip(cols, values))
        tables.append(
            OracleTable(**row)
        )

    return tables


async def fetch_columns(
    log: Logger, conn: oracledb.Connection,
) -> list[OracleColumn]:
    cursor = conn.cursor()

    sql_columns = ','.join(["t." + f.alias for (k, f) in OracleColumn.model_fields.items() if f.alias != 'COL_IS_PK'])

    query = f"""
    SELECT {sql_columns}, NVL2(c.constraint_type, 1, 0) as COL_IS_PK FROM user_tab_columns t
        LEFT JOIN (
                SELECT c.table_name, c.constraint_type, ac.column_name FROM all_constraints c
                    INNER JOIN all_cons_columns ac ON (
                        c.constraint_name = ac.constraint_name
                        AND c.table_name = ac.table_name
                        AND c.constraint_type = 'P'
                    )
                ) c
        ON (t.table_name = c.table_name AND t.column_name = c.column_name)
    """
    log.info(query)

    columns = []
    for values in cursor.execute(query):
        cols = [col[0] for col in cursor.description]
        row = dict(zip(cols, values))
        row = {k: v for (k, v) in row.items() if v is not None}
        log.info(row)
        columns.append(OracleColumn(**row))

    return columns


async def fetch_rows(
    # Closed over via functools.partial:
    table: Table,
    conn: oracledb.Connection,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: datetime,
) -> AsyncGenerator[dict, None]:
    yield {}
