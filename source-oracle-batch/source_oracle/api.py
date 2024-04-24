from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import json
import pytz
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator, Dict, Tuple
import asyncio
import itertools
from copy import deepcopy
import oracledb
import inspect
from jinja2 import Template

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)

from .models import (
    OracleTable,
    OracleColumn,
    EndpointConfig,
    Table,
    Document,
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

    columns = []
    for values in cursor.execute(query):
        cols = [col[0] for col in cursor.description]
        row = dict(zip(cols, values))
        row = {k: v for (k, v) in row.items() if v is not None}
        columns.append(OracleColumn(**row))

    return columns


async def fetch_page(
    # Closed over via functools.partial:
    table: Table,
    conn: oracledb.Connection,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: Tuple[str],
) -> AsyncGenerator[Document | str, None]:
    is_first_query = False
    if page is None:
        is_first_query = True
        with conn.cursor() as c:
            c.execute(f"select min(ROWID) from {table.table_name}")
            page = c.fetchone()[0]

    query = backfill_query_template.render(table=table, rowid=page, max_rowid=cutoff[0], is_first_query=is_first_query)

    log.info(query, page)

    last_rowid = None
    with conn.cursor() as c:
        for values in c.execute(query):
            cols = [col[0] for col in c.description]
            row = dict(zip(cols, values))
            row = {k: v for (k, v) in row.items() if v is not None}
            last_rowid = row[rowid_column_name]

            doc = Document()
            doc.meta_ = Document.Meta(op='c')
            for (k, v) in row.items():
                if k == rowid_column_name:
                    continue
                setattr(doc, k, v)

            yield doc

    if last_rowid is not None:
        yield last_rowid


op_mapping = {
    'I': 'c',
    'D': 'd',
    'U': 'u',
}


async def fetch_changes(
    # Closed over via functools.partial:
    table: Table,
    conn: oracledb.Connection,
    # Remainder is common.FetchPageFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Document | LogCursor, None]:
    query = inc_query_template.render(table=table, cursor=log_cursor)

    log.info(query, log_cursor)

    last_scn = log_cursor
    with conn.cursor() as c:
        for values in c.execute(query):
            cols = [col[0] for col in c.description]
            row = dict(zip(cols, values))
            row = {k: v for (k, v) in row.items() if v is not None}

            if scn_column_name not in row or op_column_name not in row:
                continue

            if row[scn_column_name] > last_scn:
                last_scn = row[scn_column_name] + 1
            elif last_scn == log_cursor:
                last_scn = last_scn + 1
            log.info("setting last_scn to", last_scn)

            op = row[op_column_name]

            doc = Document()
            doc.meta_ = Document.Meta(op=op_mapping[op])
            for (k, v) in row.items():
                if k in (scn_column_name, op_column_name):
                    continue
                setattr(doc, k, v)

            yield doc

    if last_scn != log_cursor:
        yield last_scn

# an all-uppercase ROWID is a reserved keyword that cannot be used
# as a column identifier, however other casings of the same word
# can be used as a column name.
rowid_column_name = "ROWID"
backfill_query_template = Template("""
SELECT ROWID, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c.column_name }}
{%- endfor %} FROM {{ table.table_name }}
    WHERE ROWID >{% if is_first_query %}={% endif %} '{{ rowid }}'
      AND ROWID <= '{{ max_rowid }}'
    ORDER BY ROWID ASC
""")

scn_column_name = "VERSIONS_STARTSCN"
op_column_name = "VERSIONS_OPERATION"
inc_query_template = Template("""
SELECT VERSIONS_STARTSCN, VERSIONS_OPERATION, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c.column_name }}
{%- endfor %} FROM {{ table.table_name }}
    VERSIONS BETWEEN SCN {{ cursor }} AND MAXVALUE
    ORDER BY VERSIONS_STARTSCN ASC
""")