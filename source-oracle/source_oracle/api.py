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
from jinja2 import Template, Environment, DictLoader

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


async def init_session(connection, _requested_tag):
    with connection.cursor() as cursor:
        await cursor.execute(
            """
            alter session set
                time_zone = 'UTC'
            """
        )


def create_pool(config: EndpointConfig) -> oracledb.AsyncConnectionPool:
    # Generally a fixed-size pool is recommended, i.e. pool_min=pool_max.  Here
    # the pool contains 4 connections, which will allow 4 concurrent users.
    pool = oracledb.create_pool_async(
        user=config.credentials.username,
        password=config.credentials.password,
        dsn=config.address,
        min=4,
        max=4,
        increment=0,
        session_callback=init_session,
        cclass="ESTUARY",
        purity=oracledb.ATTR_PURITY_SELF,
    )

    return pool


async def fetch_tables(
    log: Logger, pool: oracledb.AsyncConnectionPool,
) -> list[OracleTable]:
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            sql_columns = ','.join([f.alias for (k, f) in OracleTable.model_fields.items()])

            query = f"SELECT {sql_columns} FROM user_tables"

            tables = []
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                tables.append(
                    OracleTable(**row)
                )

            return tables


async def fetch_columns(
    log: Logger, pool: oracledb.AsyncConnectionPool,
) -> list[OracleColumn]:
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            sql_columns = ','.join(["t." + f.alias for (k, f) in OracleColumn.model_fields.items() if f.alias != 'COL_IS_PK' and not f.exclude])

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
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                row = {k: v for (k, v) in row.items() if v is not None}
                columns.append(OracleColumn(**row))

            return columns


async def fetch_page(
    # Closed over via functools.partial:
    table: Table,
    pool: oracledb.AsyncConnectionPool,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: Tuple[str],
) -> AsyncGenerator[Document | str, None]:
    is_first_query = False
    if page is None:
        is_first_query = True
        async with pool.acquire() as conn:
            with conn.cursor() as c:
                await c.execute(f"select min(ROWID) from {table.table_name}")
                page = (await c.fetchone())[0]

    query = template_env.get_template("backfill").render(table=table, rowid=page, max_rowid=cutoff[0], is_first_query=is_first_query)

    log.debug("fetch_page", query, page)

    last_rowid = None
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                row = {k: v for (k, v) in row.items() if v is not None}
                last_rowid = row[rowid_column_name]

                doc = Document()
                source = Document.Meta.Source(
                    table=table.table_name,
                    row_id=row[rowid_column_name]
                )
                doc.meta_ = Document.Meta(op='c', source=source)
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
    pool: oracledb.AsyncConnectionPool,
    # Remainder is common.FetchPageFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Document | LogCursor, None]:
    query = template_env.get_template("inc").render(table=table, cursor=log_cursor)

    log.debug("fetch_changes", query, log_cursor)

    last_scn = log_cursor
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                row = {k: v for (k, v) in row.items() if v is not None}
                log.debug("change", row)

                if scn_column_name not in row or op_column_name not in row:
                    continue

                if row[scn_column_name] > last_scn:
                    last_scn = row[scn_column_name] + 1
                elif last_scn == log_cursor:
                    last_scn = last_scn + 1
                log.debug("setting last_scn to", last_scn)

                op = row[op_column_name]

                doc = Document()
                source = Document.Meta.Source(
                    table=table.table_name,
                    scn=row[scn_column_name]
                )
                doc.meta_ = Document.Meta(op=op_mapping[op], source=source)
                for (k, v) in row.items():
                    if k in (scn_column_name, op_column_name):
                        continue
                    setattr(doc, k, v)

                yield doc

    if last_scn != log_cursor:
        yield last_scn


# datetime and some other data types must be cast to string
# this helper function takes care of formatting datetimes as RFC3339 strings
def cast_column(c: OracleColumn) -> str:
    if not c.is_datetime and not c.cast_to_string:
        return c.column_name

    if c.cast_to_string and not c.is_datetime:
        return f"TO_CHAR({c.column_name}) AS {c.column_name}"

    out = "TO_CHAR(" + c.column_name
    fmt = ""
    if c.has_timezone:
        fmt = """'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"'"""
    elif c.data_scale > 0:
        fmt = """'YYYY-MM-DD"T"HH24:MI:SS.FF'"""
    else:
        fmt = """'YYYY-MM-DD"T"HH24:MI:SS'"""

    if c.has_timezone:
        out = out + " AT TIME ZONE 'UTC'"

    out = out + f", {fmt}) AS {c.column_name}"

    return out


rowid_column_name = "ROWID"
scn_column_name = "VERSIONS_STARTSCN"
op_column_name = "VERSIONS_OPERATION"

template_env = Environment(loader=DictLoader({
    # an all-uppercase ROWID is a reserved keyword that cannot be used
    # as a column identifier, however other casings of the same word
    # can be used as a column name.
    'backfill': """
SELECT ROWID, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c | cast }}
{%- endfor %} FROM {{ table.table_name }}
    WHERE ROWID >{% if is_first_query %}={% endif %} '{{ rowid }}'
      AND ROWID <= '{{ max_rowid }}'
    ORDER BY ROWID ASC
""",
    'inc': """
SELECT VERSIONS_STARTSCN, VERSIONS_OPERATION, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c | cast }}
{%- endfor %} FROM {{ table.table_name }}
    VERSIONS BETWEEN SCN {{ cursor }} AND MAXVALUE
    ORDER BY VERSIONS_STARTSCN ASC
"""
}))

template_env.filters['cast'] = cast_column
