from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import tempfile
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
    BasicAuth,
)

from .models import (
    OracleTable,
    OracleColumn,
    EndpointConfig,
    Table,
    Document,
    Wallet,
    ConnectorState,
)


def create_pool(config: EndpointConfig) -> oracledb.AsyncConnectionPool:
    # Generally a fixed-size pool is recommended, i.e. pool_min=pool_max.  Here
    # the pool contains 4 connections, which will allow 4 concurrent users.
    credentials = {}
    if isinstance(config.credentials, Wallet):
        tmpdir = tempfile.TemporaryDirectory(delete=False)
        with open(f"{tmpdir.name}/tnsnames.ora", 'w') as f:
            f.write(config.credentials.tnsnames)

        with open(f"{tmpdir.name}/ewallet.pem", 'w') as f:
            f.write(config.credentials.ewallet)

        credentials = {
            'config_dir': tmpdir.name,
            'wallet_location': tmpdir.name,
            'wallet_password': config.credentials.wallet_password,
        }
    pool = oracledb.create_pool_async(
        user=config.credentials.username,
        password=config.credentials.password,
        dsn=config.address,
        min=4,
        max=4,
        increment=0,
        cclass="ESTUARY",
        purity=oracledb.ATTR_PURITY_SELF,
        **credentials,
    )

    return pool


async def fetch_tables(
    log: Logger, pool: oracledb.AsyncConnectionPool,
) -> list[OracleTable]:
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            sql_columns = ','.join([f.alias for (k, f) in OracleTable.model_fields.items()])

            query = f"SELECT {sql_columns} FROM all_tables WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA') AND owner NOT IN ('SYS', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS') and table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')"  # noqa
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
    log: Logger, pool: oracledb.AsyncConnectionPool, owners: [str]
) -> list[OracleColumn]:
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            sql_columns = ','.join(["t." + f.alias for (k, f) in OracleColumn.model_fields.items() if f.alias != 'COL_IS_PK' and not f.exclude])

            owners_comma = "'" + "','".join(owners) + "'"
            query = f"""
            SELECT {sql_columns}, NVL2(c.constraint_type, 1, 0) as COL_IS_PK FROM all_tab_columns t
                LEFT JOIN (
                        SELECT c.owner, c.table_name, c.constraint_type, ac.column_name FROM all_constraints c
                            INNER JOIN all_cons_columns ac ON (
                                c.constraint_name = ac.constraint_name
                                AND c.table_name = ac.table_name
                                AND c.owner = ac.owner
                                AND c.constraint_type = 'P'
                            )
                        ) c
                ON (t.owner = c.owner AND t.table_name = c.table_name AND t.column_name = c.column_name)
                WHERE t.owner IN ({owners_comma})
            """

            columns = []
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                row = {k: v for (k, v) in row.items() if v is not None}
                columns.append(OracleColumn(**row))

            return columns

# CHECKPOINT_EVERY = 10000
CHECKPOINT_EVERY = 1


async def fetch_page(
    # Closed over via functools.partial:
    table: Table,
    pool: oracledb.AsyncConnectionPool,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: Tuple[str],
    full_state: ConnectorState,
) -> AsyncGenerator[Document | str, None]:
    is_first_query = False
    if page is None:
        is_first_query = True
        # ROWID is a base64 encoded string, so this string is the minimum value possible
        page = 'AAAAAAAAAAAAAAAAAA'

    query = template_env.get_template("backfill").render(table=table, rowid=page, max_rowid=cutoff[0], is_first_query=is_first_query)

    log.debug("fetch_page", query, page)

    last_rowid = None
    i = 0
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
                    if k in (rowid_column_name,):
                        continue
                    setattr(doc, k, v)

                yield doc

                i = i + 1

                if i % CHECKPOINT_EVERY == 0:
                    yield last_rowid

    if last_rowid is not None and (i % CHECKPOINT_EVERY != 0):
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
    full_state: ConnectorState,
) -> AsyncGenerator[Document | LogCursor, None]:
    query = template_env.get_template("inc").render(table=table, cursor=log_cursor)

    log.debug("fetch_changes", query, log_cursor)

    # We may receive many documents from the same transaction, as such we need to only emit the checkpoint
    # for a transaction at the end, when all documents of that transaction have been emitted
    # so we have to keep track of the first two transaction SCNs we have to check whether we have all the documents
    # of the current SCN before moving on to the next one.
    first_transaction = None
    current_transaction = None
    scn = log_cursor
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            await c.execute(query)
            async for values in c:
                cols = [col[0] for col in c.description]
                row = dict(zip(cols, values))
                row = {k: v for (k, v) in row.items() if v is not None}

                scn = row[scn_column_name]
                op = row[op_column_name]
                rowid = row[rowid_column_name]

                if full_state.backfill is not None and rowid > full_state.backfill.next_page:
                    # we are reading updates for documents which have not yet been backfilled
                    # in order to ensure data consistency, we do not emit these updates until they have been backfilled first
                    # so here we return and wait for another interval
                    log.debug("got event for a row which as not been backfilled yet, looping, rowid:", rowid, "next_page:", full_state.backfill.next_page)
                    return

                doc = Document()
                source = Document.Meta.Source(
                    table=table.table_name,
                    scn=scn
                )
                doc.meta_ = Document.Meta(op=op_mapping[op], source=source)
                for (k, v) in row.items():
                    if k in (scn_column_name, op_column_name, rowid_column_name):
                        continue
                    setattr(doc, k, v)

                yield doc

                # The query to fetch incremental changes uses "VERSIONS BETWEEN X and Y" where
                # BETWEEN is an inclusive operator. So once we have processed all items of one SCN,
                # in order to move on to the next SCN we need to increment the SCN by one. Since the query
                # is always inclusive, this does not risk losing any subsequent events.
                if first_transaction is None:
                    first_transaction = scn
                elif current_transaction is None and scn > first_transaction:
                    current_transaction = scn
                    yield scn + 1
                elif current_transaction is not None and scn > current_transaction:
                    yield current_transaction + 1
                    current_transaction = scn

    if first_transaction is not None and current_transaction is None:
        yield first_transaction + 1
    elif current_transaction is not None and current_transaction > scn:
        yield current_transaction + 1


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
SELECT VERSIONS_STARTSCN, VERSIONS_OPERATION, ROWID, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c | cast }}
{%- endfor %} FROM {{ table.table_name }}
    VERSIONS BETWEEN SCN {{ cursor }} AND MAXVALUE
    WHERE VERSIONS_STARTSCN IS NOT NULL
    ORDER BY VERSIONS_STARTSCN ASC
"""
}))

template_env.filters['cast'] = cast_column
