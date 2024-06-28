from datetime import datetime, UTC, timedelta
import functools
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import time
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
    Task,
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


def create_pool(log: Logger, config: EndpointConfig) -> oracledb.AsyncConnectionPool:
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
        max=12,
        increment=1,
        cclass="ESTUARY",
        purity=oracledb.ATTR_PURITY_SELF,
        **credentials,
    )

    return pool


DISCOVERY_PAGE_SIZE = 1000


async def fetch_tables(
    log: Logger, pool: oracledb.AsyncConnectionPool,
) -> list[OracleTable]:
    async with pool.acquire() as conn:
        with conn.cursor() as c:
            sql_columns = ','.join([f.alias for (k, f) in OracleTable.model_fields.items()])

            query = f"SELECT DISTINCT(NVL(IOT_NAME, TABLE_NAME)) AS table_name, owner FROM all_tables WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA') AND owner NOT IN ('SYS', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'RDSADMIN') and table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')"  # noqa
            tables = []
            c.arraysize = DISCOVERY_PAGE_SIZE
            c.prefetchrows = DISCOVERY_PAGE_SIZE + 1
            await c.execute(query)
            cols = [col[0] for col in c.description]
            c.rowfactory = lambda *args: dict(zip(cols, args))
            async for row in c:
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
            c.arraysize = DISCOVERY_PAGE_SIZE
            c.prefetchrows = DISCOVERY_PAGE_SIZE + 1
            await c.execute(query)
            cols = [col[0] for col in c.description]
            c.rowfactory = lambda *args: dict(zip(cols, args))
            async for row in c:
                row = {k: v for (k, v) in row.items() if v is not None}
                columns.append(OracleColumn(**row))

            return columns

# Backfills operate on ROWID, which is a unique identifier for each row based on its
# physical location. This identifier is consistent in a single transaction but can change between
# transactions. We use the ROWID to order all rows of a table and capture all rows up until the last ROWID
# we have identified for this table at the time of starting the backfill. It is possible that some of the rows from
# the middle of the range move to a place after the last ROWID, or to a smaller value than the ROWIDs we have processed
# thus being missed by the backfill process, however since the incremental changes are captured based on transaction IDs, and not ROWIDs
# the incremental changes task will capture those updated rows, so we will eventually have all the documents

# In order to avoid races between backfill and incremental tasks, we implement a ping-pong between the two using a lock,
# meaning that they don't really run concurrently ever, but rather they run one after another. We start by running
# a backfill task, backfilling CHECKPOINT_EVERY documents, and then passing the ball to the
# incremental task, which will fetch updates since the last seen transaction ID up until the latest transaction
# of the database. The ball is then passed back to backfill. This ensures
# that we do not capture incremental updates to documents and emit them while a backfill task may be reading and emitting an older
# version of the same row afterwards.


CHECKPOINT_EVERY = 1000

BACKFILL_MAX_TIME = timedelta(hours=1)


# the first two arguments are provided by functools.partial, the rest are row values
# the first value is taken as rowid
def backfill_rowfactory(table_name, cols, rowid, *args):
    obj = dict(zip(cols[1:], args))
    obj['_meta'] = {
        'op': 'c',
        'source': {
            'table': table_name,
            'row_id': rowid,
        }
    }

    return obj


async def fetch_page(
    # Closed over via functools.partial:
    table: Table,
    pool: oracledb.AsyncConnectionPool,
    task: Task,
    backfill_chunk_size: int,
    lock: asyncio.Lock,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: str | None,
    cutoff: Tuple[str],
) -> AsyncGenerator[Document | str, None]:
    async with lock:
        start_time = datetime.now()
        end_time = start_time + BACKFILL_MAX_TIME

        if page is None:
            # ROWID is a base64 encoded string, so this string is the minimum value possible
            page = 'AAAAAAAAAAAAAAAAAA'

        query = template_env.get_template("backfill").render(table=table, rowid=page, max_rowid=cutoff[0])

        log.debug("fetch_page", query, page)

        last_rowid = None
        i = 0
        async with pool.acquire() as conn:
            with conn.cursor() as c:
                while datetime.now() < end_time:
                    c.arraysize = backfill_chunk_size
                    c.prefetchrows = backfill_chunk_size + 1
                    await c.execute(query, rownum_end=backfill_chunk_size)
                    cols = [col[0] for col in c.description]
                    c.rowfactory = functools.partial(backfill_rowfactory, table.table_name, cols)

                    async for row in c:
                        last_rowid = row['_meta']['source']['row_id']
                        yield row

                        i = i + 1
                        if i % CHECKPOINT_EVERY == 0:
                            yield last_rowid

                    if c.rowcount < backfill_chunk_size:
                        break

        if last_rowid is not None and (i % CHECKPOINT_EVERY) != 0:
            yield last_rowid

op_mapping = {
    'I': 'c',
    'D': 'd',
    'U': 'u',
}

# the first two arguments are provided by functools.partial, the rest are row values
# the first three values are taken as scn, op and rowid, in order


def changes_rowfactory(table_name, cols, scn, op, rowid, *args):
    obj = dict(zip(cols[3:], args))
    obj['_meta'] = {
        'op': op_mapping[op],
        'source': {
            'table': table_name,
            'scn': scn,
            'row_id': rowid,
        }
    }

    return obj


async def fetch_changes(
    # Closed over via functools.partial:
    table: Table,
    pool: oracledb.AsyncConnectionPool,
    lock: asyncio.Lock,
    # Remainder is common.FetchPageFn:
    log: Logger,
    scn: LogCursor,
) -> AsyncGenerator[Document | LogCursor, None]:
    async with lock:
        query = template_env.get_template("inc").render(table=table, cursor=scn)
        log.debug("fetch_changes", query, scn)

        current_scn = None
        # if the task has found no events, we update the cursor
        # to the latest current_scn value to avoid falling behind. The SCN has a timestamp
        # component which can progress even if no transactions have happened
        async with pool.acquire() as conn:
            with conn.cursor() as c:
                await c.execute("SELECT current_scn FROM V$DATABASE")
                current_scn = (await c.fetchone())[0]

        # We may receive many documents from the same transaction, as such we need to only emit the checkpoint
        # for a transaction at the end, when all documents of that transaction have been emitted
        # so we have to keep track of the first two transaction SCNs we have to check whether we have all the documents
        # of the current SCN before moving on to the next one.
        first_transaction = None
        current_transaction = None
        async with pool.acquire() as conn:
            with conn.cursor() as c:
                c.arraysize = CHECKPOINT_EVERY
                c.prefetchrows = CHECKPOINT_EVERY + 1
                await c.execute(query)
                cols = [col[0] for col in c.description]
                c.rowfactory = functools.partial(changes_rowfactory, table.table_name, cols)

                async for row in c:
                    scn = row['_meta']['source']['scn']
                    yield row

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
        elif first_transaction is None and current_transaction is None:
            # over a long enough time with no events, the scn we hold will expire and be no longer
            # valid, hence this logic to catch up if there are no events
            if current_scn > scn:
                yield current_scn


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
{%- endfor %} FROM {{ table.quoted_owner }}.{{ table.quoted_table_name }}
    WHERE ROWID > '{{ rowid }}'
      AND ROWID <= '{{ max_rowid }}'
      AND ROWNUM <= :rownum_end
    ORDER BY ROWID ASC
""",
    'inc': """
SELECT /*+parallel */ VERSIONS_STARTSCN, VERSIONS_OPERATION, ROWID, {% for c in table.columns -%}
{%- if not loop.first %}, {% endif -%}
{{ c | cast }}
{%- endfor %} FROM {{ table.quoted_owner }}.{{ table.quoted_table_name }}
    VERSIONS BETWEEN SCN {{ cursor }} AND MAXVALUE
    WHERE VERSIONS_STARTSCN IS NOT NULL
    ORDER BY VERSIONS_STARTSCN ASC
"""
}))

template_env.filters['cast'] = cast_column
