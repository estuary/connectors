import asyncio
import time
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, TypeVar

import netsuite
import pyodbc
from estuary_cdk.capture import common
from pydantic import AwareDatetime, BaseModel

from source_netsuite.api.pool import NetsuiteConnectionPool
from source_netsuite.api.utils import assert_no_duplicate_primary_keys

from ..models import (
    EndpointConfig,
    NextPageCursor,
    OAColumn,
    ResourceConfig,
    Table,
    TokenInfo,
    build_table,
)

# Format cursors into strings like "2024-03-15T22:34:37"
DATETIME_TO_TS = "%Y-%m-%dT%H:%M:%S"
# Parse strings like "2024-03-15T22:34:37" into Oracle timestamps.
TS_TO_TIMESTAMP = 'YYYY-MM-DD"T"HH24:MI:SS'


async def log_netsuite_role(log: Logger, config: EndpointConfig):
    """
    NetSuite allows us to get some metadata around the token that was given to us.

    A common mistakes users will make is setting up the wrong role for the integration record.
    NetSuite makes it very challenging to know what keys are assigned to what role, by inspecting
    and logging some of the information it will make it easier for users to debug their connector.
    """

    ns = netsuite.NetSuite(config.as_netsuite_config())
    token = TokenInfo.model_validate(await ns.rest_api.token_info())

    # on suiteanalytics there is a default role provided by netsuite which makes it easy for ETL solutions to extract data
    # if the user chooses *not* to use this role (which could be a good idea, principle of least privilege) then we should
    # notify them that there will be potential weird errors that will have to debug on their own
    if config.connection_type == "suiteanalytics":
        default_suiteanalytics_role = "Data Warehouse Integrator"
        if token.role_name != default_suiteanalytics_role:
            log.warn(
                f"NetSuite role is '{token.role_name}' instead of the default {default_suiteanalytics_role}. This may trigger errors caused by permission configuration which are challenging to understand and debug."
            )

    log.info("Using effective NetSuite role", token)


async def fetch_oa_columns(log: Logger, conn: pyodbc.Connection) -> list[OAColumn]:
    select = [f'{v.alias} as "{v.alias}"' for v in OAColumn.model_fields.values()]
    out: list[OAColumn] = []

    async for row in run_odbc_query(
        log,
        OAColumn,
        conn,
        f"""
        SELECT {','.join(select)}
        FROM OA_COLUMNS 

        LEFT JOIN OA_FKEYS AS PK ON
                PK.PKTABLE_NAME  = OA_COLUMNS.TABLE_NAME
            AND PK.PKCOLUMN_NAME = OA_COLUMNS.COLUMN_NAME
            AND PK.PK_NAME LIKE '%_PK'
        
        LEFT JOIN OA_FKEYS AS FK ON
                FK.FKTABLE_NAME  = OA_COLUMNS.TABLE_NAME
            AND FK.FKCOLUMN_NAME = OA_COLUMNS.COLUMN_NAME
            AND FK.FK_NAME LIKE '%_FK'

        WHERE TABLE_OWNER != 'SYSTEM'
        """,
        [],
        timedelta(minutes=5),
    ):
        out.append(row)

    return out


async def check_nulls(log: Logger, conn: pyodbc.Connection, table_name: str, col: str, timeout: timedelta) -> bool:
    class RespModel(BaseModel):
        pass

    try:
        await anext(
            run_odbc_query(
                log,
                RespModel,
                conn,
                f'SELECT TOP 1 "{col}" AS null_count FROM "{table_name}" WHERE "{col}" IS NULL',
                [],
                timeout,
            )
        )
        return True
    except StopAsyncIteration:
        return False


# TODO(johnny): Somewhat principled hack to generate a monotonic row_id.
# Ideally this sequence would count by ones from zero,
# so that backfilling a collection keyed on /_meta/row_id doesn't result
# in duplicates, similar to how snapshots behave. That requires CDK support.
NEXT_ROW_ID: int = int(time.time() * 10_000)


async def fetch_snapshot(
    # Closed over via functools.partial:
    config: ResourceConfig,
    pool: NetsuiteConnectionPool,
    table: Table,
    table_model: type[common.BaseDocument],
    key: list[str],
    # Remainder is common.FetchSnapshotFn:
    log: Logger,
) -> AsyncGenerator[common.BaseDocument, None]:
    log.info("started snapshot")
    started_at = datetime.now(tz=UTC)

    columns = [f'"{c.column_name}"' for c in table.columns]
    num_rows: int = 0
    key_set: set[tuple[Any]] = set()

    global NEXT_ROW_ID

    async with pool.get_connection() as conn:
        async for row in run_odbc_query(
            log,
            table_model,
            conn,
            f"""
            SELECT {",".join(columns)}
            FROM {table.table_name}
            """,
            [],
            config.query_timeout,
        ):
            num_rows += 1

            row.meta_ = row.Meta(row_id=NEXT_ROW_ID, op="u")
            assert_no_duplicate_primary_keys(key, key_set, row)
            yield row

            NEXT_ROW_ID += 1

    finished_at = datetime.now(tz=UTC)
    duration = finished_at - started_at
    log.info(
        "completed snapshot",
        {
            "num_rows": num_rows,
            "duration": duration,
            "rps": int(num_rows / duration.total_seconds()),
        },
    )


async def fetch_first_cursor_val(
    config: ResourceConfig,
    pool: NetsuiteConnectionPool,
    table: Table,
    log: Logger,
) -> common.PageCursor:
    class RespModel(BaseModel):
        cursor: common.PageCursor

    async with pool.get_connection() as conn:
        try:
            row = await anext(
                run_odbc_query(
                    log,
                    RespModel,
                    conn,
                    f"""
                    SELECT TOP 1 "{config.page_cursor}" AS cursor
                    FROM {table.table_name}
                    ORDER BY "{config.page_cursor}" ASC
                    """,
                    [],
                    config.query_timeout,
                )
            )
            return row.cursor
        except StopAsyncIteration:
            return None


async def fetch_backfill_page(
    # Closed over via functools.partial:
    config: ResourceConfig,
    pool: NetsuiteConnectionPool,
    table: Table,
    table_model: type[common.BaseDocument],
    key: list[str],
    # Remainder is common.FetchSnapshotFn:
    log: Logger,
    start_cursor: common.PageCursor,
    log_cutoff: common.LogCursor,
) -> AsyncGenerator[common.BaseDocument | common.PageCursor, None]:
    log.info(
        "started backfill page",
        {
            "start_cursor": start_cursor,
            "log_cutoff": log_cutoff,
        },
    )

    if not start_cursor:
        log.info(
            "fetching very first cursor value",
        )
        start_cursor = await fetch_first_cursor_val(config, pool, table, log)
        log.info(
            "fetched very first cursor value",
            {"first_cursor": start_cursor},
        )

    assert isinstance(start_cursor, int)
    assert isinstance(log_cutoff, datetime)

    started_at = datetime.now(tz=UTC)

    columns = [f'"{c.column_name}"' for c in table.columns]
    cur_cursor = start_cursor
    cur_cursor_rows: list[common.BaseDocument] = []
    num_checkpoints: int = 0
    num_rows: int = 0
    stop_cursor = start_cursor + config.query_limit
    key_set: set[tuple[Any]] = set()

    global NEXT_ROW_ID

    def raise_too_many_rows():
        raise RuntimeError(
            f"""
            Backfill paging cursor {config.page_cursor} value {cur_cursor} has at least {len(cur_cursor_rows)} rows,
            and the connector cannot make progress backfilling {table.table_name}.

            Possible cursors: {[n.column_name for _, n in table.possible_page_cursors]}

            Please choose another backfill cursor, disable this binding if it's not needed,
            or contact support@estuary.dev for help.
            """.strip()
        )

    query = f"""
        SELECT TOP {config.query_limit} {",".join(columns)}
        FROM {table.table_name}
        WHERE {config.page_cursor} >= ?
            AND {config.page_cursor} <  ?
        ORDER BY {config.page_cursor} ASC
        """
    args = [start_cursor, stop_cursor]

    async with pool.get_connection() as conn:
        async for row in run_odbc_query(
            log,
            table_model,
            conn,
            query,
            args,
            config.query_timeout,
        ):

            next_cursor: int = getattr(row, config.page_cursor)

            # If we start off in the middle of a hole, i.e the first row returned has a cursor
            # value after `start_cursor`, we won't have any previously buffered rows, so we
            # need to skip this step or else we'll incorrectly emit an empty checkpoint
            if next_cursor > cur_cursor and len(cur_cursor_rows) > 0:
                # We've read through all cursor values < `next_cursor`.
                for buffered_row in cur_cursor_rows:
                    num_rows += 1
                    assert_no_duplicate_primary_keys(key, key_set, buffered_row)
                    yield buffered_row
                yield next_cursor

                num_checkpoints += 1
                cur_cursor_rows.clear()
                cur_cursor = next_cursor

            # We've reached the limit that we're comfortable buffering in memory. At this point,
            # we opt to load all rows for this cursor value as a single shot, and emit them as they
            # come in. The hope is that this will be a streaming query response, and we won't have
            # to buffer so many rows in memory.
            elif len(cur_cursor_rows) >= 25_000:
                # Fetch all rows in this same-valued region in one shot and emit them as a checkpoint.
                # We have to throw away `cur_cursor_rows` because it contains an incomplete set of rows
                # for this cursor value, and by definition we don't have any way to know which of those
                # rows we have, or have not loaded yet.
                cur_cursor_rows.clear()
                log.info("attempting to one-shot load all rows for single cursor value", {"cursor": cur_cursor})
                row_count = 0
                async for row in run_odbc_query(
                    log,
                    table_model,
                    conn,
                    f"""
                    SELECT {",".join(columns)}
                    FROM {table.table_name}
                    WHERE {config.page_cursor} = ?
                    """,
                    [cur_cursor],
                    config.query_timeout,
                ):
                    row_count += 1
                    num_rows += 1

                    if config.log_cursor and (ts := getattr(row, config.log_cursor)) and ts >= log_cutoff:
                        continue  # Suppress row captured by incremental replication.

                    row.meta_ = row.Meta(row_id=NEXT_ROW_ID, op="u")
                    NEXT_ROW_ID += 1

                    assert_no_duplicate_primary_keys(key, key_set, row)

                    yield row

                log.info("finished one-shot load", {"cursor": cur_cursor, "rows": row_count})
                num_checkpoints += 1
                cur_cursor += 1
                yield cur_cursor

                # We cannot continue to read rows from the original paginated query cursor as we just
                # re-used its connection to do this one-shot load.
                break

            if config.log_cursor and (ts := getattr(row, config.log_cursor)) and ts >= log_cutoff:
                continue  # Suppress row captured by incremental replication.

            row.meta_ = row.Meta(row_id=NEXT_ROW_ID, op="u")
            NEXT_ROW_ID += 1

            cur_cursor_rows.append(row)

    num_rows += len(cur_cursor_rows)
    if num_rows >= config.query_limit:
        # Our query results were cut off upon reaching the query limit, and we
        # cannot assume we've read all rows of `cur_cursor`
        # NOTE: So long as config.query_limit is > 25k, this condition should never be met.
        # Either we see multiple cursor values with <25k rows in which case we checkpoint,
        # or we exceed the 25k limit and one-shot load, in which case we checkpoint,
        # or we fail in which case we don't reach this code in the first place.
        if num_checkpoints == 0:
            raise_too_many_rows()
        else:
            pass  # Discard partial `cur_cursor` rows.

    elif cur_cursor_rows:
        # Our query returned fewer than the limit, indicating that
        # we've read through all cursor values < `stop_cursor`.
        for row in cur_cursor_rows:
            assert_no_duplicate_primary_keys(key, key_set, row)
            yield row
        yield stop_cursor

        num_checkpoints += 1
        cur_cursor_rows.clear()
        cur_cursor = stop_cursor

    elif num_rows == 0:
        # Our query returned no rows, either because no rows remain
        # or due to a gap in the page cursor sequence.
        #
        # Attempt to fetch a next, greater page cursor to resume from:
        # * If one is found then we'll yield it as an empty checkpoint,
        #   from which the next backfill will resume.
        # * If not then our non-yielding return signals that backfill is done.
        async with pool.get_connection() as conn:
            if config.log_cursor:
                next_cursor_query = f"""
                    SELECT TOP 1 {config.page_cursor} AS "next_page_cursor"
                    FROM {table.table_name}
                    WHERE {config.page_cursor} > ?
                    AND "{config.log_cursor}" < TO_DATE(?, '{TS_TO_TIMESTAMP}')
                    ORDER BY {config.page_cursor} ASC
                """
                next_cursor_args = [start_cursor, log_cutoff.strftime(DATETIME_TO_TS)]
            else:
                next_cursor_query = f"""
                    SELECT TOP 1 {config.page_cursor} AS "next_page_cursor"
                    FROM {table.table_name}
                    WHERE {config.page_cursor} > ?
                    ORDER BY {config.page_cursor} ASC
                """
                next_cursor_args = [start_cursor]
            async for next in run_odbc_query(
                log,
                NextPageCursor,
                conn,
                next_cursor_query,
                next_cursor_args,
                config.query_timeout,
            ):
                yield next.next_page_cursor

                num_checkpoints += 1
                cur_cursor = next.next_page_cursor

    finished_at = datetime.now(tz=UTC)
    duration = finished_at - started_at
    log.info(
        "completed backfill page",
        {
            "num_rows": num_rows,
            "num_checkpoints": num_checkpoints,
            "cur_cursor": cur_cursor,
            "discarded": len(cur_cursor_rows),
            "duration": duration,
            "rps": int(num_rows / duration.total_seconds()),
        },
    )


async def fetch_incremental_changes(
    # Closed over via functools.partial:
    config: ResourceConfig,
    pool: NetsuiteConnectionPool,
    table: Table,
    table_model: type[common.BaseDocument],
    key: list[str],
    # Remainder is common.FetchChangesFn:
    log: Logger,
    start_cursor: common.LogCursor,
) -> AsyncGenerator[common.BaseDocument | common.LogCursor, None]:
    assert isinstance(start_cursor, datetime)

    log.info(
        "started incremental page",
        {
            "start_cursor": start_cursor,
        },
    )
    started_at = datetime.now(tz=UTC)

    columns = [f'"{c.column_name}"' for c in table.columns]
    cur_cursor = start_cursor
    cur_cursor_rows: list[common.BaseDocument] = []
    num_checkpoints: int = 0
    num_rows: int = 0
    key_set: set[tuple[Any]] = set()

    def raise_too_many_rows():
        raise RuntimeError(
            f"""
            Incremental cursor {config.log_cursor} value {cur_cursor} has at least {len(cur_cursor_rows)} rows,
            and the connector cannot make progress replicating {table.table_name}.

            Possible cursors: {[n.column_name for _, n in table.possible_log_cursors]}

            Please backfill this capture binding, disable this binding if it's not needed,
            or contact support@estuary.dev for help.
            """.strip()
        )

    async with pool.get_connection() as conn:
        async for row in run_odbc_query(
            log,
            table_model,
            conn,
            f"""
            SELECT TOP {config.query_limit} {",".join(columns)}
            FROM {table.table_name}
            WHERE {config.log_cursor} >= TO_DATE(?, '{TS_TO_TIMESTAMP}')
              AND {config.log_cursor} <  TO_DATE(?, '{TS_TO_TIMESTAMP}')
            ORDER BY {config.log_cursor} ASC
            """,
            [
                start_cursor.strftime(DATETIME_TO_TS),
                # Fetch only records which are at least a minute old, in an attempt
                # to avoid reading from cursor values which could still be updating.
                (datetime.now(tz=UTC) - timedelta(minutes=1)).strftime(DATETIME_TO_TS),
            ],
            config.query_timeout,
        ):
            num_rows += 1
            next_cursor: AwareDatetime = getattr(row, config.log_cursor)

            if cur_cursor < next_cursor:
                # We've read through all cursor values < `next_cursor`.
                for buffered_row in cur_cursor_rows:
                    yield buffered_row
                yield next_cursor

                num_checkpoints += 1
                cur_cursor_rows.clear()
                cur_cursor = next_cursor

            elif len(cur_cursor_rows) == 25_000:
                raise_too_many_rows()  # Don't OOM.

            global NEXT_ROW_ID
            row.meta_ = row.Meta(row_id=NEXT_ROW_ID, op="u")
            NEXT_ROW_ID += 1

            assert_no_duplicate_primary_keys(key, key_set, row)

            cur_cursor_rows.append(row)

    if num_rows >= config.query_limit:
        # Our query results were cut off upon reaching the query limit,
        # and we cannot assume we've read all rows of `cur_cursor`.
        if num_checkpoints == 0:
            raise_too_many_rows()
        else:
            pass  # Discard partial `cur_cursor` rows.

    elif cur_cursor_rows:
        # We've read through `cur_cursor` and progress to the next second,
        # which is NetSuite's timestamp resolution.
        # CAUTION: We're making an assumption here, that no rows can arrive
        # in the future that have this cursor value (to the second).
        for row in cur_cursor_rows:
            yield row

        cur_cursor += timedelta(seconds=1)
        yield cur_cursor

        num_checkpoints += 1
        cur_cursor_rows.clear()

    finished_at = datetime.now(tz=UTC)
    duration = finished_at - started_at
    log.info(
        "completed incremental page",
        {
            "num_rows": num_rows,
            "num_checkpoints": num_checkpoints,
            "cur_cursor": cur_cursor,
            "discarded": len(cur_cursor_rows),
            "duration": duration,
            "rps": int(num_rows / duration.total_seconds()),
        },
    )


Row = TypeVar("Row", bound=BaseModel)
"""Row is a generic pydantic model which is mapped into from a rows of an ODBC query."""


async def run_odbc_query(
    log: Logger,
    cls: type[Row],
    conn: pyodbc.Connection,
    query: str,
    params: list[Any],
    timeout: timedelta,
) -> AsyncGenerator[Row, None]:
    log.debug("run_odbc_query started", {"query": query, "params": params})

    # Start the query on a background thread and await its cursor,
    # which resolves with the first row of data.
    cur: pyodbc.Cursor = await asyncio.wait_for(
        asyncio.to_thread(_start_query, conn, query, params),
        timeout=timeout.total_seconds(),
    )
    log.debug("run_odbc_query received result set cursor")

    count = 0
    while True:
        rows: list[Row] = await asyncio.wait_for(
            asyncio.to_thread(_read_rows, cls, cur, 1000),
            timeout=timeout.total_seconds(),
        )
        count += len(rows)

        if not rows:
            cur.close()
            break

        for row in rows:
            yield row

    log.debug("run_odbc_query completed", {"count": count})


def _start_query(
    conn: pyodbc.Connection,
    query: str,
    params: list[Any],
) -> pyodbc.Cursor:
    cur = conn.cursor()
    cur.execute(query, *params)
    return cur


def _read_rows(
    cls: type[Row],
    cur: pyodbc.Cursor,
    n: int,
) -> list[Row]:
    rows: list[pyodbc.Row] = cur.fetchmany(n)
    # Extract column names of the result set.
    columns: list[str] = [c[0] for c in cur.description]
    # Map `rows` into `cls` instances by passing non-NULL columns as named arguments.
    return [cls(**{c: r[i] for i, c in enumerate(columns) if r[i] is not None}) for r in rows]
