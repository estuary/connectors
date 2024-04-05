import asyncio
import json
import logging
import logging.config
import subprocess
from datetime import timedelta
from typing import Any, Coroutine, TypeVar

import pyodbc
from estuary_cdk.logger import init_logger
from perscache import Cache
from perscache.serializers import JSONSerializer
from perscache.storage import LocalFileStorage
from pydantic import BaseModel

from source_netsuite.api import fetch_oa_columns, run_odbc_query
from source_netsuite.api.pool import NetsuiteConnectionPool, make_pool
from source_netsuite.models import EndpointConfig, Table

creds_file = "doma.yaml"


def strip_suffix_from_keys(input_dict: dict[str, Any], suffix) -> dict[str, Any]:
    """Recursively strip a suffix from all keys in a dictionary."""
    if not isinstance(input_dict, dict):
        return input_dict

    new_dict = {}
    for key, value in input_dict.items():
        # Remove the suffix from the key if it exists
        new_key = key[: -len(suffix)] if key.endswith(suffix) else key

        # Recurse into nested dictionaries
        if isinstance(value, dict):
            new_value = strip_suffix_from_keys(value, suffix)
        # Recurse into lists
        elif isinstance(value, list):
            new_value = [strip_suffix_from_keys(item, suffix) if isinstance(item, dict) else item for item in value]
        else:
            new_value = value

        new_dict[new_key] = new_value

    return new_dict


def decrypt_endpoint_config(filename: str):
    # First we need to load the config file into an EndpointConfig.
    # To do that, we need to de-sops it, and then load it with Pydantic

    decrypted = subprocess.run(
        [
            "sops",
            "--decrypt",
            "--output-type",
            "json",
            filename,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    parsed: dict[str, Any] = json.loads(decrypted.stdout)
    # We need to manually strip `_sops` here since we aren't running through `flowctl`
    return EndpointConfig.model_validate(strip_suffix_from_keys(parsed, "_sops"))


cache = Cache(storage=LocalFileStorage(f"./.cache/{creds_file.split('.')[0]}"), serializer=JSONSerializer())


@cache(ignore=["log", "pool"])
async def count_rows(
    log: logging.Logger, pool: NetsuiteConnectionPool, table_name: str, timeout: timedelta, timeout_val=None
) -> int:
    class RespModel(BaseModel):
        row_count: int

    try:
        async with pool.get_connection() as conn:
            return (
                await anext(
                    run_odbc_query(
                        log,
                        RespModel,
                        conn,
                        f'SELECT COUNT(1) AS row_count FROM "{table_name}"',
                        [],
                        timeout,
                    )
                )
            ).row_count
    except TimeoutError as e:
        if timeout_val is not None:
            return timeout_val
        raise e


# @cache(ignore=["log", "conn", "cls", "table"])
def get_all_vals(
    log: logging.Logger,
    conn: pyodbc.Connection,
    table_name: str,
    cls: type[BaseModel],
    table: Table,
    timeout: timedelta,
):
    columns = [f'"{c.column_name}"' for c in table.columns]

    return run_odbc_query(
        log,
        cls,
        conn,
        f'SELECT {", ".join(columns)} FROM "{table_name}"',
        [],
        timeout,
    )


@cache(ignore=["log", "conn"])
def get_rows_for_null(
    log: logging.Logger,
    conn: pyodbc.Connection,
    table_name: str,
    column_name: str,
    timeout: timedelta,
):
    class RespModel(BaseModel):
        val: Any

    return run_odbc_query(
        log,
        RespModel,
        conn,
        f'SELECT * FROM "{table_name}" where "{column_name}" IS NULL',
        [],
        timeout,
    )


T = TypeVar("T")


async def run_concurrently(tasks: list[Coroutine[Any, Any, T]], concurrency_limit: int) -> list[T]:
    sem = asyncio.Semaphore(concurrency_limit)
    results: dict[int, T] = {}

    async def sem_task(task: Coroutine[Any, Any, T], index: int):
        async with sem:
            result = await task
            results[index] = result  # Store result at correct position

    # Wrap tasks with sem_task to control concurrency and track their index
    wrapped_tasks = [sem_task(task, i) for i, task in enumerate(tasks)]

    # Wait for all wrapped tasks to complete
    await asyncio.gather(*wrapped_tasks)

    return list(results.values())


def load_config():
    cfg = decrypt_endpoint_config(creds_file)
    return cfg


@cache
async def feasible_tables(limit: int):
    log = init_logger()

    cfg = load_config()
    pool = make_pool(cfg, cfg.advanced.connection_limit)

    async with pool.get_connection() as conn:
        columns = await fetch_oa_columns(log, conn)

    tables = set(col.table_name for col in columns)

    log.info(f"Got {len(tables)} tables.")

    table_counts = list(
        zip(
            tables,
            await run_concurrently(
                list(count_rows(log, pool, table, timeout=timedelta(seconds=20), timeout_val=-1) for table in tables),
                cfg.advanced.connection_limit - 1,
            ),
        )
    )

    if limit:
        cond = lambda count: count is not None and count > 0 and count < limit
    else:
        cond = lambda count: count is not None

    feasible_tables = sorted(
        [table for table, count in table_counts if cond(count)],
        reverse=True,
    )

    infeasible_tables = sorted(
        [table for table, count in table_counts if not cond(count)],
        reverse=True,
    )

    log.info(
        f"Got {len(feasible_tables)} feasible tables, and {len(infeasible_tables)} infeasible tables.",
        {"infeasible_tables": infeasible_tables},
    )

    return feasible_tables
