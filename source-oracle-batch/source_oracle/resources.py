from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Awaitable, Iterable, Dict
from logging import Logger
import functools

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import Task
from estuary_cdk.capture import common
from estuary_cdk.http import HTTPSession, HTTPMixin, TokenSource


from .models import (
    EndpointConfig,
    build_table,
    ResourceState,
    ResourceConfig,
)
from .api import (
    connect,
    fetch_tables,
    fetch_columns,
    fetch_page,
    fetch_changes,
)

one_week_seconds = 60 * 60 * 24 * 7


async def validate_flashback(
    log: Logger, config: EndpointConfig
):
    skip_retention_checks = config.advanced.skip_flashback_retention_checks
    conn = connect(config)
    with conn.cursor() as c:
        c.execute("SELECT flashback_on FROM V$DATABASE")
        flashback_on = c.fetchone()[0] == "YES"

        c.execute("SELECT name, value FROM V$PARAMETER WHERE NAME IN ('undo_tablespace', 'undo_management', 'undo_retention')")
        params = dict(c.fetchall())
        undo_tablespace = params['undo_tablespace']
        undo_management = params['undo_management']
        undo_retention_seconds = int(params['undo_retention'])

        c.execute("SELECT max_size, retention FROM dba_tablespaces WHERE tablespace_name=:tablespace", tablespace=undo_tablespace)
        max_size, retention_mode = c.fetchone()

        c.execute("SELECT autoextensible FROM dba_data_files WHERE tablespace_name=:tablespace", tablespace=undo_tablespace)
        autoextensible = c.fetchone()[0] == "YES"

        avg_retention = None
        if params['undo_management'] == 'AUTO':
            c.execute("SELECT AVG(tuned_undoretention) from v$undostat")
            avg_retention_seconds = int(c.fetchone()[0])

        log.info("flashback configuration", {
            "undo_tablespace": undo_tablespace,
            "undo_management": undo_management,
            "undo_retention_seconds": undo_retention_seconds,
            "max_size": max_size,
            "retention_mode": retention_mode,
            "autoextensible": autoextensible,
            "flashback_on": flashback_on,
            "avg_retention_seconds": avg_retention_seconds,
        })

        if not flashback_on:
            raise Exception("Flashback must be enabled on the database. See go.estuary.dev/source-oracle for more information.")

        if undo_retention_seconds < one_week_seconds:
            msg = f"We require a minimum of 7 days UNDO_RETENTION to ensure consistency of this task. The current UNDO_RETENTION is {undo_retention_seconds} seconds. See go.estuary.dev/source-oracle for more information on how to configure the UNDO_RETENTION."  # nopep8
            if skip_retention_checks:
                log.warn(msg)
            else:
                raise Exception(msg)

        if avg_retention_seconds < one_week_seconds:
            msg = f"We require a minimum of 7 days undo retention to ensure consistency of this task. The current average auto-tuned retention of the database for the past four days is {avg_retention_seconds} seconds. See go.estuary.dev/source-oracle for more information on how to configure the UNDO_RETENTION."  # nopep8
            if skip_retention_checks:
                log.warn(msg)
            else:
                raise Exception(msg)

        if not autoextensible:
            log.warn("We recommend making your undo tablespace auto-extensible. See go.estuary.dev/source-oracle for more information.")

        if retention_mode != 'GUARANTEE':
            log.warn("We recommend guaranteeing retention of the undo tablespace. See go.estuary.dev/source-oracle for more information.")


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    resources_list = []

    conn = connect(config)
    oracle_tables = await fetch_tables(log, conn)
    oracle_columns = await fetch_columns(log, conn)

    current_scn = None
    with conn.cursor() as c:
        c.execute("SELECT current_scn FROM V$DATABASE")
        current_scn = c.fetchone()[0]

    for ot in oracle_tables:
        columns = [col for col in oracle_columns if col.table_name == ot.table_name]
        t = build_table(config.advanced, ot.table_name, columns)

        max_rowid = None
        with conn.cursor() as c:
            c.execute(f"SELECT max(ROWID) FROM {t.table_name}")
            max_rowid = c.fetchone()[0]
        backfill_cutoff = (max_rowid,)

        def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
        ):
            common.open_binding(
                binding,
                binding_index,
                state,
                task,
                fetch_page=functools.partial(fetch_page, t, conn),
                fetch_changes=functools.partial(fetch_changes, t, conn),
            )
        resources_list.append(common.Resource(
            name=t.table_name,
            key=[f"/{c.column_name}" for c in t.primary_key],
            model=t.create_model(),
            open=open,
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(cutoff=backfill_cutoff),
                inc=ResourceState.Incremental(cursor=current_scn),
            ),
            initial_config=ResourceConfig(
                name=t.table_name,
                interval=timedelta(seconds=0),
            ),
            schema_inference=False,
        ))

    return resources_list
