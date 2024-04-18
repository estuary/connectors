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
    fetch_rows,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    started_at = datetime.now(tz=UTC)
    resources_list = []

    conn = connect(config)
    oracle_tables = await fetch_tables(log, conn)
    oracle_columns = await fetch_columns(log, conn)

    for ot in oracle_tables:
        columns = [col for col in oracle_columns if col.table_name == ot.table_name]
        t = build_table(config.advanced, ot.table_name, columns)

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
                fetch_page=functools.partial(fetch_rows, t, conn, binding.resourceConfig.cursor),
            )
        resources_list.append(common.Resource(
            name=t.table_name,
            key=[f"/{c.column_name}" for c in t.primary_key],
            model=t.create_model(),
            open=open,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=started_at),
                backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
            ),
            initial_config=ResourceConfig(
                name=t.table_name,
                interval=timedelta(seconds=0),
                cursor=[],
            ),
            schema_inference=False,
        ))

    return resources_list
