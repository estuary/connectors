import asyncio
import functools
import itertools
import re
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding

from source_netsuite.api.pool import NetsuiteConnectionPool, make_pool

from .api import (
    check_nulls,
    fetch_backfill_page,
    fetch_incremental_changes,
    fetch_oa_columns,
    fetch_snapshot,
)
from .models import (
    EndpointConfig,
    OAColumn,
    ResourceConfig,
    ResourceState,
    Table,
    build_table,
    build_tables,
    fuzzy_diff,
)


async def all_resources(log: Logger, config: EndpointConfig) -> list[common.Resource]:
    # await log_netsuite_role(config, logger)
    pool = make_pool(config, config.advanced.connection_limit)

    async with pool.get_connection() as conn:
        columns = await fetch_oa_columns(log, conn)

    # Persist OAColumns for faster debugging cycles.
    # with open("/tmp/oa_columns.jsonl", "w") as file:
    #    for column in columns:
    #        # Convert each instance to a JSON string and write it to the file
    #        file.write(column.model_dump_json(by_alias=True, exclude_unset=True) + "\n")

    # Reload OAColumns for faster debugging cycles.
    # with open("/tmp/oa_columns.jsonl", "r") as file:
    #    columns = [OAColumn.model_validate_json(line) for line in file]

    return await asyncio.gather(
        *[
            build_table_resource(log, table, pool, config.advanced.enable_auto_cursor)
            for table in build_tables(columns, config)
        ]
    )


async def build_table_resource(
    log: Logger, table: Table, pool: NetsuiteConnectionPool, auto_cursor: bool
) -> common.Resource:

    recommended_name = re.sub(r"\W", "_", table.table_name)

    initial_log_cursor = ""
    if table.possible_log_cursors:
        initial_log_cursor = table.possible_log_cursors[0][1].column_name

    initial_page_cursor = ""
    if table.possible_page_cursors:
        if auto_cursor:
            async with pool.get_connection() as conn:
                for possible_page_cursor in table.possible_page_cursors:
                    cursor_candidate = possible_page_cursor[1].column_name
                    if not await check_nulls(log, conn, table.table_name, cursor_candidate, timedelta(seconds=30)):
                        initial_page_cursor = cursor_candidate
                        log.info(
                            "found page_cursor candidate with no null rows",
                            {
                                "cursor_candidate": cursor_candidate,
                                "table": table.table_name,
                                "possible_page_cursors": [c[1].column_name for c in table.possible_page_cursors],
                            },
                        )
                        break
                    else:
                        log.info(
                            "page_cursor candidate has null rows",
                            {"cursor_candidate": cursor_candidate, "table": table.table_name},
                        )
            if not initial_page_cursor:
                raise Exception(f"No page_cursor candidate found with no null rows for table {table.table_name}")
        else:
            initial_page_cursor = table.possible_page_cursors[0][1].column_name

    if table.primary_key:
        key = [f"/{pk.column_name}" for pk in table.primary_key]
    elif (
        initial_log_cursor
        and initial_page_cursor
        and fuzzy_diff(table.table_name, initial_page_cursor) < 0.5
        and fuzzy_diff(initial_page_cursor, table.table_name) < 0.5
    ):
        # It's common for custom tables to have no official primary key, but the
        # detected paging cursor is an obvious semantic key. For example,
        # "EVENTS" => "EVENT_ID". Using the paging cursor as key is a guess but
        # tends to work well to make more collections "do the right thing".
        key = [f"/{initial_page_cursor}"]

        # Fix-up the generated schema to make the primary key a non-NULL int.
        field_info = table.model_fields[initial_page_cursor]
        field_info[1].default = 0
        table.model_fields[initial_page_cursor] = (int, field_info[1])
    else:
        # We don't have a reasonable key for this collection.
        # We can backfill it, but cannot incrementally replicate it.
        initial_log_cursor = ""
        key = ["/_meta/row_id"]

    # start incremental replication as-of 24 hours ago.
    started_at = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    started_at -= timedelta(days=1)

    initial_config = ResourceConfig(
        name=table.table_name,
        interval=timedelta(hours=1),
        log_cursor=initial_log_cursor,
        page_cursor=initial_page_cursor,
    )
    initial_state = ResourceState(
        inc=ResourceState.Incremental(cursor=started_at),
        backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
    )
    table_model = table.create_model()

    log.debug(
        f"discovered binding",
        {
            "table_name": table.table_name,
            "detected_key": key,
            "detected_config": initial_config,
        },
    )

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
    ) -> None:

        return common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=(
                functools.partial(fetch_backfill_page, binding.resourceConfig, pool, table, table_model, key)
                if binding.resourceConfig.page_cursor
                else None
            ),
            fetch_changes=(
                functools.partial(fetch_incremental_changes, binding.resourceConfig, pool, table, table_model, key)
                if binding.resourceConfig.log_cursor
                else None
            ),
            fetch_snapshot=(
                functools.partial(fetch_snapshot, binding.resourceConfig, pool, table, table_model, key)
                if not (binding.resourceConfig.log_cursor or binding.resourceConfig.page_cursor)
                else None
            ),
            tombstone=table_model(_meta=common.BaseDocument.Meta(op="d")),
        )

    return common.Resource(
        name=recommended_name,
        key=key,
        model=table_model,
        open=open,
        initial_config=initial_config,
        initial_state=initial_state,
        schema_inference=False,
    )
