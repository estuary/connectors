# We want to test that the number of unique primary keys that were captured
# match the number of unique primary keys in the table itself.
import asyncio
import io
import json
from datetime import timedelta

import debugpy
import pytest
from estuary_cdk.capture import Task
from estuary_cdk.flow import CaptureBinding, CollectionSpec
from estuary_cdk.logger import init_logger

from source_netsuite.api import fetch_oa_columns
from source_netsuite.api.pool import make_pool
from source_netsuite.api.utils import resolve_json_pointer
from source_netsuite.models import build_table
from source_netsuite.resources import build_table_resource
from source_netsuite.test.utils import count_rows, get_all_vals, load_config


@pytest.mark.asyncio
async def test_unique_primary_keys(feasible_table_name: str):
    # debugpy.listen(5678)
    # debugpy.wait_for_client()
    log = init_logger()
    config = load_config()

    pool = make_pool(config, config.advanced.connection_limit)

    async with pool.get_connection() as conn:
        columns = await fetch_oa_columns(log, conn)

    # TODO(jshearer): Factor this out
    table = build_table(
        config.advanced, feasible_table_name, list(col for col in columns if col.table_name == feasible_table_name)
    )
    if not table:
        raise Exception(f"Could not find table {feasible_table_name}")

    resource = await build_table_resource(log, table, pool, True)

    capture_binding = CaptureBinding(
        collection=CollectionSpec(
            name=table.table_name, key=resource.key, writeSchema=resource.model.model_json_schema()
        ),
        resourceConfig=resource.initial_config,
        resourcePath=resource.initial_config.path(),
        stateKey="foo",
        backfill=0,
    )

    membuffer = io.BytesIO()
    stopping = Task.Stopping(asyncio.Event())

    # Critically: All tasks are awaited when the context manager exits.
    async with asyncio.TaskGroup() as tg:
        state = resource.initial_state

        task = Task(
            log.getChild("capture"),
            "capture",
            membuffer,
            stopping,
            tg,
        )
        resource.open(capture_binding, 0, state, task)
        # Pause for one second and then set stopping
        await asyncio.sleep(1)
        stopping.event.set()

    keys = {}

    for line in membuffer.getvalue().decode("utf-8").split("\n"):
        if not line:
            continue
        doc = json.loads(line)
        try:
            doc = doc["captured"]["doc"]
        except KeyError:
            continue
        doc_keys = []
        for key in resource.key:
            doc_keys.append(resolve_json_pointer(doc, key))
        tuple_key = tuple(doc_keys)
        if tuple_key not in keys:
            keys[tuple_key] = 1
        else:
            keys[tuple_key] += 1

    num_rows = await count_rows(log, pool, feasible_table_name, timeout=timedelta(seconds=20), timeout_val=-1)
    duplicate_keys = list(k for k, v in keys.items() if v > 1)
    print("Duplicate keys: ", duplicate_keys)

    if len(keys.keys()) != num_rows:
        async with pool.get_connection() as conn:
            all_docs = [
                doc
                async for doc in get_all_vals(
                    log, conn, feasible_table_name, table.create_model(), table, timedelta(seconds=30)
                )
            ]
        if resource.key == ["/_meta/row_id"]:
            # /meta/row_id _should_ be idempotent, i.e start at 0 and count up, but
            # that's not supported by the CDK yet so it doesn't, so all we can do is
            # compare doc counts.
            # The reason we need to compare doc counts is that APPARENTLY, `select count(1)`
            # sometimes doesn't quite line up with the number of docs from `select *`...
            assert len(keys.keys()) == len(all_docs)
        else:

            all_vals = set(
                [
                    tuple(
                        (
                            resolve_json_pointer(doc.model_dump(mode="json", by_alias=True, exclude_unset=True), key)
                            for key in resource.key
                        )
                    )
                    for doc in all_docs
                ]
            )

            key_list = set(keys.keys())

            assert key_list == all_vals
    else:
        assert len(keys.keys()) == num_rows
