import asyncio
import base64
import io
import json
from collections import deque
from logging import getLogger
from unittest.mock import MagicMock

import pytest
from azure.cosmos.exceptions import CosmosHttpResponseError
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Triggers
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec
from estuary_cdk.utils import json_merge_patch
from jsonschema import Draft202012Validator

from source_cosmosdb_nosql import resources
from source_cosmosdb_nosql.api import SDK_CONTINUATION_PREFIX, fetch_container
from source_cosmosdb_nosql.models import ConnectorState, Document, EndpointConfig, ResourceConfig, ResourceState

from .fakes import (
    CONFIG, ENDPOINT, KEY, FakeTransport, RoutedTransport, capture_once, feed_headers, install_cosmos,
    sample_replies,
)
from .test_discover import discover


LOG = getLogger(__name__)
ORDERS = {
    "id": "orders",
    "_rid": "dGVzdHJpZDA=",
    "partitionKey": {"paths": ["/customer"], "kind": "Hash", "version": 2},
}
# The bound collection's write schema, which sets the partition key's default.
ORDERS_SCHEMA = {"properties": {"customer": {"type": "string"}}}
VALUE = {"op": "u", "partitionKeyState": "value"}
RANGES = [{"id": "0", "minInclusive": "", "maxExclusive": "FF"}]
TWO_RANGES = [
    {"id": "0", "minInclusive": "", "maxExclusive": "80"},
    {"id": "1", "minInclusive": "80", "maxExclusive": "FF"},
]
ITEMS = [
    {
        "id": "o1", "customer": "acme", "status": "new", "canary": "o1",
        "_rid": "made-up-item-rid", "_ts": 123, "_etag": '"item-etag"', "_lsn": 1,
        "_self": "dbs/test/colls/orders/docs/o1/", "_attachments": "attachments/",
        "details": {"flag": True, "values": [1, None, "two"]},
    },
    {"id": "o2", "customer": "acme", "status": "new", "canary": "o2"},
    {"id": "o3", "customer": "globex"},
]


def initialization(container=ORDERS, ranges=RANGES):
    return [
        (200, {}, {}),
        (200, container, {}),
        (200, {"PartitionKeyRanges": ranges}, {"etag": '"ranges"'}),
        (304, {}, {"etag": '"ranges"'}),
    ]


def page(items, etag):
    return (200, {"Documents": items}, {"etag": etag})


def caught_up(etag):
    return (304, {}, {"etag": etag})


async def poll(cursor=("",), container="orders", schema=ORDERS_SCHEMA):
    async with resources.CosmosClient(
        CONFIG["endpoint"], credential=KEY, enable_endpoint_discovery=False,
    ) as client:
        return [
            item async for item in fetch_container(client, CONFIG["database"], container, LOG, cursor, schema=schema)
        ]


@pytest.mark.asyncio
async def test_backfill_pages_preserve_documents(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        *initialization(),
        page(ITEMS[:2], '"2"'),
        page(ITEMS[2:], '"3"'),
        caught_up('"3"'),
    ])
    output = await poll()
    documents = [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)]
    assert documents == [
        {
            **{key: value for key, value in item.items() if key not in ("_self", "_attachments")},
            "_meta": VALUE,
        }
        for item in ITEMS
    ]
    assert [type(item) for item in output] == [Document, Document, tuple, Document, tuple]
    headers = feed_headers(transport)
    assert [header.get("if-none-match") for header in headers] == [None, '"2"', '"3"']
    assert all(header["a-im"] == "Incremental Feed" for header in headers)
    assert transport.client_options["enable_endpoint_discovery"] is False
    assert all(url.startswith(ENDPOINT) for _, url, _ in transport.requests)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("samples", [[], ["new-partition"]])
async def test_system_key_container_discovery_and_capture(monkeypatch, samples):
    container = {
        "id": "legacy", "_rid": "dGVzdHJpZDA=",
        "partitionKey": {"paths": ["/_partitionKey"], "kind": "Hash", "systemKey": True},
    }
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": [container]}, {}),
        *sample_replies(container, samples),
    ])
    binding = (await discover()).bindings[0]
    assert binding.key == ["/_partitionKey", "/_meta/partitionKeyState", "/id"]
    assert not transport.replies
    assert transport.closed
    items = [{"id": "same-id", "name": "legacy", "_meta": {"source": True}}]
    if samples:
        items.append({"id": "same-id", "_partitionKey": samples[0], "name": "migrated"})
    transport = install_cosmos(monkeypatch, [
        *initialization(container), page(items, '"2"'), caught_up('"2"'),
    ])
    output = await poll(container="legacy", schema=binding.documentSchema)
    documents = [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)]
    assert documents == [
        {**items[0], "_partitionKey": "", "_meta": {"op": "u", "partitionKeyState": "undefined"}},
        *[{**item, "_meta": VALUE} for item in items[1:]],
    ]
    validator = Draft202012Validator(binding.documentSchema)
    for document in documents:
        validator.validate(document)
    assert len({
        (item["_partitionKey"], item["_meta"]["partitionKeyState"], item["id"]) for item in documents
    }) == len(items)
    assert isinstance(output[-1], tuple)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_missing_partition_keys_are_captured_with_defaults(monkeypatch):
    # Cosmos DB accepts items without a partition key value; capture them with
    # non-null defaults and a key state that keeps them apart from real values.
    container = {
        "id": "hierarchical", "_rid": "dGVzdHJpZDA=",
        "partitionKey": {"paths": ["/tenant", "/customer/region"], "kind": "MultiHash", "version": 2},
    }
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": [container]}, {}),
        *sample_replies(container, ["acme"], ["us"]),
    ])
    binding = (await discover()).bindings[0]
    assert binding.key == ["/tenant", "/customer/region", "/_meta/partitionKeyState", "/id"]
    items = [
        {"id": "none"},
        {"id": "parent", "customer": {"name": "kept"}},
        {"id": "present", "tenant": "acme", "customer": {"region": "us"}},
        {"id": "null", "tenant": None, "customer": {"region": None}},
        {"id": "null-parent", "tenant": "acme", "customer": None},
        {"id": "scalar-parent", "tenant": "acme", "customer": "scalar"},
        {"id": "default-value", "tenant": "", "customer": {"region": ""}},
    ]
    transport = install_cosmos(monkeypatch, [
        *initialization(container), page(items, '"4"'), caught_up('"4"'),
    ])
    output = await poll(container="hierarchical", schema=binding.documentSchema)
    documents = [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)]

    def meta(state):
        return {"op": "u", "partitionKeyState": state}

    assert documents == [
        {"id": "none", "tenant": "", "customer": {"region": ""}, "_meta": meta("undefined,undefined")},
        {"id": "parent", "customer": {"name": "kept", "region": ""}, "tenant": "", "_meta": meta("undefined,undefined")},
        {**items[2], "_meta": meta("value,value")},
        {"id": "null", "tenant": "", "customer": {"region": ""}, "_meta": meta("null,null")},
        # A parent that can't hold the key is replaced, as if it were missing.
        {"id": "null-parent", "tenant": "acme", "customer": {"region": ""}, "_meta": meta("value,undefined")},
        {"id": "scalar-parent", "tenant": "acme", "customer": {"region": ""}, "_meta": meta("value,undefined")},
        {**items[6], "_meta": meta("value,value")},
    ]
    validator = Draft202012Validator(binding.documentSchema)
    for document in documents:
        validator.validate(document)
    # Defaults never collide with a real value of the same ID and partition.
    assert len({
        (doc["tenant"], doc["customer"]["region"], doc["_meta"]["partitionKeyState"]) for doc in documents
    }) == 5
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("key_type,default", [
    ("string", ""), (["string", "null"], ""), ("integer", 0), (["integer"], 0), ("boolean", False),
])
async def test_partition_key_defaults_follow_collection_schema(monkeypatch, key_type, default):
    transport = install_cosmos(monkeypatch, [
        *initialization(), page([{"id": "o1"}, {"id": "o2", "customer": None}], '"2"'), caught_up('"2"'),
    ])
    output = await poll(schema={"properties": {"customer": {"type": key_type}}})
    assert [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)] == [
        {"id": "o1", "customer": default, "_meta": {"op": "u", "partitionKeyState": "undefined"}},
        {"id": "o2", "customer": default, "_meta": {"op": "u", "partitionKeyState": "null"}},
    ]
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("key_type,value,found,remedy", [
    ("string", 42, "int", "is of type integer, re-discover and backfill"),
    ("integer", "acme", "str", "is of type string, re-discover and backfill"),
    ("integer", True, "bool", "is of type boolean, re-discover and backfill"),
    ("boolean", 0, "int", "is of type integer, re-discover and backfill"),
    ("boolean", 1.0, "float", "is of type integer, re-discover and backfill"),
    ("string", True, "bool", "is of type boolean, re-discover and backfill"),
    ("integer", 42.5, "float", "cannot be used as a collection key"),
    ("string", 42.5, "float", "cannot be used as a collection key"),
    ("integer", {"a": 1}, "dict", "cannot be used as a collection key"),
    ("string", [1], "list", "cannot be used as a collection key"),
])
async def test_partition_key_of_another_type_fails(monkeypatch, key_type, value, found, remedy):
    transport = install_cosmos(monkeypatch, [*initialization(), page([{"id": "o1", "customer": value}], '"2"')])
    with pytest.raises(RuntimeError, match=(
        f"Item 'o1' has a {found} value at partition key '/customer', "
        f"which does not match the collection key type \\({key_type}\\)\\. .*{remedy}"
    )):
        await poll(schema={"properties": {"customer": {"type": key_type}}})
    assert transport.closed


@pytest.mark.asyncio
async def test_integral_float_matches_integer_key(monkeypatch):
    install_cosmos(monkeypatch, [*initialization(), page([{"id": "o1", "customer": 42.0}], '"2"'), caught_up('"2"')])
    output = await poll(schema={"properties": {"customer": {"type": "integer"}}})
    assert [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)] == [
        {"id": "o1", "customer": 42.0, "_meta": VALUE},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [
    {"type": "object"},
    {"properties": {"customer": {}}},
    {"properties": {"customer": {"type": ["string", "integer"]}}},
    {"properties": {"customer": {"type": "number"}}},
])
async def test_undetermined_partition_key_type_fails(monkeypatch, schema):
    transport = install_cosmos(monkeypatch, initialization()[:2])
    with pytest.raises(RuntimeError, match="Cannot determine the type of partition key '/customer'"):
        await poll(schema=schema)
    assert not feed_headers(transport)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_caught_up_poll_emits_nothing(monkeypatch):
    install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS, '"3"'), caught_up('"3"'),
    ])
    cursor = (await poll())[-1]
    transport = install_cosmos(monkeypatch, [*initialization(), caught_up('"3"')])
    assert await poll(cursor) == []
    assert feed_headers(transport)[0]["if-none-match"] == '"3"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_resumes_legacy_unwrapped_continuation(monkeypatch):
    install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS, '"3"'), caught_up('"3"'),
    ])
    cursor = (await poll())[-1][0]
    assert cursor.startswith(SDK_CONTINUATION_PREFIX)
    legacy_cursor = (cursor[len(SDK_CONTINUATION_PREFIX):],)
    updated = {**ITEMS[1], "status": "shipped"}
    transport = install_cosmos(monkeypatch, [
        *initialization(), page([updated], '"4"'), caught_up('"4"'),
    ])
    output = await poll(legacy_cursor)
    assert [item.model_dump(by_alias=True, exclude_unset=True) for item in output if isinstance(item, Document)] == [
        {**updated, "_meta": VALUE},
    ]
    assert feed_headers(transport)[0]["if-none-match"] == '"3"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_legacy_continuation_resumes_unread_range(monkeypatch):
    from source_cosmosdb_nosql import api

    monkeypatch.setattr(api, "MAX_PAGES_PER_POLL", 1)
    ranges = TWO_RANGES
    transport = install_cosmos(monkeypatch, [*initialization(ranges=ranges), page(ITEMS[:1], '"1"')])
    cursor = (await poll())[-1][0]
    legacy = cursor[len(SDK_CONTINUATION_PREFIX):]
    assert json.loads(base64.b64decode(legacy))["continuation"]["continuation"][0]["token"] is None
    assert not transport.replies
    assert transport.closed
    transport = install_cosmos(monkeypatch, [*initialization(ranges=ranges), page(ITEMS[1:2], '"2"')])
    output = await poll((legacy,))
    assert [item.id for item in output if isinstance(item, Document)] == ["o2"]
    assert output[-1][0].startswith(SDK_CONTINUATION_PREFIX)
    assert feed_headers(transport)[0]["x-ms-documentdb-partitionkeyrangeid"] == "1"
    assert feed_headers(transport)[0].get("if-none-match") is None
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_backfill_reads_all_feed_ranges(monkeypatch):
    ranges = TWO_RANGES
    transport = install_cosmos(monkeypatch, [
        *initialization(ranges=ranges),
        page(ITEMS[:2], '"2"'), page(ITEMS[2:], '"1"'),
        caught_up('"2"'), caught_up('"1"'),
    ])
    output = await poll()
    assert [item.id for item in output if isinstance(item, Document)] == ["o1", "o2", "o3"]
    headers = feed_headers(transport)
    assert [header["x-ms-documentdb-partitionkeyrangeid"] for header in headers] == ["0", "1", "0", "1"]
    assert [header.get("if-none-match") for header in headers] == [None, None, '"2"', '"1"']
    assert not transport.replies
    assert transport.closed

    changed = {**ITEMS[1], "status": "shipped", "canary": "o2-v2"}
    resumed = install_cosmos(monkeypatch, [
        *initialization(ranges=ranges),
        page([changed], '"4"'), caught_up('"1"'), caught_up('"4"'),
    ])
    updates = await poll(output[-1])
    assert [item.model_dump(by_alias=True, exclude_unset=True) for item in updates if isinstance(item, Document)] == [
        {**changed, "_meta": VALUE},
    ]
    headers = feed_headers(resumed)
    assert [header["x-ms-documentdb-partitionkeyrangeid"] for header in headers] == ["0", "1", "0"]
    assert [header["if-none-match"] for header in headers] == ['"2"', '"1"', '"4"']
    assert not resumed.replies
    assert resumed.closed


@pytest.mark.asyncio
async def test_empty_container(monkeypatch):
    transport = install_cosmos(monkeypatch, [*initialization(), caught_up('"0"')])
    output = await poll()
    assert len(output) == 1 and isinstance(output[0], tuple)
    assert output[0][0].startswith(SDK_CONTINUATION_PREFIX)
    assert not transport.replies
    assert transport.closed
    transport = install_cosmos(monkeypatch, [*initialization(), caught_up('"0"')])
    assert await poll(output[0]) == []
    assert feed_headers(transport)[0]["if-none-match"] == '"0"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_busy_container_returns_at_checkpoint_and_resumes(monkeypatch):
    class BusyTransport(FakeTransport):
        pages = 0

        async def send(self, request, **kwargs):
            if request.url.endswith("/docs/"):
                self.pages += 1
                assert self.pages <= 200, "The poll did not return at its page budget"
                self.replies.append(page([{"id": str(self.pages)}], f'"{self.pages}"'))
            return await super().send(request, **kwargs)

    transport = BusyTransport(initialization())
    install_cosmos(monkeypatch, transport)
    async with asyncio.timeout(5):
        output = await poll()
    assert [item.id for item in output if isinstance(item, Document)] == [
        str(number) for number in range(1, 101)
    ]
    assert isinstance(output[-1], tuple)
    assert transport.pages == 100

    transport.replies.extend(initialization())
    resumed = await poll(output[-1])
    assert [item.id for item in resumed if isinstance(item, Document)] == [
        str(number) for number in range(101, 201)
    ]
    assert isinstance(resumed[-1], tuple)
    assert feed_headers(transport)[100]["if-none-match"] == '"100"'
    assert transport.closed


@pytest.mark.asyncio
async def test_recreated_container_requests_backfill(monkeypatch, caplog):
    install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS, '"3"'), caught_up('"3"'),
    ])
    cursor = (await poll())[-1]
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {**ORDERS, "_rid": "bmV3cmlkMDA="}, {}),
    ])
    assert await poll(cursor) == [Triggers.BACKFILL]
    assert "recreated" in caplog.text
    assert not feed_headers(transport)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_now_recreated_container_reads_new_ranges_on_same_client(monkeypatch):
    source = {
        "container": ORDERS, "ranges": RANGES, "etag": '"ranges-1"',
        # A Now start checks the feed once more after its first 304.
        "docs": {"0": deque([page([{"id": "old", "customer": "acme"}], '"1"'), caught_up('"1"'), caught_up('"1"')])},
    }

    class RecreateTransport(FakeTransport):
        async def send(self, request, **kwargs):
            prefix = ENDPOINT + "dbs/shop/colls/orders/"
            if request.url == ENDPOINT:
                reply = (200, {}, {})
            elif request.url == prefix:
                reply = (200, source["container"], {})
            elif request.url == prefix + "pkranges/":
                reply = caught_up(source["etag"]) if request.headers.get("If-None-Match") else (
                    200, {"PartitionKeyRanges": source["ranges"]}, {"etag": source["etag"]}
                )
            else:
                reply = source["docs"][request.headers["x-ms-documentdb-partitionkeyrangeid"]].popleft()
            self.replies = deque([reply])
            return await super().send(request, **kwargs)

    transport = RecreateTransport([])
    install_cosmos(monkeypatch, transport)
    async with resources.CosmosClient(ENDPOINT, credential=KEY, enable_endpoint_discovery=False) as cosmos:
        async def poll_now(cursor):
            return [
                item async for item in fetch_container(
                    cosmos, "shop", "orders", LOG, cursor, schema=ORDERS_SCHEMA, start_time="Now",
                )
            ]

        output = await poll_now(("",))
        assert [item.id for item in output if isinstance(item, Document)] == ["old"]

        # The service rejects the old rid and the SDK refreshes container properties.
        source.update(
            container={**ORDERS, "_rid": "bmV3cmlkMDA="}, etag='"ranges-2"',
            ranges=TWO_RANGES,
            docs={
                "0": deque([page([{"id": "new-a", "customer": "acme"}], '"2"'), caught_up('"2"')]),
                "1": deque([page([{"id": "new-b", "customer": "globex"}], '"3"'), caught_up('"3"')]),
            },
        )
        await cosmos.client_connection._refresh_container_properties_cache("dbs/shop/colls/orders")
        assert await poll_now(output[-1]) == [("beginning",)]

        restarted = len(transport.requests)
        output = await poll_now(("beginning",))
        assert [item.id for item in output if isinstance(item, Document)] == ["new-a", "new-b"]
        requests = list(zip(transport.requests, transport.request_headers))[restarted:]
        assert ENDPOINT + "dbs/shop/colls/orders/pkranges/" in [url for (_, url, _), _ in requests]
        assert [
            headers["x-ms-documentdb-partitionkeyrangeid"]
            for (_, url, _), headers in requests if url.endswith("/docs/")
        ] == ["0", "1", "0", "1"]
        assert not any(source["docs"].values())
    assert transport.closed


@pytest.mark.asyncio
async def test_other_invalid_continuation_errors_propagate(monkeypatch):
    transport = install_cosmos(monkeypatch, initialization()[:2])
    cursor = (base64.b64encode(json.dumps({
        "v": "v2", "startFrom": {"Type": "Beginning"},
        "continuation": {"continuation": [{"token": None}]},
    }).encode()).decode(),)
    with pytest.raises(ValueError, match="Missing containerRid"):
        await poll(cursor)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("feed_error", [False, True])
async def test_binding_reuses_client_and_closes_on_exit(monkeypatch, feed_error):
    transport = install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS[:2], '"2"'), caught_up('"2"'),
        *([(400, {"message": "test feed failure"}, {})] if feed_error else [
            page(ITEMS[2:], '"3"'), caught_up('"3"'),
        ]),
    ], handle_health_checks=True)
    stopping = Task.Stopping()
    polls = 0

    async def fetch_twice(*args, **kwargs):
        nonlocal polls
        assert not transport.closed
        polls += 1
        async for item in fetch_container(*args, **kwargs):
            yield item
        if polls == 2:
            stopping.event.set()

    monkeypatch.setattr(resources, "fetch_container", fetch_twice)
    binding = CaptureBinding[ResourceConfig](
        resourceConfig=ResourceConfig(container="orders"), resourcePath=["orders"], stateKey="orders",
        collection=CollectionSpec(
            name="test/orders", writeSchema=ORDERS_SCHEMA, key=["/customer", "/_meta/partitionKeyState", "/id"],
        ),
    )
    output = io.BytesIO()
    async with asyncio.timeout(5), asyncio.TaskGroup() as tg:
        task = Task(
            log=LOG, connector_status=MagicMock(), name="test-capture", output=output,
            stopping=stopping, tg=tg, transactor=Transactor(output),
            catalog_task_name="test/capture",
        )
        resources.open_container(
            EndpointConfig(**CONFIG, advanced={"disable_endpoint_discovery": True}),
            binding, 0, ResourceState(inc=ResourceState.Incremental(cursor=("",))), task, [],
        )

    assert polls == 2
    assert isinstance(stopping.first_error, CosmosHttpResponseError) if feed_error else stopping.first_error is None
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == (
        ["o1", "o2"] if feed_error else ["o1", "o2", "o3"]
    )
    assert [url for _, url, _ in transport.requests if url != ENDPOINT and not url.endswith("/docs/")] == [
        ENDPOINT + "dbs/shop/colls/orders/",
        ENDPOINT + "dbs/shop/colls/orders/pkranges/",
        ENDPOINT + "dbs/shop/colls/orders/pkranges/",
    ]
    assert feed_headers(transport)[2]["if-none-match"] == '"2"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_feed_error_does_not_checkpoint_failed_page(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS[:2], '"2"'),
        (400, {"message": "test feed failure"}, {}),
    ])
    output = []
    async with resources.CosmosClient(
        CONFIG["endpoint"], credential=KEY, enable_endpoint_discovery=False,
    ) as client:
        with pytest.raises(CosmosHttpResponseError):
            async for item in fetch_container(
                client, CONFIG["database"], "orders", LOG, ("",), schema=ORDERS_SCHEMA,
            ):
                output.append(item)
    assert [type(item) for item in output] == [Document, Document, tuple]
    assert not transport.replies
    assert transport.closed

    resumed = install_cosmos(monkeypatch, [
        *initialization(), page(ITEMS[2:], '"3"'), caught_up('"3"'),
    ])
    remaining = await poll(output[-1])
    assert [item.id for item in remaining if isinstance(item, Document)] == ["o3"]
    assert feed_headers(resumed)[0]["if-none-match"] == '"2"'
    assert not resumed.replies
    assert resumed.closed


@pytest.mark.asyncio
async def test_capture_emits_both_bindings_and_resumes_committed_state(monkeypatch):
    events = {
        "id": "events", "_rid": "dGVzdHJpZDE=",
        "partitionKey": {"paths": ["/id"], "kind": "Hash", "version": 2},
    }
    event_items = [{"id": "e1", "kind": "click"}, {"id": "e2"}]
    reserved = {"id": "reserved", "partitionKey": {"paths": ["/_meta/tenant"]}}
    routes = {
        ENDPOINT + "dbs/shop/colls/": deque([
            (200, {"DocumentCollections": [reserved, ORDERS, events]}, {}),
        ]),
    }
    for container, items in ((ORDERS, ITEMS), (events, event_items)):
        prefix = ENDPOINT + f"dbs/shop/colls/{container['id']}/"
        routes[prefix] = deque([(200, container, {})])
        routes[prefix + "pkranges/"] = deque(initialization(container)[2:])
        routes[prefix + "docs/"] = deque([page(items, '"3"'), caught_up('"3"')])

    transport = install_cosmos(monkeypatch, RoutedTransport(routes))
    capture_spec = CaptureSpec[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", intervalSeconds=0,
        config=EndpointConfig(**CONFIG, advanced={"disable_endpoint_discovery": True}),
        bindings=[
            CaptureBinding[ResourceConfig](
                resourceConfig=ResourceConfig(container=container["id"], interval="PT5S"),
                resourcePath=[container["id"]], stateKey=container["id"],
                collection=CollectionSpec(
                    name=f"test/{container['id']}", writeSchema=ORDERS_SCHEMA,
                    key=["/id"] if container is events else ["/customer", "/_meta/partitionKeyState", "/id"],
                ),
            )
            for container in (events, ORDERS)
        ],
    )

    records = await capture_once(monkeypatch, capture_spec, ConnectorState())
    assert transport.client_options["enable_endpoint_discovery"] is False
    captured = [record["captured"] for record in records if "captured" in record]
    assert len(captured) == 5
    assert [doc["doc"]["id"] for doc in captured if doc["binding"] == 0] == ["e1", "e2"]
    assert [doc["doc"]["id"] for doc in captured if doc["binding"] == 1] == ["o1", "o2", "o3"]
    assert [doc["doc"]["_meta"] for doc in captured] == [{"op": "u"}] * 2 + [VALUE] * 3
    assert all("_self" not in doc["doc"] and "_attachments" not in doc["doc"] for doc in captured)
    state = {}
    for record in records:
        if "checkpoint" in record:
            checkpoint = record["checkpoint"]["state"]
            assert checkpoint["mergePatch"] is True
            json_merge_patch(state, checkpoint["updated"])
    restored = ConnectorState.model_validate_json(json.dumps(state))
    assert set(restored.bindingStateV1) == {"orders", "events"}
    for binding_state in restored.bindingStateV1.values():
        assert binding_state.inc.cursor[0]
    assert all(not replies for replies in routes.values())
    assert transport.closed

    changed = [
        {**ITEMS[1], "status": "shipped", "canary": "o2-v2"},
        {"id": "o4", "customer": "initech", "status": "new", "canary": "o4"},
    ]
    routes[ENDPOINT + "dbs/shop/colls/"] = deque([
        (200, {"DocumentCollections": [reserved, ORDERS, events]}, {}),
    ])
    for container, replies in (
        (ORDERS, [page(changed, '"5"'), caught_up('"5"')]),
        (events, [caught_up('"3"')]),
    ):
        prefix = ENDPOINT + f"dbs/shop/colls/{container['id']}/"
        routes[prefix] = deque([(200, container, {})])
        routes[prefix + "pkranges/"] = deque(initialization(container)[2:])
        routes[prefix + "docs/"] = deque(replies)

    transport = install_cosmos(monkeypatch, RoutedTransport(routes))
    # The CDK advances its input state in place; retain the original checkpoint.
    resumed_records = await capture_once(monkeypatch, capture_spec, restored.model_copy(deep=True))
    assert all(not replies for replies in routes.values())
    assert transport.closed
    resumed = [record["captured"] for record in resumed_records if "captured" in record]
    assert len(resumed) == 2
    assert all(item["binding"] == 1 for item in resumed)
    assert [item["doc"] for item in resumed] == [
        {**item, "_meta": VALUE} for item in changed
    ]
    assert not {"o1", "o3", "e1", "e2"}.intersection(item["doc"]["id"] for item in resumed)
    for name, tokens in (("orders", ['"3"', '"5"']), ("events", ['"3"'])):
        assert [
            headers["if-none-match"]
            for (method, url, _), headers in zip(transport.requests, transport.request_headers)
            if method == "GET" and url.endswith(f"/{name}/docs/")
        ] == tokens

    for record in resumed_records:
        if "checkpoint" in record:
            json_merge_patch(state, record["checkpoint"]["state"]["updated"])
    resumed_state = ConnectorState.model_validate_json(json.dumps(state))
    assert resumed_state.bindingStateV1["events"] == restored.bindingStateV1["events"]
    assert resumed_state.bindingStateV1["orders"].inc.cursor != restored.bindingStateV1["orders"].inc.cursor
