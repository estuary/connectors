import json
from collections import deque
from logging import getLogger
from pathlib import Path

import pytest
from azure.cosmos.aio import CosmosClient
from estuary_cdk.capture import request
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec, ValidationError
from estuary_cdk.utils import json_merge_patch

from source_cosmosdb_nosql import Connector, resources
from source_cosmosdb_nosql.models import ConnectorState, EndpointConfig, ResourceConfig

from .fakes import CONFIG, ENDPOINT, RoutedTransport, capture_once, feed_headers, install_cosmos, sample_replies
from .test_capture import ORDERS, caught_up, initialization, page
from .test_discover import NUMBERS, discover


LOG = getLogger(__name__)
# Validate and Open list databases and containers but never sample partition keys.
OPEN_LISTING = [
    (200, {}, {}),
    (200, {"Databases": [{"id": "analytics"}, {"id": "shop"}]}, {}),
    (200, {"DocumentCollections": [NUMBERS]}, {}),
    (200, {"DocumentCollections": [ORDERS]}, {}),
]


def endpoint_config(all_databases=False):
    return {
        "endpoint": CONFIG["endpoint"], "credentials": CONFIG["credentials"],
        **({"all_databases": True} if all_databases else {"databases": ["shop", "billing"]}),
        "advanced": {"disable_endpoint_discovery": True},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("all_databases", [False, True])
async def test_discover_multiple_databases(monkeypatch, all_databases):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        *([
            (200, {"Databases": [{"id": "shop"}]}, {"x-ms-continuation": "next-db"}),
            (200, {"Databases": [{"id": "billing"}, {"id": "empty"}]}, {}),
        ] if all_databases else []),
        (200, {"DocumentCollections": [ORDERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
        (200, {"DocumentCollections": [ORDERS]}, {}),
        *sample_replies(ORDERS, [42]),
        *([(200, {"DocumentCollections": []}, {})] if all_databases else []),
    ])
    result = await discover(endpoint_config(all_databases))
    bindings = result.model_dump(mode="json", by_alias=True, exclude_unset=True)["bindings"]
    assert [binding["recommendedName"] for binding in bindings] == ["shop/orders", "billing/orders"]
    assert [binding["resourceConfig"] for binding in bindings] == [
        {"database": database, "container": "orders"} for database in ("shop", "billing")
    ]
    assert [binding["resourcePath"] for binding in bindings] == [["shop", "orders"], ["billing", "orders"]]
    assert bindings == [
        json.loads(line) for line in Path(__file__).with_name("discover-multiple.jsonl").read_text().splitlines()
    ]
    assert [binding["documentSchema"]["properties"]["customer"]["type"] for binding in bindings] == [
        ["string"], ["integer"],
    ]
    assert transport.requests == [
        ("GET", ENDPOINT, None),
        *([("GET", ENDPOINT + "dbs", None), ("GET", ENDPOINT + "dbs", "next-db")] if all_databases else []),
        *[
            (method, ENDPOINT + f"dbs/{database}/colls/{suffix}", None)
            for database in ("shop", "billing")
            for method, suffix in (("GET", ""), ("GET", "orders/"), ("POST", "orders/docs/"))
        ],
        *([("GET", ENDPOINT + "dbs/empty/colls/", None)] if all_databases else []),
    ]
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_no_databases(monkeypatch):
    transport = install_cosmos(monkeypatch, [(200, {}, {}), (200, {"Databases": []}, {})])
    assert (await discover(endpoint_config(True))).bindings == []
    assert transport.closed


@pytest.mark.asyncio
async def test_all_databases_skips_fractional_keys_in_unbound_container(monkeypatch, caplog):
    config = EndpointConfig(**endpoint_config(True))
    discovery = [
        (200, {}, {}),
        (200, {"Databases": [{"id": "analytics"}, {"id": "shop"}]}, {}),
        (200, {"DocumentCollections": [NUMBERS]}, {}),
        *sample_replies(NUMBERS, [2.5]),
        (200, {"DocumentCollections": [ORDERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ]
    transport = install_cosmos(monkeypatch, discovery)
    bindings = (await discover(endpoint_config(True))).bindings
    assert [binding.recommendedName for binding in bindings] == ["shop/orders"]
    assert "analytics/numbers" in caplog.text and "/tenantId" in caplog.text
    assert "Skipping" in caplog.text and "non-integer" in caplog.text
    assert not transport.replies
    assert transport.closed

    spec = CaptureSpec[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", intervalSeconds=0, config=config,
        bindings=[CaptureBinding[ResourceConfig](
            resourceConfig=bindings[0].resourceConfig, resourcePath=["shop", "orders"], stateKey="orders",
            collection=CollectionSpec(name="test/orders", key=bindings[0].key, writeSchema=bindings[0].documentSchema),
        )],
    )
    transport = install_cosmos(monkeypatch, [
        *OPEN_LISTING, *initialization(), page([{"id": "o1", "customer": "acme"}], '"1"'), caught_up('"1"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, spec, ConnectorState())
    assert [record["captured"]["doc"] for record in records if "captured" in record] == [
        {"id": "o1", "customer": "acme", "_meta": {"op": "u", "partitionKeyState": "value"}},
    ]
    assert all(method == "GET" for method, _, _ in transport.requests)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_open_keeps_bound_container_whose_sample_changed(monkeypatch):
    # Discovery chose an integer key. If a later sample would return a fractional
    # value, the bound container must still resolve, so Open and Validate never sample.
    config = EndpointConfig(**endpoint_config(True))
    # A sampling query would find no scripted reply and fail the test.
    transport = install_cosmos(monkeypatch, OPEN_LISTING)
    validate = request.Validate[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", config=config,
        bindings=[request.ValidateBinding[ResourceConfig](
            resourceConfig=ResourceConfig(database="analytics", container="numbers"),
            collection=CollectionSpec(name="test/numbers", key=["/tenantId", "/id"], writeSchema={"type": "object"}),
        )],
    )
    result = await Connector().validate(LOG, validate)
    assert result.bindings[0].resourcePath == ["analytics", "numbers"]
    assert all(method == "GET" for method, _, _ in transport.requests)

    spec = CaptureSpec[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", intervalSeconds=0, config=config,
        bindings=[CaptureBinding[ResourceConfig](
            resourceConfig=ResourceConfig(database="analytics", container="numbers"),
            resourcePath=["analytics", "numbers"], stateKey="numbers",
            collection=CollectionSpec(
                name="test/numbers", key=["/tenantId", "/_meta/partitionKeyState", "/id"],
                writeSchema={"properties": {"tenantId": {"type": ["integer"]}}},
            ),
        )],
    )
    numbers = {**NUMBERS, "_rid": "dGVzdHJpZDA="}
    transport = install_cosmos(monkeypatch, [
        *OPEN_LISTING, *initialization(numbers), page([{"id": "n1", "tenantId": 7}], '"1"'), caught_up('"1"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, spec, ConnectorState())
    assert [record["captured"]["doc"] for record in records if "captured" in record] == [
        {"id": "n1", "tenantId": 7, "_meta": {"op": "u", "partitionKeyState": "value"}},
    ]
    assert all(method == "GET" for method, _, _ in transport.requests)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_selected_database_error_names_database(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": []}, {}), (404, {"message": "missing"}, {}),
    ])
    with pytest.raises(ValidationError) as exc:
        await discover(endpoint_config())
    assert "Database 'billing' was not found" in exc.value.errors[0]
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("database", ["", "shop", "billing", "missing"])
async def test_validate_qualified_bindings(monkeypatch, database):
    container = {**ORDERS, "partitionKey": {"paths": ["/id"]}}
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [container]}, {}),
        (200, {"DocumentCollections": [container]}, {}),
    ])
    validate = request.Validate[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", config=EndpointConfig(**endpoint_config()),
        bindings=[request.ValidateBinding[ResourceConfig](
            resourceConfig=ResourceConfig(database=database or None, container="orders"),
            collection=CollectionSpec(name="test/orders", key=["/id"], writeSchema={"type": "object"}),
        )],
    )
    if database in ("shop", "billing"):
        result = await Connector().validate(LOG, validate)
        assert result.bindings[0].resourcePath == [database, "orders"]
    else:
        with pytest.raises(ValidationError):
            await Connector().validate(LOG, validate)
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("all_databases", [False, True])
async def test_multi_database_capture_resumes_independent_state(monkeypatch, all_databases):
    containers = [
        {**ORDERS, "partitionKey": {"paths": ["/id"]}},
        {**ORDERS, "_rid": "dGVzdHJpZDE=", "partitionKey": {"paths": ["/id"]}},
    ]
    spec = CaptureSpec[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", intervalSeconds=0,
        config=EndpointConfig(**endpoint_config(all_databases)),
        bindings=[CaptureBinding[ResourceConfig](
            resourceConfig=ResourceConfig(database=database, container="orders"),
            resourcePath=[database, "orders"], stateKey=f"{database}-orders",
            collection=CollectionSpec(name=f"test/{database}/orders", key=["/id"], writeSchema={"type": "object"}),
        ) for database in ("shop", "billing")],
    )
    state = {}
    routes = {}
    transports = []

    def client(endpoint, **kwargs):
        transport = RoutedTransport(routes)
        transports.append(transport)
        return CosmosClient(endpoint, transport=transport, retry_total=0, **kwargs)

    monkeypatch.setattr(resources, "CosmosClient", client)
    for resumed in (False, True):
        routes.clear()
        transports.clear()
        if all_databases:
            routes[ENDPOINT + "dbs"] = deque([(
                200, {"Databases": [{"id": database} for database in ("billing", "shop")]}, {},
            )])
        for database, container in zip(("shop", "billing"), containers):
            prefix = ENDPOINT + f"dbs/{database}/colls/"
            routes[prefix] = deque([(200, {"DocumentCollections": [container]}, {})])
            routes[prefix + "orders/"] = deque([(200, container, {})])
            routes[prefix + "orders/pkranges/"] = deque(initialization(container)[2:])
            replies = [page([{"id": "same", "database": database, "version": 1}], '"1"'), caught_up('"1"')]
            if resumed:
                replies = [caught_up('"1"')] if database == "shop" else [
                    page([{"id": "same", "database": "billing", "version": 2}, {"id": "new"}], '"3"'),
                    caught_up('"3"'),
                ]
            routes[prefix + "orders/docs/"] = deque(replies)

        before = ConnectorState.model_validate_json(json.dumps(state))
        records = await capture_once(monkeypatch, spec, before.model_copy(deep=True))
        captured = [record["captured"] for record in records if "captured" in record]
        if resumed:
            assert len(captured) == 2
            assert all(item["binding"] == 1 for item in captured)
            assert [item["doc"] for item in captured] == [
                {"id": "same", "database": "billing", "version": 2, "_meta": {"op": "u"}},
                {"id": "new", "_meta": {"op": "u"}},
            ]
        else:
            assert sorted(captured, key=lambda item: item["binding"]) == [
                {"binding": index, "doc": {"id": "same", "database": database, "version": 1, "_meta": {"op": "u"}}}
                for index, database in enumerate(("shop", "billing"))
            ]
        for record in records:
            if "checkpoint" in record:
                json_merge_patch(state, record["checkpoint"]["state"]["updated"])
        after = ConnectorState.model_validate_json(json.dumps(state))
        assert set(after.bindingStateV1) == {"shop-orders", "billing-orders"}
        assert after.bindingStateV1["shop-orders"].inc.cursor != after.bindingStateV1["billing-orders"].inc.cursor
        if resumed:
            assert after.bindingStateV1["shop-orders"] == before.bindingStateV1["shop-orders"]
            assert after.bindingStateV1["billing-orders"] != before.bindingStateV1["billing-orders"]
            for transport in transports[1:]:
                assert feed_headers(transport)[0]["if-none-match"] == '"1"'
        assert all(not replies for replies in routes.values())
        assert all(transport.closed for transport in transports)
