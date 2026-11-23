import asyncio
import json
import shutil
import sqlite3
from logging import getLogger
from pathlib import Path
from typing import Any

import pytest
from azure.cosmos.exceptions import CosmosHttpResponseError
from estuary_cdk.capture import request
from estuary_cdk.flow import CollectionSpec, ValidationError
from jsonschema import Draft202012Validator
from pydantic import ValidationError as ModelValidationError

from source_cosmosdb_nosql import Connector
from source_cosmosdb_nosql.models import EndpointConfig, ResourceConfig

from .fakes import CONFIG, ENDPOINT, install_cosmos, sample_replies


LOG = getLogger(__name__)
ORDERS = {"id": "orders", "partitionKey": {"paths": ["/customer"], "kind": "Hash"}}
EVENTS = {"id": "events", "partitionKey": {"paths": ["/id"], "kind": "Hash"}}
NUMBERS = {"id": "numbers", "partitionKey": {"paths": ["/tenantId"], "kind": "Hash"}}


async def discover(config: dict[str, Any] = CONFIG):
    return await Connector().discover(
        LOG,
        request.Discover[EndpointConfig](connectorType="IMAGE", config=EndpointConfig(**config)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("disable_endpoint_discovery", [False, True])
async def test_discover_paged_containers(monkeypatch, disable_endpoint_discovery):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [ORDERS]}, {"x-ms-continuation": "next-page"}),
        (200, {"DocumentCollections": [EVENTS, NUMBERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
        *sample_replies(NUMBERS, [42]),
    ])
    result = await discover({
        **CONFIG,
        "advanced": {"disable_endpoint_discovery": disable_endpoint_discovery},
    })
    bindings = result.model_dump(mode="json", by_alias=True, exclude_unset=True)["bindings"]
    assert [binding["recommendedName"] for binding in bindings] == ["shop/orders", "shop/events", "shop/numbers"]
    assert [binding["resourceConfig"] for binding in bindings] == [
        {"database": "shop", "container": name} for name in ("orders", "events", "numbers")
    ]
    assert [binding["resourcePath"] for binding in bindings] == [["shop", "orders"], ["shop", "events"], ["shop", "numbers"]]
    assert [binding["key"] for binding in bindings] == [
        ["/customer", "/_meta/partitionKeyState", "/id"], ["/id"], ["/tenantId", "/_meta/partitionKeyState", "/id"],
    ]
    assert bindings == [
        json.loads(line) for line in Path(__file__).with_name("discover.jsonl").read_text().splitlines()
    ]
    for binding in bindings:
        schema = binding["documentSchema"]
        Draft202012Validator.check_schema(schema)
        assert schema["x-infer-schema"] is True
        assert "_meta" in schema["properties"]
        Draft202012Validator(schema).validate({
            "id": "test-item", "customer": "test-customer", "tenantId": 42, "arbitrary": {"nested": 1},
            "_meta": {"op": "u", "partitionKeyState": "value"},
        })
    assert transport.client_options["enable_endpoint_discovery"] is not disable_endpoint_discovery
    assert transport.requests == [
        ("GET", ENDPOINT, None),
        ("GET", ENDPOINT + "dbs/shop/colls/", None),
        ("GET", ENDPOINT + "dbs/shop/colls/", "next-page"),
        ("GET", ENDPOINT + "dbs/shop/colls/orders/", None),
        ("POST", ENDPOINT + "dbs/shop/colls/orders/docs/", None),
        ("GET", ENDPOINT + "dbs/shop/colls/numbers/", None),
        ("POST", ENDPOINT + "dbs/shop/colls/numbers/docs/", None),
    ]
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("paths,expected", [
    (["/tenant", "/customer/region"], ["/tenant", "/customer/region", "/_meta/partitionKeyState", "/id"]),
    (["/tenant", "/id"], ["/tenant", "/id", "/_meta/partitionKeyState"]),
    (["/id", "/region"], ["/id", "/region", "/_meta/partitionKeyState"]),
])
async def test_hierarchical_partition_keys(monkeypatch, paths, expected):
    container = {"id": "hierarchical", "partitionKey": {"paths": paths, "kind": "MultiHash"}}
    install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [container]}, {}),
        *sample_replies(container, *[["test-value"] for path in paths if path != "/id"]),
    ])
    result = await discover()
    assert result.bindings[0].key == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("samples,expected_type", [
    ([42], "integer"), ([42.0], "integer"), ([True], "boolean"), ([False], "boolean"),
    (["acme"], "string"), ([None], "string"), ([], "string"),
])
async def test_sampled_partition_key_types(monkeypatch, samples, expected_type):
    for _ in range(2):
        transport = install_cosmos(monkeypatch, [
            (200, {}, {}), (200, {"DocumentCollections": [NUMBERS]}, {}),
            *sample_replies(NUMBERS, samples),
        ])
        binding = (await discover()).bindings[0]
        assert binding.key == ["/tenantId", "/_meta/partitionKeyState", "/id"]
        assert binding.documentSchema["properties"]["tenantId"]["type"] == [expected_type]
        validator = Draft202012Validator(binding.documentSchema)
        meta = {"op": "u", "partitionKeyState": "value"}
        for value in samples:
            if value is not None:
                validator.validate({"id": "n1", "tenantId": value, "_meta": meta})
        # Key fields are never null, and the connector always writes the state.
        assert not validator.is_valid({"id": "n1", "tenantId": None, "_meta": meta})
        assert not validator.is_valid({"id": "n1", "_meta": meta})
        assert not validator.is_valid({"id": "n1", "tenantId": {"string": "", "integer": 0, "boolean": False}[expected_type]})
        assert not transport.replies
        assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("pointer,key,expression,document", [
    ('/"a/b"/~region', "/a~1b/~0region", 'c["a/b"]["~region"]', {"a/b": {"~region": 42}}),
    ("/'tenant/id'", "/tenant~1id", 'c["tenant/id"]', {"tenant/id": 42}),
    ("/tenant~1id", "/tenant~01id", 'c["tenant~1id"]', {"tenant~1id": 42, "tenant/id": "other"}),
    ('/quote"field/back\\slash', '/quote"field/back\\slash', r'c["quote\"field"]["back\\slash"]', {'quote"field': {"back\\slash": 42}}),
    ('/"_meta/tenant"', "/_meta~1tenant", 'c["_meta/tenant"]', {"_meta/tenant": 42}),
    ("/_metadata/tenant", "/_metadata/tenant", 'c["_metadata"]["tenant"]', {"_metadata": {"tenant": 42}}),
])
async def test_partition_key_sampling_escapes_paths(monkeypatch, pointer, key, expression, document):
    container = {"id": "escaped", "partitionKey": {"paths": [pointer]}}
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": [container]}, {}),
        *sample_replies(container, [42]),
    ])
    binding = (await discover()).bindings[0]
    assert binding.key == [key, "/_meta/partitionKeyState", "/id"]
    Draft202012Validator(binding.documentSchema).validate({
        "id": "x1", **document, "_meta": {"op": "u", "partitionKeyState": "value"},
    })
    assert json.loads(transport.request_bodies[-1]) == {
        "query": f"SELECT TOP 1 VALUE {expression} FROM c WHERE IS_DEFINED({expression}) AND NOT IS_NULL({expression})",
    }
    assert transport.request_headers[-1]["x-ms-documentdb-query-enablecrosspartition"] == "True"
    assert transport.request_headers[-1]["x-ms-documentdb-query-enable-scan"] == "True"
    assert transport.request_headers[-1]["x-ms-max-item-count"] == "1"
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_rejected_partition_key_sample_skips_container(monkeypatch, caplog):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": [NUMBERS, ORDERS]}, {}),
        (200, {"_rid": "dGVzdHJpZDA=", **NUMBERS}, {}),
        (400, {"message": "test query rejected"}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ])
    assert [binding.recommendedName for binding in (await discover()).bindings] == ["shop/orders"]
    assert "Skipping" in caplog.text
    assert "shop/numbers" in caplog.text
    assert "/tenantId" in caplog.text
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_fractional_partition_key_is_skipped(monkeypatch, caplog):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": [NUMBERS]}, {}),
        *sample_replies(NUMBERS, [1.5]),
    ])
    assert (await discover()).bindings == []
    assert "Skipping" in caplog.text
    assert "shop/numbers" in caplog.text
    assert "/tenantId" in caplog.text
    assert "non-integer" in caplog.text
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("pointer", ["/_meta", "/_meta/tenant", "/_meta/op", '/"_meta"/tenant', "/'_meta'"])
async def test_reserved_partition_keys_are_skipped(monkeypatch, caplog, pointer):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [
            {"id": "reserved", "partitionKey": {"paths": [pointer]}}, ORDERS,
        ]}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ])
    assert [binding.recommendedName for binding in (await discover()).bindings] == ["shop/orders"]
    assert "reserved" in caplog.text
    assert "_meta" in caplog.text
    assert "Skipping" in caplog.text
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_discovered_partition_key_schemas(monkeypatch):
    containers = [ORDERS, EVENTS, {
        "id": "hierarchical",
        "partitionKey": {"paths": ["/tenant", "/customer/region", "/customer/code"]},
    }, {
        "id": "escaped", "partitionKey": {"paths": ['/"a/b"/~region']},
    }]
    install_cosmos(monkeypatch, [
        (200, {}, {}), (200, {"DocumentCollections": containers}, {}),
        *sample_replies(ORDERS, ["acme"]),
        *sample_replies(containers[2], ["acme"], [None], ["us"]),
        *sample_replies(containers[3], ["us"]),
    ])
    result = await discover()
    for binding in result.bindings:
        schema = binding.documentSchema
        Draft202012Validator.check_schema(schema)
        assert schema["properties"]["id"]["type"] == "string"
        assert "_meta" in schema["properties"]
        assert schema["additionalProperties"] is True
        for pointer in binding.key:
            node = schema
            for segment in pointer[1:].split("/"):
                field = segment.replace("~1", "/").replace("~0", "~")
                assert field in node["required"]
                node = node["properties"][field]
                if "$ref" in node:
                    node = schema["$defs"][node["$ref"].removeprefix("#/$defs/")]
            # Key fields are never null: missing values are captured with a
            # default, and the key state tells them apart from real values.
            assert node["type"] in (["string"], ["integer"], ["boolean"], "string")
            assert "default" not in node
        assert ("/_meta/partitionKeyState" in binding.key) is (binding.key != ["/id"])

    meta = {"op": "u", "partitionKeyState": "value"}

    validator = Draft202012Validator(result.bindings[0].documentSchema)
    for value in ("acme", ""):
        validator.validate({"id": "o1", "customer": value, "extra": [1, 2], "_meta": meta})
    for document in (
        {"id": "o1", "_meta": meta}, {"id": "o1", "customer": None, "_meta": meta},
        {"id": "o1", "customer": 1, "_meta": meta}, {"id": "o1", "customer": True, "_meta": meta},
        {"id": "o1", "customer": 1.5, "_meta": meta}, {"id": "o1", "customer": "acme"},
        {"id": "o1", "customer": "acme", "_meta": {"op": "u"}},
        {"id": "o1", "customer": "acme", "_meta": {"op": "u", "partitionKeyState": None}},
    ):
        assert not validator.is_valid(document)
    validator = Draft202012Validator(result.bindings[2].documentSchema)
    validator.validate({"id": "h1", "tenant": "acme", "customer": {"region": "", "code": "us"}, "_meta": meta})
    assert not validator.is_valid({"id": "h1", "tenant": "acme", "customer": {"region": None, "code": "us"}, "_meta": meta})
    assert not validator.is_valid({"id": "h1", "tenant": "acme", "customer": {"region": "us"}, "_meta": meta})
    Draft202012Validator(result.bindings[3].documentSchema).validate({
        "id": "x1", "a/b": {"~region": "us"}, "_meta": meta,
    })
    # A container keyed only on /id has no key state.
    assert "partitionKeyState" not in result.bindings[1].documentSchema["$defs"]["Meta"]["properties"]


@pytest.mark.asyncio
async def test_discovered_bindings_build(monkeypatch, tmp_path):
    flowctl = shutil.which("flowctl")
    if flowctl is None:
        pytest.skip("flowctl is required for offline collection builds")
    hierarchical = {"id": "hierarchical", "partitionKey": {"paths": ["/tenant", "/customer/region"]}}
    install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [ORDERS, EVENTS, hierarchical, NUMBERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
        *sample_replies(hierarchical, [42], [True]),
        *sample_replies(NUMBERS, [42]),
    ])
    result = await discover()
    catalog = {"collections": {}}
    for binding in result.bindings:
        # Local $refs must resolve within the schema, not the catalog document.
        schema = tmp_path / f"{binding.resourceConfig.name}.schema.json"
        schema.write_text(json.dumps(binding.documentSchema))
        for inferred in (False, True):
            collection = {"key": binding.key}
            if inferred:
                collection.update({
                    "writeSchema": schema.name,
                    "readSchema": {"allOf": [
                        {"$ref": "flow://write-schema"}, {"$ref": "flow://inferred-schema"},
                    ]},
                })
            else:
                collection["schema"] = schema.name
            catalog["collections"][f"test/{binding.recommendedName}/{inferred}"] = collection

    source = tmp_path / "flow.json"
    source.write_text(json.dumps(catalog))
    database = tmp_path / "build.sqlite"
    process = await asyncio.create_subprocess_exec(
        flowctl, "raw", "build", "--source", str(source), "--db-path", str(database),
        "--build-id", "00:00:00:00:00:00:00:01",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    async with asyncio.timeout(30):
        stdout, stderr = await process.communicate()
    assert database.is_file(), (stdout.decode(), stderr.decode())
    with sqlite3.connect(database) as connection:
        errors = connection.execute("SELECT * FROM errors").fetchall()
    assert not errors, errors
    assert process.returncode == 0, (stdout.decode(), stderr.decode())


@pytest.mark.asyncio
async def test_disable_endpoint_discovery_uses_configured_endpoint(monkeypatch):
    location = {"name": "West US", "databaseAccountEndpoint": "http://unreachable.test/"}
    transport = install_cosmos(monkeypatch, [
        (200, {"readableLocations": [location], "writableLocations": [location]}, {}),
        (200, {"DocumentCollections": [ORDERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ])
    result = await discover({**CONFIG, "advanced": {"disable_endpoint_discovery": True}})
    assert result.bindings[0].recommendedName == "shop/orders"
    assert all(url.startswith(ENDPOINT) for _, url, _ in transport.requests)
    assert transport.closed


@pytest.mark.asyncio
async def test_empty_database(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": []}, {}),
    ])
    assert (await discover()).bindings == []
    assert transport.client_options["enable_endpoint_discovery"] is True
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [
    (401, "rejected the account key"),
    (403, "denied access"),
    (404, "Database 'shop' was not found"),
])
async def test_discovery_config_errors(monkeypatch, status, expected):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (status, {"message": "test rejection"}, {}),
    ])
    with pytest.raises(ValidationError) as exc:
        await discover()
    assert expected in exc.value.errors[0]
    assert transport.closed


@pytest.mark.asyncio
async def test_account_initialization_failure_closes_client(monkeypatch):
    monkeypatch.setenv("AZURE_COSMOS_HEALTH_CHECK_MAX_RETRIES", "1")
    monkeypatch.setenv("AZURE_COSMOS_HEALTH_CHECK_RETRY_AFTER_MS", "0")
    transport = install_cosmos(monkeypatch, [
        (401, {"message": "test rejected account key"}, {}),
        (401, {"message": "test rejected account key"}, {}),
    ])
    with pytest.raises(ValidationError) as exc:
        await discover()
    assert "rejected the account key" in exc.value.errors[0]
    assert transport.requests == [("GET", ENDPOINT, None)] * 2
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_unexpected_sdk_error_propagates(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (400, {"message": "test bad request"}, {}),
    ])
    with pytest.raises(CosmosHttpResponseError) as exc:
        await discover()
    assert exc.value.status_code == 400
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("container", [
    {"id": "orders"},
    {"id": "orders", "partitionKey": {}},
    {"partitionKey": {"paths": ["/customer"]}},
])
async def test_missing_metadata_fails_discovery(monkeypatch, container):
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [container]}, {}),
    ])
    with pytest.raises(ModelValidationError):
        await discover()
    assert transport.closed


@pytest.mark.asyncio
async def test_validate_resolves_containers(monkeypatch):
    install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [ORDERS, EVENTS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ])
    result = await Connector().validate(
        LOG,
        request.Validate[EndpointConfig, ResourceConfig](
            name="test/capture",
            connectorType="IMAGE",
            config=EndpointConfig(**CONFIG),
            bindings=[
                request.ValidateBinding[ResourceConfig](
                    resourceConfig=ResourceConfig(database="shop", container=name, interval="PT5S"),
                    collection=CollectionSpec(
                        name=f"test/{name}",
                        key=["/id"] if name == "events" else ["/customer", "/id"],
                        writeSchema={"type": "object"},
                    ),
                )
                for name in ("events", "orders")
            ],
        ),
    )
    assert [binding.resourcePath for binding in result.bindings] == [["shop", "events"], ["shop", "orders"]]


@pytest.mark.asyncio
async def test_validate_rejects_missing_container(monkeypatch):
    install_cosmos(monkeypatch, [
        (200, {}, {}),
        (200, {"DocumentCollections": [ORDERS]}, {}),
        *sample_replies(ORDERS, ["acme"]),
    ])
    with pytest.raises(ValidationError) as exc:
        await Connector().validate(
            LOG,
            request.Validate[EndpointConfig, ResourceConfig](
                name="test/capture",
                connectorType="IMAGE",
                config=EndpointConfig(**CONFIG),
                bindings=[request.ValidateBinding[ResourceConfig](
                    resourceConfig=ResourceConfig(database="shop", container="missing"),
                    collection=CollectionSpec(
                        name="test/missing", key=["/id"], writeSchema={"type": "object"},
                    ),
                )],
            ),
        )
    assert "'shop.missing' was not found" in exc.value.errors[0]
