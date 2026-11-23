import logging
from urllib.parse import parse_qs

import pytest
import pyarrow.fs as pafs
import requests
from estuary_cdk.capture import request
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec, RangeSpec, ValidationError
from pyiceberg.exceptions import ForbiddenError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from source_iceberg import Connector, resources
from source_iceberg.api import IcebergCatalog
from source_iceberg.models import ConnectorState, EndpointConfig, ResourceConfig
from source_iceberg.schema import FALLBACK_KEY

from .conftest import CATALOG_URI


LOG = logging.getLogger(__name__)


async def discover(**kwargs):
    return await Connector().discover(
        LOG,
        request.Discover[EndpointConfig](
            connectorType="IMAGE",
            config=EndpointConfig(catalog_uri=CATALOG_URI, **kwargs),
        ),
    )


@pytest.mark.asyncio
async def test_discover_selected_namespace(shop_catalog):
    discovered = await discover(namespaces=["shop", "shop"])
    bindings = discovered.bindings
    assert [b.recommendedName for b in bindings] == ["shop_customers", "shop_orders"]
    assert [b.resourceConfig.path() for b in bindings] == [["shop", "customers"], ["shop", "orders"]]
    assert bindings[0].key == FALLBACK_KEY
    assert bindings[0].isFallbackKey is True
    assert bindings[1].key == ["/id"]
    assert bindings[1].isFallbackKey is False
    assert bindings[1].documentSchema["properties"]["at"] == {"type": ["string", "null"], "format": "date-time"}
    for binding in bindings:
        assert binding.resourcePath == binding.resourceConfig.path()
        assert "_meta" in binding.documentSchema["required"]
    # Discovery reads REST metadata, including for empty tables, without opening S3 files.
    assert all(call.hostname == "catalog.test" for call in shop_catalog.request_history)
    assert sum(call.path.endswith("/shop/tables") for call in shop_catalog.request_history) == 1


@pytest.mark.asyncio
async def test_discover_all_namespaces_and_pages(catalog_api, register_table):
    catalog_api.get(f"{CATALOG_URI}/v1/namespaces", json={"namespaces": [["shop"]]}, complete_qs=True)
    catalog_api.get(f"{CATALOG_URI}/v1/namespaces?parent=shop", json={"namespaces": [["shop", "archive"]]}, complete_qs=True)
    catalog_api.get(f"{CATALOG_URI}/v1/namespaces?parent=shop%1Farchive", json={"namespaces": []}, complete_qs=True)
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables",
        json={"identifiers": [{"namespace": ["shop"], "name": "orders"}], "next-page-token": "next"},
        complete_qs=True,
    )
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables?pageToken=next",
        json={"identifiers": [{"namespace": ["shop"], "name": "customers"}]},
        complete_qs=True,
    )
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop%1Farchive/tables",
        json={"identifiers": [{"namespace": ["shop", "archive"], "name": "old_orders"}]},
    )
    schema = Schema(NestedField(1, "id", LongType()))
    for namespace, table in [(("shop",), "orders"), (("shop",), "customers"), (("shop", "archive"), "old_orders")]:
        register_table(namespace, table, schema)
    bindings = (await discover()).bindings
    assert {tuple(b.resourceConfig.path()) for b in bindings} == {
        ("shop", "orders"), ("shop", "customers"), ("shop.archive", "old_orders")
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["list_tables", "list_namespaces"])
async def test_discover_all_skips_unreadable_namespaces(shop_catalog, caplog, operation):
    shop_catalog.get(
        f"{CATALOG_URI}/v1/namespaces",
        json={"namespaces": [["finance"], ["dbt_ci"], ["shop"]]}, complete_qs=True,
    )
    shop_catalog.get(
        f"{CATALOG_URI}/v1/namespaces?parent=shop",
        json={"namespaces": []}, complete_qs=True,
    )
    for namespace, status, error_type in [
        ("finance", 403, "ForbiddenException"), ("dbt_ci", 404, "NoSuchNamespaceException"),
    ]:
        tables_url = f"{CATALOG_URI}/v1/namespaces/{namespace}/tables"
        if operation == "list_namespaces":
            shop_catalog.get(tables_url, json={"identifiers": []})
        shop_catalog.get(
            tables_url if operation == "list_tables" else f"{CATALOG_URI}/v1/namespaces?parent={namespace}",
            status_code=status,
            json={"error": {"message": "made-up namespace failure", "type": error_type, "code": status}},
            complete_qs=True,
        )

    bindings = (await discover()).bindings
    assert [binding.resourceConfig.path() for binding in bindings] == [["shop", "customers"], ["shop", "orders"]]
    assert {
        record.args["namespace"] for record in caplog.records if record.levelno == logging.WARNING
    } == {"finance", "dbt_ci"}


@pytest.mark.asyncio
async def test_empty_catalog(catalog_api):
    catalog_api.get(f"{CATALOG_URI}/v1/namespaces", json={"namespaces": []})
    assert (await discover()).bindings == []


@pytest.mark.asyncio
async def test_token_and_warehouse(shop_catalog):
    await discover(
        namespaces=["shop"], warehouse="made-up-warehouse",
        credentials={"auth_type": "Bearer Token", "token": "made-up-token"},
    )
    config_call = shop_catalog.request_history[0]
    assert config_call.qs["warehouse"] == ["made-up-warehouse"]
    assert all(call.headers["Authorization"] == "Bearer made-up-token" for call in shop_catalog.request_history)


@pytest.mark.asyncio
@pytest.mark.parametrize("oauth2_server_uri, scope, endpoint", [
    ("", "", f"{CATALOG_URI}/v1/oauth/tokens"),
    ("v1/oauth/custom", "PRINCIPAL_ROLE:analyst", f"{CATALOG_URI}/v1/oauth/custom"),
    ("https://login.dremio.test/oauth/token", "dremio.all", "https://login.dremio.test/oauth/token"),
])
async def test_client_credential(shop_catalog, oauth2_server_uri, scope, endpoint):
    oauth = shop_catalog.post(
        endpoint,
        json={"access_token": "made-up-token", "token_type": "Bearer", "expires_in": 3600},
    )
    await discover(
        namespaces=["shop"], credentials={
            "auth_type": "OAuth2", "credential": "made-up-client:made-up-secret",
            "scope": scope, "oauth2_server_uri": oauth2_server_uri,
        },
    )
    assert oauth.called
    form = parse_qs(oauth.last_request.text)
    assert form["client_id"] == ["made-up-client"]
    assert form["client_secret"] == ["made-up-secret"]
    assert form["grant_type"] == ["client_credentials"]
    assert form["scope"] == [scope or "catalog"]
    assert all(
        call.headers["Authorization"] == "Bearer made-up-token"
        for call in shop_catalog.request_history if call.method == "GET"
    )


@pytest.mark.asyncio
async def test_s3_overrides_catalog_and_table_properties(shop_catalog, register_table, monkeypatch):
    shop_catalog.get(
        f"{CATALOG_URI}/v1/config",
        json={"defaults": {}, "overrides": {"s3.endpoint": "http://catalog-storage.test", "s3.region": "catalog-region"}},
    )
    register_table(
        ("shop",), "orders", Schema(NestedField(1, "id", LongType())),
        {"s3.endpoint": "http://table-storage.test", "s3.region": "table-region", "s3.request-timeout": "30"},
    )
    loaded = []
    load_table = IcebergCatalog.load_table

    def record_table(catalog, identifier):
        table = load_table(catalog, identifier)
        loaded.append(table)
        return table

    monkeypatch.setattr(IcebergCatalog, "load_table", record_table)
    await discover(namespaces=["shop"], s3={
        "auth_type": "AWSAccessKey",
        "endpoint": "http://storage.test",
        "access_key_id": "made-up-key",
        "secret_access_key": "made-up-secret",
        "path_style_access": True,
    })
    assert len(loaded) == 2
    for table in loaded:
        assert table.io.properties["s3.endpoint"] == "http://storage.test"
        assert table.io.properties["s3.region"] == "us-east-1"
        assert table.io.properties["s3.access-key-id"] == "made-up-key"
        assert table.io.properties["s3.secret-access-key"] == "made-up-secret"
        assert table.io.properties["s3.force-virtual-addressing"] == "false"
    assert loaded[1].io.properties["s3.request-timeout"] == "30"


@pytest.mark.asyncio
async def test_discovery_errors_propagate(catalog_api):
    catalog_api.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables",
        status_code=403,
        json={"error": {"message": "No table access", "type": "ForbiddenException", "code": 403}},
    )
    with pytest.raises(ValidationError, match="No table access") as raised:
        await discover(namespaces=["shop"])
    assert isinstance(raised.value.__cause__, ForbiddenError)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unauthorized", "unreachable"])
async def test_catalog_connection_errors_are_validation_errors(requests_mock, failure):
    if failure == "unauthorized":
        requests_mock.get(
            f"{CATALOG_URI}/v1/config",
            status_code=401,
            json={"error": {"message": "bad token", "type": "NotAuthorizedException", "code": 401}},
        )
    else:
        requests_mock.get(f"{CATALOG_URI}/v1/config", exc=requests.exceptions.ConnectionError("refused"))
    message = "bad token" if failure == "unauthorized" else "refused"
    with pytest.raises(ValidationError, match=f"Could not access Iceberg catalog {CATALOG_URI}: .*{message}"):
        await discover()


@pytest.mark.asyncio
async def test_validate_forbidden_binding_names_table(shop_catalog):
    shop_catalog.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables/orders",
        status_code=403,
        json={"error": {"message": "No table access", "type": "ForbiddenException", "code": 403}},
    )
    with pytest.raises(ValidationError, match="Cannot read Iceberg table shop.orders: .*No table access"):
        await Connector().validate(LOG, request.Validate[EndpointConfig, ResourceConfig](
            name="test/capture", connectorType="IMAGE",
            config=EndpointConfig(catalog_uri=CATALOG_URI, namespaces=["shop"]),
            bindings=[request.ValidateBinding[ResourceConfig](
                resourceConfig=ResourceConfig(namespace="shop", table="orders"),
                collection=CollectionSpec(name="test/orders", key=FALLBACK_KEY, writeSchema={"type": "object"}),
            )],
        ))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["forbidden", "dropped", "reserved", "unsupported"])
async def test_discovery_skips_unreadable_tables(shop_catalog, register_table, monkeypatch, caplog, failure):
    if failure in ("forbidden", "dropped"):
        status = 403 if failure == "forbidden" else 404
        shop_catalog.get(
            f"{CATALOG_URI}/v1/namespaces/shop/tables/customers",
            status_code=status,
            json={"error": {"message": "made-up table failure", "type": "CatalogException", "code": status}},
        )
    else:
        register_table(("shop",), "customers", Schema(NestedField(1, "_meta", StringType())))
        if failure == "unsupported":
            collection_schema = resources.collection_schema

            def unsupported_schema(schema):
                if schema.fields[0].name == "_meta":
                    raise ValueError("Unsupported Iceberg type")
                return collection_schema(schema)

            monkeypatch.setattr(resources, "collection_schema", unsupported_schema)

    bindings = (await discover(namespaces=["shop"])).bindings
    assert [binding.resourceConfig.path() for binding in bindings] == [["shop", "orders"]]
    assert any(
        record.levelno == logging.WARNING and record.args["table"] == "shop.customers"
        for record in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["open", "validate"])
@pytest.mark.parametrize("failure", ["reserved", "dropped"])
async def test_bound_tables_only(shop_catalog, register_table, operation, failure):
    if failure == "reserved":
        register_table(("shop",), "customers", Schema(NestedField(1, "_meta", StringType())))
    else:
        shop_catalog.get(
            f"{CATALOG_URI}/v1/namespaces/shop/tables/customers",
            status_code=404,
            json={"error": {"message": "Table was dropped", "type": "NoSuchTableException", "code": 404}},
        )
    config = EndpointConfig(catalog_uri=CATALOG_URI, namespaces=["shop"])
    resource = ResourceConfig(namespace="shop", table="orders")
    collection = CollectionSpec(name="test/orders", key=["/id"], writeSchema={"type": "object"})
    if operation == "validate":
        validated = await Connector().validate(LOG, request.Validate[EndpointConfig, ResourceConfig](
            name="test/capture", connectorType="IMAGE", config=config,
            bindings=[request.ValidateBinding[ResourceConfig](resourceConfig=resource, collection=collection)],
        ))
        assert validated.bindings[0].resourcePath == ["shop", "orders"]
    else:
        opened, _ = await Connector().open(LOG, request.Open[EndpointConfig, ResourceConfig, ConnectorState](
            capture=CaptureSpec[EndpointConfig, ResourceConfig](
                name="test/capture", connectorType="IMAGE", config=config, intervalSeconds=0,
                bindings=[CaptureBinding[ResourceConfig](
                    resourceConfig=resource, collection=collection,
                    resourcePath=["shop", "orders"], stateKey="shop/orders",
                )],
            ),
            version="test", range=RangeSpec(), state=ConnectorState(),
        ))
        assert opened.explicitAcknowledgements is False
    assert [call.path for call in shop_catalog.request_history] == [
        "/v1/config", "/v1/namespaces/shop/tables/orders",
    ]


@pytest.mark.asyncio
async def test_discovery_skips_literal_dots_in_namespace(catalog_api, register_table, caplog):
    catalog_api.get(f"{CATALOG_URI}/v1/namespaces", json={"namespaces": [["sales.us"], ["sales", "us"]]}, complete_qs=True)
    for namespace, path in [(("sales.us",), "sales.us"), (("sales", "us"), "sales%1Fus")]:
        catalog_api.get(f"{CATALOG_URI}/v1/namespaces?parent={path}", json={"namespaces": []}, complete_qs=True)
        catalog_api.get(
            f"{CATALOG_URI}/v1/namespaces/{path}/tables",
            json={"identifiers": [{"namespace": list(namespace), "name": "orders"}]},
        )
        register_table(namespace, "orders", Schema(NestedField(1, "id", LongType())))

    bindings = (await discover()).bindings
    assert [binding.resourceConfig.path() for binding in bindings] == [["sales.us", "orders"]]
    assert not any("/namespaces/sales.us/tables/orders" in call.path for call in catalog_api.request_history)
    assert any(
        record.levelno == logging.WARNING and "literal dots" in record.message
        and record.args["table"] == "sales.us.orders"
        for record in caplog.records
    )


@pytest.mark.parametrize("static_keys", [False, True])
@pytest.mark.parametrize("credential_source", ["catalog", "table"])
def test_s3_credentials_do_not_mix(catalog_api, monkeypatch, static_keys, credential_source):
    credentials = {
        "s3.access-key-id": "made-up-vended-key",
        "s3.secret-access-key": "made-up-vended-secret",
        "s3.session-token": "made-up-vended-token",
        "client.access-key-id": "made-up-client-key",
        "client.secret-access-key": "made-up-client-secret",
        "client.session-token": "made-up-client-token",
    }
    if credential_source == "catalog":
        catalog_api.get(f"{CATALOG_URI}/v1/config", json={"defaults": {}, "overrides": credentials})
    settings = {"s3.region": "us-east-1"}
    if static_keys:
        settings.update({"s3.access-key-id": "made-up-user-key", "s3.secret-access-key": "made-up-user-secret"})
    clients = []
    monkeypatch.setattr(pafs, "S3FileSystem", lambda **kwargs: clients.append(kwargs))
    with IcebergCatalog("test", settings, uri=CATALOG_URI) as catalog:
        file_io = catalog._load_file_io(credentials if credential_source == "table" else {}, "s3://warehouse/file")
        if static_keys:
            assert file_io.properties["s3.access-key-id"] == "made-up-user-key"
            assert file_io.properties["s3.secret-access-key"] == "made-up-user-secret"
            assert not any(key in file_io.properties for key in (
                "s3.session-token", "client.session-token", "client.access-key-id", "client.secret-access-key",
            ))
        else:
            assert all(file_io.properties[key] == value for key, value in credentials.items())
        file_io.fs_by_scheme("s3", "warehouse")
    assert clients[0]["session_token"] == (None if static_keys else "made-up-vended-token")
    assert credentials["s3.session-token"] == "made-up-vended-token"


@pytest.mark.asyncio
async def test_validate_bindings(shop_catalog):
    shop_catalog.get(
        f"{CATALOG_URI}/v1/namespaces/shop/tables/missing",
        status_code=404,
        json={"error": {"message": "Table not found", "type": "NoSuchTableException", "code": 404}},
    )
    def validation(table):
        return request.Validate[EndpointConfig, ResourceConfig](
            name="test/capture", connectorType="IMAGE",
            config=EndpointConfig(catalog_uri=CATALOG_URI, namespaces=["shop"]),
            bindings=[request.ValidateBinding[ResourceConfig](
                resourceConfig=ResourceConfig(namespace="shop", table=table),
                collection=CollectionSpec(name="test/orders", key=FALLBACK_KEY, writeSchema={"type": "object"}),
            )],
        )

    validated = await Connector().validate(LOG, validation("orders"))
    assert validated.bindings[0].resourcePath == ["shop", "orders"]
    with pytest.raises(ValidationError, match="shop.missing.*not found"):
        await Connector().validate(LOG, validation("missing"))
