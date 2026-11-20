import logging

import pyarrow.fs as pafs
import pytest
import requests_mock
from botocore.awsrequest import AWSRequest
from estuary_cdk.flow import ValidationError
from pyiceberg.catalog.rest.auth import AUTH_MANAGER
from pyiceberg.io.fsspec import FsspecFileIO, S3V4RestSigner
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField
from requests.adapters import HTTPAdapter

from source_iceberg.api import IcebergCatalog
from source_iceberg.models import EndpointConfig
from source_iceberg.resources import _all_resources

from .conftest import CATALOG_URI


LOG = logging.getLogger(__name__)


def test_oauth_client_credential_authenticates_remote_s3_signer(catalog_api, register_table):
    # Guard forwarding the catalog's OAuth manager to FileIO for authenticated remote signing.
    oauth = catalog_api.post(
        f"{CATALOG_URI}/v1/oauth/tokens",
        json={"access_token": "made-up-token", "token_type": "Bearer", "expires_in": 3600},
    )
    register_table(("shop",), "orders", Schema(NestedField(1, "id", LongType())), config={
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO", "s3.signer": "S3V4RestSigner",
    })

    def sign(request, context):
        if request.headers.get("Authorization") != "Bearer made-up-token":
            context.status_code = 401
            return {"error": "Bearer token required"}
        return {"uri": request.json()["uri"], "headers": {"Authorization": ["AWS4 signed"]}}

    signer = catalog_api.post(f"{CATALOG_URI}/v1/aws/s3/sign", json=sign)
    with IcebergCatalog("test", {}, uri=CATALOG_URI, credential="made-up-client:made-up-secret") as catalog:
        table = catalog.load_table(("shop", "orders"))
        assert isinstance(table.io, FsspecFileIO)
        assert "token" not in table.io.properties
        request = AWSRequest(method="GET", url="https://warehouse.s3.amazonaws.com/shop/orders/data.parquet")
        request.context["client_region"] = "us-east-1"
        S3V4RestSigner(table.io.properties)(request)
        assert table.io.properties[AUTH_MANAGER] is catalog._auth_manager
        assert request.headers["Authorization"] == "AWS4 signed"
    assert oauth.called and signer.called
    assert signer.last_request.json()["method"] == "GET"


@pytest.fixture
def signed_transport(monkeypatch):
    transport = requests_mock.Adapter()

    def send(adapter, request, **kwargs):
        # Exercise PyIceberg's real SigV4 adapter before replacing the transport.
        adapter.add_headers(request, **kwargs)
        return transport.send(request, **kwargs)

    monkeypatch.setattr(HTTPAdapter, "send", send)
    return transport


@pytest.mark.parametrize("service", ["glue", "s3tables"])
@pytest.mark.parametrize("auth_type", ["AWS SigV4", "AWSIAM"])
def test_sigv4_catalog_requests(signed_transport, service, auth_type):
    uri = f"https://{service}.us-west-2.amazonaws.com/iceberg"
    signed_transport.register_uri("GET", f"{uri}/v1/config", json={"defaults": {}, "overrides": {}})
    signed_transport.register_uri("GET", f"{uri}/v1/namespaces", json={"namespaces": []})
    auth = {
        "auth_type": auth_type, "aws_region": "us-west-2", "signing_name": service,
        "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
        "aws_session_token": "made-up-session",
    }
    if auth_type == "AWSIAM":
        auth["aws_role_arn"] = "arn:aws:iam::123456789012:role/iceberg-reader"
    config = EndpointConfig(
        catalog_uri=uri, warehouse="arn:aws:s3tables:us-west-2:123456789012:bucket/warehouse",
        catalog_auth={"auth_type": "AWSIAM", "signing_name": service} if auth_type == "AWSIAM" else auth,
        credentials=auth if auth_type == "AWSIAM" else None,
    )
    assert _all_resources(LOG, config) == []
    assert len(signed_transport.request_history) == 2
    for call in signed_transport.request_history:
        assert call.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=made-up-key/")
        assert f"/us-west-2/{service}/aws4_request" in call.headers["Authorization"]
        assert "Signature=" in call.headers["Authorization"]
        assert call.headers["X-Amz-Security-Token"] == "made-up-session"
        assert call.headers["X-Amz-Date"]
        assert call.headers["x-amz-content-sha256"]
    assert signed_transport.request_history[0].qs["warehouse"] == [config.warehouse]
    assert "made-up-secret" not in repr(config)
    assert "made-up-session" not in repr(config)


@pytest.mark.parametrize("field", ["catalog_auth", "s3"])
@pytest.mark.parametrize("tokens", [{}, {"aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret"}])
def test_iam_requires_runtime_session(field, tokens):
    config = EndpointConfig.model_validate({
        "catalog_uri": CATALOG_URI,
        field: {"auth_type": "AWSIAM"},
        "credentials": {
            "auth_type": "AWSIAM", "aws_region": "us-east-1",
            "aws_role_arn": "arn:aws:iam::123456789012:role/reader", **tokens,
        },
    })
    with pytest.raises(ValidationError, match="Missing AWS IAM session credentials"):
        _all_resources(LOG, config)


@pytest.mark.parametrize("auth_type", ["AWSAccessKey", "AWSIAM"])
def test_s3_session_credentials_override_catalog(monkeypatch, shop_catalog, auth_type):
    properties = []
    clients = []
    load_table = IcebergCatalog.load_table

    def record_table(catalog, identifier):
        table = load_table(catalog, identifier)
        properties.append(table.io.properties)
        table.io.fs_by_scheme("s3", "warehouse")
        return table

    monkeypatch.setattr(IcebergCatalog, "load_table", record_table)
    monkeypatch.setattr(pafs, "S3FileSystem", lambda **kwargs: clients.append(kwargs))
    shop_catalog.get(f"{CATALOG_URI}/v1/config", json={"defaults": {}, "overrides": {
        "s3.access-key-id": "vended-key", "s3.secret-access-key": "vended-secret",
        "s3.session-token": "vended-token", "client.session-token": "catalog-token",
    }})
    s3 = {
        "auth_type": "AWSIAM", "aws_role_arn": "arn:aws:iam::123456789012:role/storage-reader",
        "aws_region": "us-west-2", "aws_access_key_id": "storage-key",
        "aws_secret_access_key": "storage-secret", "aws_session_token": "storage-token",
    } if auth_type == "AWSIAM" else {
        "access_key_id": "storage-key", "secret_access_key": "storage-secret",
        "session_token": "storage-token", "region": "us-west-2",
    }
    config = EndpointConfig(
        catalog_uri=CATALOG_URI, namespaces=["shop"],
        s3={"auth_type": "AWSIAM"} if auth_type == "AWSIAM" else s3,
        credentials=s3 if auth_type == "AWSIAM" else None,
    )
    assert len(_all_resources(LOG, config)) == 2
    assert len(properties) == len(clients) == 2
    for client in clients:
        assert client["region"] == "us-west-2"
        assert client["access_key"] == "storage-key"
        assert client["secret_key"] == "storage-secret"
        assert client["session_token"] == "storage-token"
        assert "role_arn" not in client
    assert all("client.session-token" not in props for props in properties)


@pytest.mark.parametrize("storage", ["catalog", "vended", "explicit_iam", "shared_iam"])
def test_catalog_and_storage_identities_stay_separate(monkeypatch, signed_transport, storage):
    uri = "https://glue.us-east-1.amazonaws.com/iceberg"
    signed_transport.register_uri("GET", f"{uri}/v1/config", json={"defaults": {}, "overrides": {}})
    signed_transport.register_uri("GET", f"{uri}/v1/namespaces/shop/tables", json={"identifiers": [{"namespace": ["shop"], "name": "orders"}]})
    metadata = new_table_metadata(
        Schema(NestedField(1, "id", LongType())), UNPARTITIONED_PARTITION_SPEC,
        UNSORTED_SORT_ORDER, "s3://warehouse/shop/orders",
    )
    signed_transport.register_uri("GET", f"{uri}/v1/namespaces/shop/tables/orders", json={
        "metadata-location": "s3://warehouse/shop/orders/metadata/0.json",
        "metadata": metadata.model_dump(mode="json", by_alias=True),
        "config": {} if storage == "catalog" else {"s3.access-key-id": "vended-key", "s3.secret-access-key": "vended-secret"},
    })
    clients = []
    load_table = IcebergCatalog.load_table

    def record_table(catalog, identifier):
        table = load_table(catalog, identifier)
        table.io.fs_by_scheme("s3", "warehouse")
        return table

    monkeypatch.setattr(IcebergCatalog, "load_table", record_table)
    monkeypatch.setattr(pafs, "S3FileSystem", lambda **kwargs: clients.append(kwargs))
    auth = {
        "auth_type": "AWSIAM", "aws_region": "us-east-1",
        "aws_role_arn": "arn:aws:iam::123456789012:role/catalog-reader",
        "aws_access_key_id": "catalog-key", "aws_secret_access_key": "catalog-secret", "aws_session_token": "catalog-token",
    }
    credentials = {
        **auth, "aws_role_arn": "arn:aws:iam::123456789012:role/storage-reader",
        "aws_access_key_id": "storage-key", "aws_secret_access_key": "storage-secret", "aws_session_token": "storage-token",
    } if storage == "explicit_iam" else auth
    config = EndpointConfig(
        catalog_uri=uri, namespaces=["shop"], credentials=credentials,
        catalog_auth={**auth, "auth_type": "AWS SigV4"} if storage == "explicit_iam" else {"auth_type": "AWSIAM"},
        s3={"auth_type": "AWSIAM"} if storage in ("explicit_iam", "shared_iam") else None,
    )
    assert len(_all_resources(LOG, config)) == 1
    identity = {"catalog": "catalog", "vended": "vended", "explicit_iam": "storage", "shared_iam": "catalog"}[storage]
    assert len(clients) == 1
    assert clients[0]["access_key"] == f"{identity}-key"
    assert clients[0]["secret_key"] == f"{identity}-secret"
    assert clients[0]["session_token"] == (None if storage == "vended" else f"{identity}-token")
    assert all("Credential=catalog-key/" in call.headers["Authorization"] for call in signed_transport.request_history)
