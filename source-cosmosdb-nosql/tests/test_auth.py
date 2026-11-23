from logging import getLogger
from urllib.parse import unquote

import pytest
from azure.core.exceptions import ClientAuthenticationError
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec, ValidationError
from jsonschema import Draft202012Validator
from pydantic import ValidationError as ModelValidationError

from source_cosmosdb_nosql.models import ConnectorState, EndpointConfig, ResourceConfig
from source_cosmosdb_nosql.resources import all_resources

from .fakes import capture_once, install_cosmos, install_identity
from .test_capture import ITEMS, ORDERS, caught_up, initialization, page


ENDPOINT = "https://account.documents.azure.com/"
LOG = getLogger(__name__)
SECRET_KWARGS = {"tenant_id": "tenant", "client_id": "client", "client_secret": "secret"}
SERVICE_PRINCIPAL = {"credentials_title": "Service Principal", **SECRET_KWARGS}


@pytest.mark.parametrize("credentials", [
    {"credentials_title": "Managed Identity"},
    {"credentials_title": "Managed Identity", "client_id": "user-assigned"},
    {"credentials_title": "Workload Identity"},
    {"credentials_title": "Workload Identity", "tenant_id": "tenant", "client_id": "client", "token_file_path": "/var/run/test/token"},
])
def test_host_identity_configs_are_rejected(monkeypatch, credentials):
    monkeypatch.setenv("AZURE_TENANT_ID", "host-tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "host-client")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/host/token")
    config = {"endpoint": ENDPOINT, "databases": ["shop"], "credentials": credentials}
    with pytest.raises(ModelValidationError):
        EndpointConfig(**config)
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)


@pytest.mark.asyncio
async def test_entra_discovery_and_capture(monkeypatch):
    config = EndpointConfig(
        endpoint=ENDPOINT, credentials=SERVICE_PRINCIPAL, databases=["shop"],
        advanced={"disable_endpoint_discovery": True},
    )
    container = {**ORDERS, "partitionKey": {"paths": ["/id"]}}
    for capture in (False, True):
        credential, constructor = install_identity(monkeypatch)
        replies = [(200, {}, {}), (200, {"DocumentCollections": [container]}, {})]
        if capture:
            replies.extend([*initialization(container), page(ITEMS[:1], '"1"'), caught_up('"1"')])
        transport = install_cosmos(
            monkeypatch, replies, endpoint=ENDPOINT, credential=credential, handle_health_checks=True,
        )
        if capture:
            spec = CaptureSpec[EndpointConfig, ResourceConfig](
                name="test/capture", connectorType="IMAGE", intervalSeconds=0, config=config,
                bindings=[CaptureBinding[ResourceConfig](
                    resourceConfig=ResourceConfig(database="shop", container="orders"), resourcePath=["shop", "orders"], stateKey="orders",
                    collection=CollectionSpec(name="test/orders", key=["/id"], writeSchema={"type": "object"}),
                )],
            )
            records = await capture_once(monkeypatch, spec, ConnectorState())
            assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == ["o1"]
            assert constructor.call_count == 2
        else:
            assert [resource.name for resource in await all_resources(LOG, config)] == ["shop/orders"]
            constructor.assert_called_once_with(**SECRET_KWARGS)
        for call in constructor.call_args_list:
            assert call.kwargs == SECRET_KWARGS
        assert credential.scopes
        assert all(scopes == (ENDPOINT + ".default",) for scopes in credential.scopes)
        assert all(
            unquote(headers["authorization"]) == "type=aad&ver=1.0&sig=fake-entra-token"
            for headers in transport.request_headers
        )
        assert not transport.replies
        assert transport.closed
        assert credential.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [(401, "Microsoft Entra identity"), (403, "Cosmos DB Built-in Data Reader")])
async def test_entra_authorization_errors_close_resources(monkeypatch, status, expected):
    credential, _ = install_identity(monkeypatch)
    transport = install_cosmos(monkeypatch, [
        (200, {}, {}), (status, {"message": "test access denied"}, {}),
    ], endpoint=ENDPOINT, credential=credential)
    with pytest.raises(ValidationError) as exc:
        await all_resources(LOG, EndpointConfig(
            endpoint=ENDPOINT, databases=["shop"], credentials=SERVICE_PRINCIPAL,
        ))
    assert expected in exc.value.errors[0]
    assert transport.closed
    assert credential.closed


@pytest.mark.asyncio
async def test_entra_token_failure_closes_resources(monkeypatch):
    credential, _ = install_identity(monkeypatch)
    credential.error = ClientAuthenticationError("test sign-in failure")
    transport = install_cosmos(monkeypatch, [], endpoint=ENDPOINT, credential=credential)
    with pytest.raises(ValidationError) as exc:
        await all_resources(LOG, EndpointConfig(
            endpoint=ENDPOINT, databases=["shop"], credentials=SERVICE_PRINCIPAL,
        ))
    assert "Microsoft Entra authentication failed" in exc.value.errors[0]
    assert transport.requests == []
    assert transport.closed
    assert credential.closed
