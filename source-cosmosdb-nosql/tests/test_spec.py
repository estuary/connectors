import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from estuary_cdk.capture import request
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from source_cosmosdb_nosql.models import (
    AccountKeyCredentials, EndpointConfig, ResourceConfig, ServicePrincipalCredentials,
)

from .fakes import CONFIG, KEY
from .test_auth import SERVICE_PRINCIPAL


def test_spec_protocol():
    result = subprocess.run(
        [sys.executable, "-m", "source_cosmosdb_nosql"],
        input='{"spec":{}}\n',
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 1
    spec = json.loads(lines[0])["spec"]
    assert spec["protocol"] == 3032023
    assert spec["resourcePathPointers"] == ["/database", "/container"]
    assert spec["documentationUrl"] == "https://go.estuary.dev/source-cosmosdb-nosql"

    endpoint = spec["configSchema"]
    Draft202012Validator.check_schema(endpoint)
    assert set(endpoint["required"]) == {"endpoint", "credentials"}
    assert not {"allOf", "oneOf", "anyOf", "if"}.intersection(endpoint)
    credentials = endpoint["properties"]["credentials"]
    assert set(endpoint["properties"]) == {"endpoint", "credentials", "databases", "advanced"}
    assert endpoint["properties"]["databases"]["default"] == []
    assert endpoint["properties"]["advanced"]["title"] == "Advanced Config"
    assert endpoint["properties"]["advanced"]["advanced"] is True
    assert credentials["oneOf"] == [
        {"$ref": "#/$defs/AccountKeyCredentials"}, {"$ref": "#/$defs/ServicePrincipalCredentials"},
    ]
    assert credentials["discriminator"]["propertyName"] == "credentials_title"
    for name in ("AccountKeyCredentials", "ServicePrincipalCredentials"):
        method = endpoint["$defs"][name]["properties"]["credentials_title"]
        assert method["title"] == "Authentication Method"
        assert method["const"] == method["default"]
    assert endpoint["$defs"]["AccountKeyCredentials"]["properties"]["key"]["secret"] is True
    assert endpoint["$defs"]["Advanced"]["properties"]["disable_endpoint_discovery"]["default"] is False
    Draft202012Validator(endpoint).validate(CONFIG)
    Draft202012Validator(endpoint).validate({**CONFIG, "advanced": {"disable_endpoint_discovery": True}})

    resource = spec["resourceConfigSchema"]
    Draft202012Validator.check_schema(resource)
    assert set(resource["required"]) == {"database", "container"}
    assert "name" not in resource["properties"]
    assert resource["properties"]["interval"]["default"] == "PT1M"
    Draft202012Validator(resource).validate({"database": "shop", "container": "orders", "interval": "PT5S"})
    assert spec == json.loads(Path(__file__).with_name("spec.json").read_text())


@pytest.mark.parametrize("field", ["endpoint", "credentials"])
def test_endpoint_requires_fields(field):
    config = {name: value for name, value in CONFIG.items() if name != field}
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)
    with pytest.raises(ValidationError):
        request.Validate[EndpointConfig, ResourceConfig].model_validate({
            "name": "test/capture", "connectorType": "IMAGE", "config": config, "bindings": [],
        })
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)


def test_resource_interval_and_path():
    default = ResourceConfig(database="shop", container="orders")
    assert default.interval == timedelta(minutes=1)
    resource = ResourceConfig.model_validate({"database": "shop", "container": "events", "interval": "PT5S"})
    assert resource.path() == ["shop", "events"]
    assert resource.interval == timedelta(seconds=5)
    assert resource.model_dump(mode="json", by_alias=True, exclude_unset=True) == {
        "database": "shop", "container": "events", "interval": "PT5S",
    }


@pytest.mark.parametrize("interval", ["PT0S", "-PT1M", "invalid"])
def test_invalid_poll_interval(interval):
    with pytest.raises(ValidationError):
        ResourceConfig.model_validate({"database": "shop", "container": "orders", "interval": interval})


@pytest.mark.parametrize("selection", [{}, {"databases": []}, {"databases": ["shop"]}, {"databases": ["shop", "billing"]}])
def test_optional_auth_and_database_selections(selection):
    config = {"endpoint": CONFIG["endpoint"], "credentials": SERVICE_PRINCIPAL, **selection}
    parsed = EndpointConfig.model_validate(config)
    assert isinstance(parsed.credentials, ServicePrincipalCredentials)
    validator = Draft202012Validator(EndpointConfig.model_json_schema())
    validator.validate(config)
    validator.validate(parsed.model_dump(mode="json"))
    assert EndpointConfig.model_validate(parsed.model_dump(mode="json")) == parsed


@pytest.mark.parametrize("changes", [
    {"credentials": None}, {"credentials": {"credentials_title": "Account Key", "key": ""}},
    {"credentials": {"credentials_title": "Account Key"}}, {"credentials": None, "key": KEY},
    {"credentials": {"credentials_title": "Service Principal", "tenant_id": "tenant", "client_id": "client"}},
    {"databases": [""]}, {"databases": ["shop", "shop"]},
    {"databases": "shop"}, {"databases": None},
])
def test_invalid_auth_and_database_selections(changes):
    config = {**CONFIG, **changes}
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)
    with pytest.raises(ValidationError):
        request.Validate[EndpointConfig, ResourceConfig].model_validate({
            "name": "test/capture", "connectorType": "IMAGE", "config": config, "bindings": [],
        })


@pytest.mark.parametrize("advanced", [
    {"priority": "Medium"}, {"throughput_bucket": 0}, {"throughput_bucket": 6},
    {"max_item_count": 0}, {"max_item_count": -1}, {"max_item_count": 1.5},
    {"start_time": "Yesterday"}, {"start_time": "2026-11-11T12:00:00"},
    {"start_time": 1794398400}, {"start_time": "1794398400"},
])
def test_invalid_advanced_options(advanced):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate({**CONFIG, "advanced": advanced})


def test_defaults_and_secret_annotations():
    config = EndpointConfig(**CONFIG)
    assert config.credentials == AccountKeyCredentials(key=KEY)
    assert config.advanced.start_time == "Beginning"
    assert config.advanced.priority is None
    assert config.advanced.max_item_count is None
    assert config.advanced.throughput_bucket is None
    assert config.databases == ["shop"]
    assert EndpointConfig(endpoint=CONFIG["endpoint"], credentials=CONFIG["credentials"]).databases == []
    schema = EndpointConfig.model_json_schema()
    secret = schema["$defs"]["ServicePrincipalCredentials"]["properties"]["client_secret"]
    assert secret["secret"] is True and secret["type"] == "string"
    assert KEY not in repr(config)


def test_multi_database_resource_path():
    resource = ResourceConfig(database="shop", container="orders")
    assert resource.path() == ["shop", "orders"]
    assert resource.model_dump(mode="json", by_alias=True, exclude_unset=True) == {
        "database": "shop", "container": "orders",
    }


@pytest.mark.parametrize("resource", [
    {"container": "orders"}, {"container": "orders", "database": None},
    {"container": "orders", "database": ""},
])
def test_resource_requires_database(resource: dict[str, str | None]):
    with pytest.raises(ValidationError):
        ResourceConfig.model_validate(resource)
    assert not Draft202012Validator(ResourceConfig.model_json_schema()).is_valid(resource)
