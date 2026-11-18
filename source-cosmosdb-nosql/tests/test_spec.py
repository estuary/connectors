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
    assert credentials["oneOf"] == [
        {"$ref": "#/$defs/AccountKeyCredentials"}, {"$ref": "#/$defs/ServicePrincipalCredentials"},
    ]
    assert credentials["discriminator"]["propertyName"] == "credentials_title"
    assert endpoint["$defs"]["AccountKeyCredentials"]["properties"]["key"]["secret"] is True
    assert endpoint["$defs"]["Advanced"]["properties"]["disable_endpoint_discovery"]["default"] is False
    Draft202012Validator(endpoint).validate(CONFIG)
    Draft202012Validator(endpoint).validate({**CONFIG, "advanced": {"disable_endpoint_discovery": True}})

    resource = spec["resourceConfigSchema"]
    Draft202012Validator.check_schema(resource)
    assert resource["required"] == ["container"]
    assert "name" not in resource["properties"]
    assert resource["properties"]["interval"]["default"] == "PT1M"
    Draft202012Validator(resource).validate({"container": "orders", "interval": "PT5S"})
    assert spec == json.loads(Path(__file__).with_name("spec.json").read_text())


@pytest.mark.parametrize("field", ["endpoint", "credentials", "database"])
def test_endpoint_requires_fields(field):
    config = {name: value for name, value in CONFIG.items() if name != field}
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)
    with pytest.raises(ValidationError):
        request.Validate[EndpointConfig, ResourceConfig].model_validate({
            "name": "test/capture", "connectorType": "IMAGE", "config": config, "bindings": [],
        })
    if field != "database":
        assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)


def test_resource_interval_and_path():
    default = ResourceConfig(container="orders")
    assert default.interval == timedelta(minutes=1)
    resource = ResourceConfig.model_validate({"container": "events", "interval": "PT5S"})
    assert resource.path() == ["events"]
    assert resource.interval == timedelta(seconds=5)
    assert resource.model_dump(mode="json", by_alias=True, exclude_unset=True) == {
        "container": "events", "interval": "PT5S",
    }


@pytest.mark.parametrize("interval", ["PT0S", "-PT1M", "invalid"])
def test_invalid_poll_interval(interval):
    with pytest.raises(ValidationError):
        ResourceConfig.model_validate({"container": "orders", "interval": interval})


@pytest.mark.parametrize("selection", [{"database": "shop"}, {"databases": ["shop", "billing"]}, {"all_databases": True}])
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
    {"database": ""}, {"databases": ["another"]}, {"all_databases": True},
    {"database": "", "databases": [""]}, {"database": "", "databases": ["shop", "shop"]},
    {"database": "", "databases": ["shop"], "all_databases": True},
])
def test_invalid_auth_and_database_selections(changes):
    config = {**CONFIG, **changes}
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)
    # Cross-field selections are enforced by Validate, without root combinators.
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


def test_legacy_defaults_and_secret_annotations():
    config = EndpointConfig(**CONFIG)
    assert config.credentials == AccountKeyCredentials(key=KEY)
    assert config.advanced.start_time == "Beginning"
    assert config.advanced.priority is None
    assert config.advanced.max_item_count is None
    assert config.advanced.throughput_bucket is None
    assert config.database == "shop" and config.databases == [] and not config.all_databases
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
