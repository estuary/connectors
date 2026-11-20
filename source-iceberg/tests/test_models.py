import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from source_iceberg.models import (
    CatalogStorageConfig, CatalogTokenConfig, EndpointConfig, NoIAMConfig, ResourceConfig, S3Config,
)


def test_spec():
    result = subprocess.run(
        [sys.executable, "-m", "source_iceberg"],
        input='{"spec": {}}\n',
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    spec = json.loads(result.stdout)["spec"]
    assert spec["protocol"] == 3032023
    assert spec["resourcePathPointers"] == ["/namespace", "/table"]
    Draft202012Validator.check_schema(spec["configSchema"])
    Draft202012Validator.check_schema(spec["resourceConfigSchema"])

    config = spec["configSchema"]
    assert config["required"] == ["catalog_uri"]
    assert set(config["properties"]) == {
        "catalog_uri", "warehouse", "credential", "token", "scope", "oauth2_server_uri",
        "catalog_auth", "credentials", "namespaces", "s3"
    }
    for name in ("credential", "token"):
        assert config["properties"][name]["type"] == "string"
        assert config["properties"][name]["secret"] is True
    s3 = config["$defs"]["S3Config"]
    assert s3["properties"]["secret_access_key"]["secret"] is True
    assert s3["properties"]["session_token"]["secret"] is True
    assert s3["properties"]["region"]["default"] == "us-east-1"
    assert s3["properties"]["path_style_access"]["default"] is False
    assert spec["resourceConfigSchema"]["properties"]["interval"]["default"] == "PT5M"
    for name in ("AWSIAMConfig", "CatalogIAMConfig"):
        assert not {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} & config["$defs"][name]["properties"].keys()
    assert config["properties"]["credentials"]["x-iam-auth"] is True
    # The UI renders discriminated unions placed directly on a property, not null-wrapped ones.
    for name in ("catalog_auth", "credentials", "s3"):
        section = config["properties"][name]
        assert "anyOf" not in section
        assert section["discriminator"]["propertyName"] == "auth_type"
        assert section["default"] == {"auth_type": "None"}
        assert "None" in section["discriminator"]["mapping"]
    for name in ("aws_secret_access_key", "aws_session_token"):
        assert config["$defs"]["SigV4Config"]["properties"][name]["secret"] is True
    snapshot = Path(__file__).parent / "snapshots" / "snapshots__spec__capture.stdout.json"
    assert json.loads(snapshot.read_text()) == {"spec": spec}


def test_config_defaults():
    config = EndpointConfig(catalog_uri="http://catalog.test")
    assert config.namespaces == []
    assert isinstance(config.s3, CatalogStorageConfig)
    assert isinstance(config.catalog_auth, CatalogTokenConfig)
    assert isinstance(config.credentials, NoIAMConfig)
    assert config.scope == config.oauth2_server_uri == ""
    resource = ResourceConfig(namespace="shop", table="orders")
    assert resource.path() == ["shop", "orders"]
    assert resource.interval == timedelta(minutes=5)
    assert ResourceConfig(namespace="shop", table="orders", interval="PT5S").interval == timedelta(seconds=5)


def test_legacy_null_and_untyped_sections():
    config = EndpointConfig.model_validate({
        "catalog_uri": "http://catalog.test", "catalog_auth": None, "credentials": None, "s3": None,
    })
    assert isinstance(config.s3, CatalogStorageConfig)
    assert isinstance(config.catalog_auth, CatalogTokenConfig)
    assert isinstance(config.credentials, NoIAMConfig)

    config = EndpointConfig.model_validate({
        "catalog_uri": "http://catalog.test",
        "s3": {"access_key_id": "made-up-key", "secret_access_key": "made-up-secret"},
    })
    assert isinstance(config.s3, S3Config)


@pytest.mark.parametrize("config", [
    {"catalog_uri": "http://catalog.test"},
    {"catalog_uri": "http://catalog.test", "s3": {
        "endpoint": "http://storage.test:9000", "region": "us-east-1",
        "access_key_id": "made-up-key", "secret_access_key": "made-up-secret", "path_style_access": True,
    }},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "None"}, "catalog_auth": {"auth_type": "None"}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}, "catalog_auth": {"auth_type": "AWSIAM"},
     "credentials": {
         "auth_type": "AWSIAM", "aws_region": "us-east-1", "aws_role_arn": "arn:aws:iam::123456789012:role/reader",
     }},
    {"catalog_uri": "http://catalog.test", "catalog_auth": {
        "auth_type": "AWS SigV4", "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
    }},
])
def test_config_matches_one_schema_branch(config):
    # Schema-validating clients must accept every config the connector accepts, which
    # needs each section to match exactly one oneOf branch.
    Draft202012Validator(EndpointConfig.model_json_schema()).validate(config)
    EndpointConfig.model_validate(config)


@pytest.mark.parametrize("config", [
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"},
     "credentials": {"aws_region": "us-east-1", "aws_role_arn": "arn:aws:iam::123456789012:role/reader"}},
    {"catalog_uri": "http://catalog.test", "catalog_auth": {
        "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
    }},
])
def test_untyped_section_rejected_by_schema_and_connector(config):
    # Flow reads credentials.auth_type to decide whether to assume the role, so the
    # schema and connector both require it, and the same for catalog_auth.
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)


@pytest.mark.parametrize("config", [
    {},
    {"catalog_uri": ""},
    {"catalog_uri": "http://catalog.test", "s3": {}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}},
    {"catalog_uri": "http://catalog.test", "catalog_auth": {"auth_type": "AWSIAM"}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}, "credentials": {"auth_type": "None"}},
    {"catalog_uri": "http://catalog.test", "catalog_auth": {"auth_type": "AWS SigV4"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "AWSIAM", "aws_region": "us-east-1"}},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "AWSIAM", "aws_role_arn": "arn:aws:iam::123456789012:role/reader",
    }},
    {"catalog_uri": "http://catalog.test", "oauth2_server_uri": "file:///tmp/token"},
    {"catalog_uri": "http://catalog.test", "oauth2_server_uri": "https:/missing-host"},
    {"catalog_uri": "http://catalog.test", "oauth2_server_uri": "//login.test/token"},
    {"catalog_uri": "http://catalog.test", "token": "made-up-token", "catalog_auth": {
        "auth_type": "AWSIAM", "aws_role_arn": "arn:aws:iam::123456789012:role/reader",
    }},
])
def test_invalid_endpoint_config(config):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)


@pytest.mark.parametrize("interval", ["PT0S", "-PT5S", "invalid"])
def test_invalid_polling_interval(interval):
    with pytest.raises(ValidationError):
        ResourceConfig(namespace="shop", table="orders", interval=interval)
