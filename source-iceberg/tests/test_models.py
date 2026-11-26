import json
import subprocess
import sys
from datetime import timedelta

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from source_iceberg.models import (
    CatalogStorageConfig, EndpointConfig, NoAuthConfig, ResourceConfig,
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
        "catalog_uri", "warehouse", "credentials", "namespaces", "s3"
    }
    for name in ("credential", "token"):
        assert config["$defs"]["OAuthConfig"]["properties"][name]["type"] == "string"
        assert config["$defs"]["OAuthConfig"]["properties"][name]["secret"] is True
    assert config["$defs"]["TokenConfig"]["properties"]["token"]["secret"] is True
    s3 = config["$defs"]["S3Config"]
    assert s3["properties"]["secret_access_key"]["secret"] is True
    assert s3["properties"]["session_token"]["secret"] is True
    assert s3["properties"]["region"]["default"] == "us-east-1"
    assert s3["properties"]["path_style_access"]["default"] is False
    assert spec["resourceConfigSchema"]["properties"]["interval"]["default"] == "PT5M"
    assert not {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} & config["$defs"]["AWSIAMConfig"]["properties"].keys()
    assert config["properties"]["credentials"]["x-iam-auth"] is True
    # The UI renders discriminated unions placed directly on a property, not null-wrapped ones.
    for name in ("credentials", "s3"):
        section = config["properties"][name]
        assert "anyOf" not in section
        assert section["discriminator"]["propertyName"] == "auth_type"
        assert section["default"] == {"auth_type": "None"}
        assert "None" in section["discriminator"]["mapping"]
    for name in ("aws_secret_access_key", "aws_session_token"):
        assert config["$defs"]["SigV4Config"]["properties"][name]["secret"] is True
    assert {
        name: option["title"] for name, option in config["$defs"].items()
    } == {
        "NoAuthConfig": "No Authentication",
        "OAuthConfig": "OAuth 2.0 Client Credentials",
        "TokenConfig": "Bearer Token",
        "SigV4Config": "AWS SigV4 Access Keys",
        "AWSIAMConfig": "AWS IAM Role",
        "CatalogStorageConfig": "Catalog Storage Credentials",
        "S3Config": "S3 Access Keys",
        "IAMConfig": "Use Catalog IAM Role",
    }


def test_config_defaults():
    config = EndpointConfig(catalog_uri="http://catalog.test")
    assert config.namespaces == []
    assert isinstance(config.s3, CatalogStorageConfig)
    assert isinstance(config.credentials, NoAuthConfig)
    resource = ResourceConfig(namespace="shop", table="orders")
    assert resource.path() == ["shop", "orders"]
    assert resource.interval == timedelta(minutes=5)
    assert ResourceConfig(namespace="shop", table="orders", interval="PT5S").interval == timedelta(seconds=5)


@pytest.mark.parametrize("section", ["credentials", "s3"])
def test_null_sections_rejected(section):
    config = {"catalog_uri": "http://catalog.test", section: None}
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)


@pytest.mark.parametrize("config", [
    {"catalog_uri": "http://catalog.test"},
    {"catalog_uri": "http://catalog.test", "s3": {
        "auth_type": "AWSAccessKey",
        "endpoint": "http://storage.test:9000", "region": "us-east-1",
        "access_key_id": "made-up-key", "secret_access_key": "made-up-secret", "path_style_access": True,
    }},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "None"}, "credentials": {"auth_type": "None"}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"},
     "credentials": {
         "auth_type": "AWSIAM", "aws_region": "us-east-1", "aws_role_arn": "arn:aws:iam::123456789012:role/reader",
     }},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "AWS SigV4", "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
    }},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "OAuth2", "credential": "made-up-client:made-up-secret",
    }},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "OAuth2", "credential": "made-up-client:made-up-secret", "token": "made-up-token",
        "scope": "catalog", "oauth2_server_uri": "https://login.test/oauth/token",
    }},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "Bearer Token", "token": "made-up-token",
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
    {"catalog_uri": "http://catalog.test", "credentials": {
        "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
    }},
    {"catalog_uri": "http://catalog.test", "credentials": {"credential": "made-up-client:made-up-secret"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"token": "made-up-token"}},
    {"catalog_uri": "http://catalog.test", "credentials": {}},
    {"catalog_uri": "http://catalog.test", "s3": {
        "access_key_id": "made-up-key", "secret_access_key": "made-up-secret",
    }},
])
def test_untyped_section_rejected_by_schema_and_connector(config):
    # Estuary reads credentials.auth_type to decide whether to assume the role,
    # so the schema and connector both require it, including for storage overrides.
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(config)
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)


@pytest.mark.parametrize("config", [
    {},
    {"catalog_uri": ""},
    {"catalog_uri": "http://catalog.test", "s3": {}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "AWSIAM"}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}, "credentials": {"auth_type": "None"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "AWS SigV4"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "AWSIAM", "aws_region": "us-east-1"}},
    {"catalog_uri": "http://catalog.test", "credentials": {
        "auth_type": "AWSIAM", "aws_role_arn": "arn:aws:iam::123456789012:role/reader",
    }},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "Bearer Token"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "Bearer Token", "token": ""}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "OAuth2"}},
    {"catalog_uri": "http://catalog.test", "credentials": {"auth_type": "OAuth2", "credential": ""}},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}, "credentials": {
        "auth_type": "OAuth2", "credential": "made-up-client:made-up-secret",
    }},
    {"catalog_uri": "http://catalog.test", "s3": {"auth_type": "AWSIAM"}, "credentials": {
        "auth_type": "AWS SigV4", "aws_access_key_id": "made-up-key", "aws_secret_access_key": "made-up-secret",
    }},
])
def test_invalid_endpoint_config(config):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(config)


@pytest.mark.parametrize("uri", ["file:///tmp/token", "https:/missing-host", "//login.test/token"])
def test_invalid_oauth_server_uri(uri):
    with pytest.raises(ValidationError, match="OAuth2 server URI"):
        EndpointConfig.model_validate({
            "catalog_uri": "http://catalog.test", "credentials": {
                "auth_type": "OAuth2", "credential": "made-up-client:made-up-secret", "oauth2_server_uri": uri,
            },
        })


@pytest.mark.parametrize("interval", ["PT0S", "-PT5S", "invalid"])
def test_invalid_polling_interval(interval):
    with pytest.raises(ValidationError):
        ResourceConfig(namespace="shop", table="orders", interval=interval)
