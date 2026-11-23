import json
from datetime import UTC, datetime, timedelta

import pytest
from freezegun import freeze_time
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from source_influxdb import Connector
from source_influxdb.models import EndpointConfig, ResourceConfig, resource_name


@pytest.mark.parametrize("values", [{}, {"start_date": None}])
def test_defaults(config, values):
    before = datetime.now(tz=UTC) - timedelta(days=30)
    config = EndpointConfig(url="http://host:8086/", credentials=config.credentials, org="estuary", **values)
    start_date = datetime.fromisoformat(config.initial_start_date)
    after = datetime.now(tz=UTC) - timedelta(days=30)

    assert config.start_date is None
    assert before <= start_date <= after
    assert start_date.tzinfo == UTC
    assert config.advanced.lag_seconds == 60
    assert config.advanced.lookback_hours == 0
    assert config.advanced.ssl_ca_certificate == ""
    assert config.advanced.ssl_skip_verify is False
    assert config.url == "http://host:8086"
    assert "made-up-unit-test-token" not in repr(config)


def test_defaults_are_independent(config):
    values = config.model_dump(exclude={"start_date", "advanced"})
    with freeze_time("2026-11-10T15:00:00Z") as frozen:
        first = EndpointConfig.model_validate(values)
        assert first.initial_start_date == "2026-10-11T15:00:00+00:00"
        first.advanced.lag_seconds = 120
        frozen.tick(timedelta(days=1))
        second = EndpointConfig.model_validate(values)
        assert second.initial_start_date == "2026-10-12T15:00:00+00:00"
        assert first.initial_start_date == "2026-10-11T15:00:00+00:00"
        assert second.advanced.lag_seconds == 60
        assert first.advanced is not second.advanced
        assert "initial_start_date" not in first.model_dump()


@pytest.mark.parametrize("field", ["url", "credentials", "org"])
def test_required_config(config, field):
    values = config.model_dump()
    del values[field]
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(values)


@pytest.mark.parametrize("values", [
    {"url": "ftp://host:8086"},
    {"url": "host:8086"},
    {"url": "http://host:8086?org=other"},
    {"url": "http://user:password@host:8086"},
    {"credentials": {"credentials_title": "API Token", "access_token": ""}},
    {"org": ""},
    {"start_date": "2020-01-01T00:00:00"},
    {"start_date": "2020-01-01T00:00:00.1234567890Z"},
    {"start_date": "2020-02-30T00:00:00Z"},
    {"start_date": "2020-01-01T00:00:00+25:00"},
    {"start_date": "1234567890"},
    {"advanced": {"lag_seconds": -1}},
    {"advanced": {"lookback_hours": -1}},
    {"advanced": {"lookback_hours": 0.5}},
    {"advanced": {"ssl_ca_certificate": "certificate", "ssl_skip_verify": True}},
])
def test_invalid_config(config, values):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate({
            **config.model_dump(),
            **values,
        })


def test_config_schema(config):
    schema = EndpointConfig.model_json_schema()
    assert schema["required"] == ["url", "credentials", "org"]
    assert not {"oneOf", "anyOf", "allOf"} & schema.keys()
    assert "token" not in schema["properties"]
    credentials = schema["properties"]["credentials"]
    assert credentials["oneOf"] == [{"$ref": "#/$defs/ApiToken"}]
    assert credentials["discriminator"]["propertyName"] == "credentials_title"
    assert "default" not in credentials
    token = schema["$defs"]["ApiToken"]
    assert token["title"] == "API Token"
    assert token["properties"]["access_token"]["secret"] is True
    assert token["properties"]["access_token"]["type"] == "string"
    assert token["properties"]["credentials_title"]["const"] == "API Token"
    assert schema["properties"]["start_date"]["format"] == "date-time"
    assert schema["$defs"]["Advanced"]["properties"]["lag_seconds"]["default"] == 60
    assert schema["$defs"]["Advanced"]["properties"]["lookback_hours"]["default"] == 0
    assert "UsernamePassword" not in schema["$defs"]
    assert "DatabaseToken" not in schema["$defs"]
    validator = Draft202012Validator(schema)
    assert validator.is_valid(config.model_dump(mode="json"))
    assert not validator.is_valid(config.model_dump(mode="json", exclude={"credentials"}))
    assert not validator.is_valid({**config.model_dump(mode="json"), "credentials": None})


def test_api_token_credentials(config):
    assert config.credentials.access_token == "made-up-unit-test-token"
    assert "made-up-" not in repr(config)
    assert EndpointConfig.model_validate_json(config.model_dump_json()) == config
    assert "token" not in config.model_dump()


def test_top_level_token_does_not_authenticate(config):
    with pytest.raises(ValidationError, match="credentials"):
        EndpointConfig.model_validate({**config.model_dump(exclude={"credentials"}), "token": "made-up-token"})


@pytest.mark.parametrize("credentials", [
    {"credentials_title": "Username & Password", "username": "user", "password": "made-up-password"},
    {"credentials_title": "Database Token", "access_token": "made-up-database-token"},
])
def test_alternate_credentials_are_rejected(config, credentials):
    values = {**config.model_dump(), "credentials": credentials}
    with pytest.raises(ValidationError, match="API Token"):
        EndpointConfig.model_validate(values)
    assert not Draft202012Validator(EndpointConfig.model_json_schema()).is_valid(values)


@pytest.mark.parametrize("credentials", [
    {"credentials_title": "Username & Password", "username": "user"},
    {"credentials_title": "Username & Password", "username": "", "password": "password"},
    {"credentials_title": "Username & Password", "username": "user", "password": ""},
    {"credentials_title": "Username & Password", "username": "user:name", "password": "password"},
    {"credentials_title": "Database Token", "access_token": ""},
    {"credentials_title": "Database Token"},
    {"credentials_title": "unknown"},
    {"credentials_title": "API Token"},
    {"credentials_title": "API Token", "access_token": ""},
    {"access_token": "made-up-token"},
    None,
])
def test_invalid_alternate_credentials(config, credentials):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate({**config.model_dump(), "credentials": credentials})


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("values", [
    {"org": "estuary", "token": "made-up-token", "credentials": {
        "credentials_title": "Username & Password", "username": "user", "password": "made-up-password",
    }},
    {"token": "made-up-token"},
    {"org": "estuary", "credentials": {
        "credentials_title": "Username & Password", "password": "made-up-password",
    }},
    {"org": "estuary", "credentials": {
        "credentials_title": "API Token", "access_token": "made-up-token",
    }, "advanced": {"ssl_ca_certificate": "certificate", "ssl_skip_verify": True}},
])
def test_validation_errors_hide_credentials(values, nested):
    values = {"url": "http://host:8086", **values}
    model = EndpointConfig
    if nested:
        model = Connector().request_class()
        values = {"validate": {"name": "test/capture", "connectorType": "IMAGE", "config": values}}

    with pytest.raises(ValidationError) as error:
        model.model_validate_json(json.dumps(values))

    assert "made-up-token" not in str(error.value)
    assert "made-up-password" not in str(error.value)
    assert "input_value" not in str(error.value)


def test_start_date_preserves_nanoseconds(config):
    start = "2026-01-01T00:00:00.123456789Z"
    config = EndpointConfig.model_validate({**config.model_dump(), "start_date": start})
    assert config.start_date == start
    assert config.initial_start_date == start
    assert EndpointConfig.model_validate_json(config.model_dump_json()).start_date == start


@pytest.mark.parametrize("name, expected", [
    ("sensors/readings", ("sensors", "readings")),
    ("sensors.west/readings", ("sensors.west", "readings")),
    ("a.b.c/status", ("a.b.c", "status")),
    ("sensors%2Fa/app%25requests", ("sensors/a", "app%requests")),
])
def test_resource_name(name, expected):
    assert resource_name(*expected) == name
    resource = ResourceConfig(name=name, bucket=expected[0], measurement=expected[1])
    assert resource.components() == expected
    assert resource.path() == [name]


@pytest.mark.parametrize("name", ["sensors.readings", "sensors.west.readings", "sensors/readings"])
def test_resource_components_are_required(name):
    with pytest.raises(ValidationError) as error:
        ResourceConfig(name=name)
    assert {item["loc"] for item in error.value.errors()} == {("bucket",), ("measurement",)}
    schema = ResourceConfig.model_json_schema()
    assert schema["required"] == ["name", "bucket", "measurement"]
    assert not Draft202012Validator(schema).is_valid({"name": name})


@pytest.mark.parametrize("name", ["", "sensors", ".readings", "sensors."])
def test_invalid_resource_name(name):
    with pytest.raises(ValidationError, match="discovered name"):
        ResourceConfig(name=name, bucket="sensors", measurement="readings")


@pytest.mark.parametrize("values", [
    {}, {"bucket": "sensors"}, {"measurement": "app.requests"},
    {"bucket": "", "measurement": "app.requests"},
    {"bucket": "sensors", "measurement": ""},
    {"bucket": None, "measurement": "app.requests"},
    {"bucket": "sensors", "measurement": None},
])
def test_invalid_resource_components(values):
    with pytest.raises(ValidationError):
        ResourceConfig(name="sensors.app.requests", **values)


def test_explicit_resource_components():
    resource = ResourceConfig(name="sensors.west/app.requests", bucket="sensors.west", measurement="app.requests")
    assert resource.components() == ("sensors.west", "app.requests")
    assert resource.path() == ["sensors.west/app.requests"]
    assert ResourceConfig.model_validate_json(resource.model_dump_json()).components() == resource.components()


def test_resource_names_are_unambiguous():
    pairs = [
        ("sensors", "app.requests"), ("sensors.app", "requests"),
        ("sensors", "app%2Erequests"), ("sensors", "app%2E.requests"),
        ("sensors.app", "requests.count"), ("sensors", "app.requests.count"),
        ("sensors/a", "b.c"), ("sensors", "a/b.c"),
    ]
    names = [resource_name(*pair) for pair in pairs]
    assert len(set(names)) == len(pairs)
    for (bucket, measurement), name in zip(pairs, names):
        resource = ResourceConfig(name=name, bucket=bucket, measurement=measurement)
        assert resource.path() == [name]
        assert resource.components() == (bucket, measurement)
        assert name.count("/") == 1


def test_resource_name_must_match_components():
    with pytest.raises(ValidationError, match="discovered name"):
        ResourceConfig(name="sensors.app.requests", bucket="sensors", measurement="app.requests")
