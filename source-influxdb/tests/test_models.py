import json
from datetime import UTC, datetime, timedelta

import pytest
from estuary_cdk.flow import ValidationError as FlowValidationError
from pydantic import ValidationError

from source_influxdb import Connector
from source_influxdb.models import EndpointConfig, ResourceConfig, resource_name, split_resource_name


def test_defaults():
    before = datetime.now(tz=UTC) - timedelta(days=30)
    config = EndpointConfig(url="http://host:8086/", token="made-up-token", org="estuary")
    after = datetime.now(tz=UTC) - timedelta(days=30)

    start_date = datetime.fromisoformat(config.start_date)
    assert before <= start_date <= after
    assert start_date.tzinfo == UTC
    assert config.advanced.lag_seconds == 60
    assert config.advanced.lookback_hours == 0
    assert config.advanced.ssl_ca_certificate == ""
    assert config.advanced.ssl_skip_verify is False
    assert config.url == "http://host:8086"
    assert "made-up-token" not in repr(config)


@pytest.mark.parametrize("field", ["url", "token", "org"])
def test_required_config(field):
    values = {"url": "http://host:8086", "token": "made-up-token", "org": "estuary"}
    del values[field]
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate(values)


@pytest.mark.parametrize("values", [
    {"url": "ftp://host:8086"},
    {"url": "host:8086"},
    {"url": "http://host:8086?org=other"},
    {"url": "http://user:password@host:8086"},
    {"token": ""},
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
def test_invalid_config(values):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate({
            "url": "http://host:8086",
            "token": "made-up-token",
            "org": "estuary",
            **values,
        })


def test_config_schema():
    schema = EndpointConfig.model_json_schema()
    assert schema["required"] == ["url", "org"]
    assert not {"oneOf", "anyOf", "allOf"} & schema.keys()
    assert schema["properties"]["token"]["type"] == "string"
    assert schema["properties"]["token"]["secret"] is True
    assert schema["properties"]["start_date"]["format"] == "date-time"
    assert schema["$defs"]["Advanced"]["properties"]["lag_seconds"]["default"] == 60
    assert schema["$defs"]["Advanced"]["properties"]["lookback_hours"]["default"] == 0
    assert schema["$defs"]["UsernamePassword"]["properties"]["password"]["secret"] is True
    assert schema["$defs"]["DatabaseToken"]["properties"]["access_token"]["secret"] is True


@pytest.mark.parametrize("credentials", [
    {"credentials_title": "Username & Password", "username": "user", "password": "made-up-password"},
    {"credentials_title": "Database Token", "access_token": "made-up-database-token"},
])
def test_alternate_credentials(config, credentials):
    values = {**config.model_dump(exclude={"token"}), "credentials": credentials}
    alternate = EndpointConfig.model_validate(values)
    assert alternate.token is None
    assert alternate.common_headers == {}
    assert "made-up-" not in repr(alternate)
    assert EndpointConfig.model_validate_json(alternate.model_dump_json()) == alternate
    with pytest.raises(ValidationError, match="exactly one"):
        EndpointConfig.model_validate({**values, "token": "made-up-token"})


@pytest.mark.parametrize("credentials", [
    {"credentials_title": "Username & Password", "username": "user"},
    {"credentials_title": "Username & Password", "username": "", "password": "password"},
    {"credentials_title": "Username & Password", "username": "user", "password": ""},
    {"credentials_title": "Username & Password", "username": "user:name", "password": "password"},
    {"credentials_title": "Database Token", "access_token": ""},
    {"credentials_title": "Database Token"},
    {"credentials_title": "unknown"},
])
def test_invalid_alternate_credentials(config, credentials):
    with pytest.raises(ValidationError):
        EndpointConfig.model_validate({**config.model_dump(exclude={"token"}), "credentials": credentials})


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
        "credentials_title": "Database Token", "access_token": "made-up-token",
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
    assert EndpointConfig.model_validate_json(config.model_dump_json()).start_date == start


@pytest.mark.parametrize("name, expected", [
    ("sensors.readings", ("sensors", "readings")),
    ("sensors.west.readings", ("sensors.west", "readings")),
    ("a.b.c.status", ("a.b.c", "status")),
])
def test_split_resource_name(name, expected):
    assert split_resource_name(name) == expected
    assert ResourceConfig(name=name).components() == expected
    assert ResourceConfig(name=name).path() == [name]


@pytest.mark.parametrize("name", ["", "sensors", ".readings", "sensors."])
def test_invalid_resource_name(name):
    with pytest.raises(FlowValidationError, match="<bucket>.<measurement>"):
        split_resource_name(name)
    with pytest.raises(ValidationError, match="<bucket>.<measurement>"):
        ResourceConfig(name=name)


@pytest.mark.parametrize("values", [
    {"bucket": "sensors"}, {"measurement": "app.requests"},
    {"bucket": "", "measurement": "app.requests"},
    {"bucket": "sensors", "measurement": ""},
])
def test_invalid_resource_components(values):
    with pytest.raises(ValidationError):
        ResourceConfig(name="sensors.app.requests", **values)


def test_explicit_resource_components():
    resource = ResourceConfig(name="sensors%2Ewest/app%2Erequests", bucket="sensors.west", measurement="app.requests")
    assert resource.components() == ("sensors.west", "app.requests")
    assert resource.path() == ["sensors%2Ewest/app%2Erequests"]
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
        if "." in measurement:
            assert "." not in name


def test_resource_name_must_match_components():
    with pytest.raises(ValidationError, match="discovered name"):
        ResourceConfig(name="sensors.app.requests", bucket="sensors", measurement="app.requests")
