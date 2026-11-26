import asyncio
import logging
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from estuary_cdk.capture import request
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError

from source_influxdb import Connector
from source_influxdb import resources as resource_module
from source_influxdb.models import EndpointConfig, ResourceConfig, resource_name
from source_influxdb.resources import all_resources

LOG = logging.getLogger("test")


def test_resources_skip_system_buckets(http, config):
    resources = asyncio.run(all_resources(LOG, http, config))

    assert [resource.name for resource in resources] == ["sensors/readings", "sensors/status"]
    assert http.request_stream.await_count == 2
    assert http.request_stream.await_args_list[0].kwargs["json"]["query"] == "buckets()"
    for resource in resources:
        assert resource.key == ["/series", "/time"]
        assert resource.initial_config.name == resource.name
        assert resource.initial_config.interval == timedelta(minutes=5)
        assert resource.schema_inference is True


def test_multiple_buckets_with_dots(http, config, bucket_names):
    bucket_names[:] = ["sensors.west", "sensors.east"]

    resources = asyncio.run(all_resources(LOG, http, config))

    assert [resource.name for resource in resources] == [
        "sensors.east/readings", "sensors.east/status",
        "sensors.west/readings", "sensors.west/status",
    ]
    queries = [call.kwargs["json"]["query"] for call in http.request_stream.await_args_list]
    assert queries[0] == "buckets()"
    assert 'bucket: "sensors.west"' in queries[1]
    assert 'bucket: "sensors.east"' in queries[2]


def test_dotted_measurements_do_not_collide(http, config, bucket_names, monkeypatch, caplog):
    bucket_names[:] = ["telegraf", "telegraf.app"]
    monkeypatch.setattr(resource_module, "list_measurements", AsyncMock(
        side_effect=[["app.requests"], ["requests"]],
    ))

    resources = asyncio.run(all_resources(LOG, http, config))

    assert len(resources) == 2
    assert {tuple(resource.initial_config.components()) for resource in resources} == {
        ("telegraf", "app.requests"), ("telegraf.app", "requests"),
    }
    assert {tuple(resource.initial_config.path()) for resource in resources} == {
        ("telegraf/app.requests",), ("telegraf.app/requests",),
    }
    assert "Skipping" not in caplog.text


@pytest.mark.parametrize("names", [[], ["_tasks", "_monitoring"]])
def test_no_user_buckets(http, config, bucket_names, names):
    bucket_names[:] = names
    assert asyncio.run(all_resources(LOG, http, config)) == []
    http.request_stream.assert_awaited_once()
    assert http.request_stream.await_args.kwargs["json"]["query"] == "buckets()"


@pytest.mark.parametrize("bucket, measurement", [
    ("sensors", "readings"), ("sensors", "missing"), ("sensors.west", "readings"),
    ("missing", "readings"), ("sensors.app", "requests"), ("sensors", "app.requests"),
])
def test_validate_binding(http, config, bucket_names, monkeypatch, bucket, measurement):
    bucket_names.append("sensors.west")
    name = resource_name(bucket, measurement)
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    validate = request.Validate[EndpointConfig, ResourceConfig].model_validate({
        "name": "test/capture",
        "connectorType": "IMAGE",
        "config": config,
        "bindings": [{
            "resourceConfig": {"name": name, "bucket": bucket, "measurement": measurement, "interval": "PT5M"},
            "collection": {"name": "test/readings", "key": ["/series", "/time"], "writeSchema": {}},
        }],
    })

    if bucket not in bucket_names:
        with pytest.raises(ValidationError, match=f"Bucket '{bucket}'"):
            asyncio.run(connector.validate(LOG, validate))
    else:
        result = asyncio.run(connector.validate(LOG, validate))
        assert [binding.resourcePath for binding in result.bindings] == [[name]]
    http.request.assert_not_awaited()
    http.request_stream.assert_awaited_once()
    assert http.request_stream.await_args.kwargs["json"]["query"] == "buckets()"


@pytest.mark.parametrize("code", [401, 403, 404, 500])
def test_validate_rejects_invalid_connection(http, config, monkeypatch, code):
    connector = Connector()
    error = HTTPError("InfluxDB request failed", code)
    http.request.side_effect = error
    http.request_stream.side_effect = error
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    validate = request.Validate[EndpointConfig, ResourceConfig].model_validate({
        "name": "test/capture", "connectorType": "IMAGE", "config": config,
        "bindings": [{
            "resourceConfig": {"name": "sensors/readings", "bucket": "sensors", "measurement": "readings"},
            "collection": {"name": "test/readings", "key": ["/series", "/time"], "writeSchema": {}},
        }],
    })

    expected = HTTPError if code == 500 else ValidationError
    with pytest.raises(expected, match="InfluxDB request failed"):
        asyncio.run(connector.validate(LOG, validate))
    http.request_stream.assert_awaited_once()
    assert http.request_stream.await_args.kwargs["json"]["query"] == "buckets()"


@pytest.mark.parametrize("measurements", [[], ["readings", "expired"]])
def test_validate_lists_buckets_once(http, config, monkeypatch, measurements):
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    validate = request.Validate[EndpointConfig, ResourceConfig].model_validate({
        "name": "test/capture", "connectorType": "IMAGE", "config": config,
        "bindings": [{
            "resourceConfig": {"name": f"sensors/{measurement}", "bucket": "sensors", "measurement": measurement},
            "collection": {"name": f"test/{measurement}", "key": ["/series", "/time"], "writeSchema": {}},
        } for measurement in measurements],
    })

    result = asyncio.run(connector.validate(LOG, validate))
    assert [binding.resourcePath for binding in result.bindings] == [[f"sensors/{measurement}"] for measurement in measurements]
    http.request.assert_not_awaited()
    http.request_stream.assert_awaited_once()
    assert http.request_stream.await_args.kwargs["json"]["query"] == "buckets()"


def test_validate_explicit_dotted_measurements(http, config, bucket_names, monkeypatch):
    bucket_names[:] = ["telegraf", "telegraf.app"]
    connector = Connector()
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    pairs = [("telegraf", "app.requests"), ("telegraf.app", "requests")]
    validate = request.Validate[EndpointConfig, ResourceConfig].model_validate({
        "name": "test/capture", "connectorType": "IMAGE", "config": config,
        "bindings": [{
            "resourceConfig": {"name": resource_name(bucket, measurement), "bucket": bucket, "measurement": measurement},
            "collection": {"name": f"test/requests{index}", "key": ["/series", "/time"], "writeSchema": {}},
        } for index, (bucket, measurement) in enumerate(pairs)],
    })
    result = asyncio.run(connector.validate(LOG, validate))
    assert [binding.resourcePath for binding in result.bindings] == [[resource_name(*pair)] for pair in pairs]


@pytest.mark.parametrize("start_date", [
    "0001-01-01T00:00:00Z", "1600-01-01T00:00:00Z", "1677-09-21T00:12:43.145224194Z",
])
def test_start_date_clamped_to_influxdb_minimum(http, config, start_date):
    config = EndpointConfig.model_validate({**config.model_dump(), "start_date": start_date})
    resources = asyncio.run(all_resources(LOG, http, config))
    for resource in resources:
        assert resource.initial_state.inc.cursor == ("1677-09-21T00:12:43.145224193Z",)
