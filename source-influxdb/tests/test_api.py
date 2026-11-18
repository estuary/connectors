import asyncio
import csv
import io
import json
import logging

import pytest
from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_csv_processor import CSVProcessingError
from pydantic import ValidationError

from source_influxdb.api import flux_string, list_buckets, list_measurements

LOG = logging.getLogger("test")


def collect(gen):
    async def run():
        return [item async for item in gen]

    return asyncio.run(run())


def test_buckets_stream_without_pagination(http, config, bucket_names):
    names = [f"bucket.{i}" for i in range(101)] + ['sensor,"quoted"', "temp\n°"]
    bucket_names[:] = names

    buckets = collect(list_buckets(LOG, http, config.url, config.org, config.common_headers))

    assert [bucket.name for bucket in buckets] == names
    http.request.assert_not_awaited()
    http.request_stream.assert_awaited_once()
    call = http.request_stream.await_args
    assert call.args == (LOG, "http://influxdb.example:8086/api/v2/query")
    assert call.kwargs["method"] == "POST"
    assert call.kwargs["params"] == {"org": "estuary"}
    assert call.kwargs["headers"] == {
        "Authorization": "Token made-up-unit-test-token", "Accept": "application/csv",
    }
    assert call.kwargs["json"] == {
        "query": "buckets()", "type": "flux", "dialect": {"header": True, "annotations": []},
    }


@pytest.mark.parametrize("first_page", [[], ["a", "b", "c"]])
def test_bucket_listing_with_filtered_rest_pages(http, config, first_page):
    pages = [first_page, ["d", "e"]]
    http.request.side_effect = [
        json.dumps({"buckets": [{"name": name} for name in page]}).encode()
        for page in pages
    ]

    async def body():
        yield b",result,table,name\n"
        for page in pages:
            yield "".join(f",_result,0,{name}\n" for name in page).encode()

    http.request_stream.return_value = ({}, body)
    http.request_stream.side_effect = None
    buckets = collect(list_buckets(LOG, http, config.url, config.org, config.common_headers))

    assert [bucket.name for bucket in buckets] == first_page + ["d", "e"]
    http.request.assert_not_awaited()
    http.request_stream.assert_awaited_once()
    assert http.request_stream.await_args.kwargs["json"]["query"] == "buckets()"


def test_empty_buckets(http, config, bucket_names):
    bucket_names.clear()
    assert collect(list_buckets(LOG, http, config.url, config.org, config.common_headers)) == []
    http.request_stream.assert_awaited_once()


@pytest.mark.parametrize("data, error", [
    (b",result,table,missing\n,_result,0,sensors\n", ValidationError),
    (b",error,reference\n,query failed,123\n", ValidationError),
    (b",result,table,name\n,_result,0\n", CSVProcessingError),
    (b",result,table,name\n,_result,0,sensors,extra\n", CSVProcessingError),
])
def test_invalid_buckets_fail(http, config, data, error):
    async def body():
        yield data

    http.request_stream.return_value = ({}, body)
    http.request_stream.side_effect = None
    with pytest.raises(error):
        collect(list_buckets(LOG, http, config.url, config.org, config.common_headers))


@pytest.mark.parametrize("code", [401, 403, 500])
def test_http_errors_propagate(http, config, code):
    error = HTTPError("InfluxDB request failed", code)
    http.request.side_effect = error
    http.request_stream.side_effect = error

    with pytest.raises(HTTPError) as buckets_error:
        collect(list_buckets(LOG, http, config.url, config.org, config.common_headers))
    assert buckets_error.value is error

    with pytest.raises(HTTPError) as measurements_error:
        asyncio.run(list_measurements(LOG, http, config.url, config.org, config.common_headers, "sensors"))
    assert measurements_error.value is error


def test_measurements_stream_csv(http, config):
    names = ["status", "readings", 'sensor,"quoted"', "temp\n°", "readings"]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["", "result", "table", "_value"])
    writer.writerows(["", "_result", "0", name] for name in names)
    data = output.getvalue().encode()

    async def body():
        for byte in data:
            yield bytes([byte])

    http.request_stream.return_value = ({}, body)
    measurements = asyncio.run(
        list_measurements(LOG, http, config.url, config.org, config.common_headers, 'sensors."west"')
    )

    assert measurements == sorted(set(names))
    call = http.request_stream.await_args
    assert call.args == (LOG, "http://influxdb.example:8086/api/v2/query")
    assert call.kwargs["method"] == "POST"
    assert call.kwargs["params"] == {"org": "estuary"}
    assert call.kwargs["headers"] == {
        "Authorization": "Token made-up-unit-test-token",
        "Accept": "application/csv",
    }
    assert call.kwargs["json"] == {
        "query": 'from(bucket: "sensors.\\"west\\"")\n'
        '  |> range(start: time(v: -9223372036854775806), stop: time(v: 9223372036854775807))\n'
        '  |> keep(columns: ["_measurement"])\n'
        '  |> group()\n'
        '  |> distinct(column: "_measurement")',
        "type": "flux",
        "dialect": {"header": True, "annotations": []},
    }


@pytest.mark.parametrize("data", [b"", b"\r\n", b",result,table,_value\r\n"])
def test_empty_measurements(http, config, data):
    async def body():
        yield data

    http.request_stream.return_value = ({}, body)
    assert asyncio.run(
        list_measurements(LOG, http, config.url, config.org, config.common_headers, "sensors")
    ) == []


@pytest.mark.parametrize("data, error", [
    (b",error,reference\n,query failed,123\n", ValidationError),
    (b",result,table,_value\n,_result,0\n", CSVProcessingError),
    (b",result,table,_value\n,_result,0,readings,extra\n", CSVProcessingError),
])
def test_invalid_measurements_fail(http, config, data, error):
    async def body():
        yield data

    http.request_stream.return_value = ({}, body)
    with pytest.raises(error):
        asyncio.run(
            list_measurements(LOG, http, config.url, config.org, config.common_headers, "sensors")
        )


@pytest.mark.parametrize("value, expected", [
    ("sensors", '"sensors"'),
    ('sensors."west"', '"sensors.\\"west\\""'),
    ("sensors\\west", '"sensors\\\\west"'),
    ("sensors\nwest", '"sensors\\nwest"'),
    ("sensors.${secret}", '"sensors.\\${secret}"'),
    ("sensors.°", '"sensors.°"'),
    ("sensors\x01\x0c\bwest", '"sensors\\x01\\x0c\\x08west"'),
    ("sensors\r\twest\x7f", '"sensors\\r\\twest\x7f"'),
    ("sensors\\${x}", '"sensors\\\\\\${x}"'),
])
def test_flux_string(value, expected):
    assert flux_string(value) == expected
