import csv
import io
from unittest.mock import AsyncMock, Mock

import pytest
from estuary_cdk.http import HTTPSession

from source_influxdb.models import EndpointConfig


@pytest.fixture
def config():
    return EndpointConfig(
        url="http://influxdb.example:8086",
        token="made-up-unit-test-token",
        org="estuary",
        start_date="2020-01-01T00:00:00Z",
    )


@pytest.fixture
def bucket_names():
    return ["_monitoring", "sensors", "_tasks"]


@pytest.fixture
def http(bucket_names):
    async def body():
        data = b",result,table,_value\r\n,_result,0,status\r\n,_result,0,readings\r\n\r\n"
        for offset in range(0, len(data), 5):
            yield data[offset:offset + 5]

    async def buckets_body():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["", "result", "table", "name"])
        writer.writerows(["", "_result", "0", name] for name in bucket_names)
        data = output.getvalue().encode()
        for offset in range(0, len(data), 5):
            yield data[offset:offset + 5]

    async def query(log, url, **kwargs):
        if kwargs["json"]["query"] == "buckets()":
            return {}, buckets_body
        return http.request_stream.return_value

    http = Mock(spec=HTTPSession)
    http.request = AsyncMock()
    http.request_stream = AsyncMock(return_value=({}, body), side_effect=query)
    return http
