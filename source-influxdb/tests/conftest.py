import csv
import io
import os
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from estuary_cdk.http import HTTPSession

from source_influxdb.models import EndpointConfig


@pytest.fixture(scope="session")
def influxdb(tmp_path_factory):
    connector_dir = Path(__file__).resolve().parents[1]
    compose = ["docker", "compose", "-f", str(connector_dir / "docker-compose.yaml")]
    env = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": f"source-influxdb-pytest-{uuid4().hex}",
        "INFLUXDB_PORT": "0",
    }
    try:
        subprocess.run([*compose, "up", "--wait"], check=True, env=env)
        subprocess.run(["bash", str(connector_dir / "tests/fixtures/seed.sh")], check=True, env=env)
        result = subprocess.run(
            [*compose, "port", "db", "8086"], check=True, env=env,
            stdout=subprocess.PIPE, text=True,
        )
        port = result.stdout.splitlines()[0].rsplit(":", 1)[1]
        catalog = tmp_path_factory.mktemp("influxdb") / "test.flow.yaml"
        catalog.write_text((connector_dir / "test.flow.yaml").read_text().replace(
            "http://localhost:8087", f"http://localhost:{port}",
        ))
        yield catalog
    finally:
        subprocess.run([*compose, "down", "--volumes"], check=True, env=env)


@pytest.fixture
def config():
    return EndpointConfig(
        url="http://influxdb.example:8086",
        credentials={"credentials_title": "API Token", "access_token": "made-up-unit-test-token"},
        org="estuary",
        start_date="2020-01-01T00:00:00Z",
    )


@pytest.fixture
def tls_certificate(tmp_path):
    certificate = tmp_path / "test-ca.pem"
    key = tmp_path / "test-key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(certificate), "-days", "1",
            "-subj", "/CN=influxdb.test",
            "-addext", "subjectAltName=DNS:influxdb.test",
            "-addext", "basicConstraints=critical,CA:TRUE",
        ],
        check=True,
        capture_output=True,
    )
    return certificate, key


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
