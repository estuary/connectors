import asyncio
import json
import logging
import os
import subprocess
import sys

import pytest
from estuary_cdk.capture import request

from source_influxdb import Connector
from source_influxdb.models import EndpointConfig

LOG = logging.getLogger("test")


def test_spec(snapshot):
    result = subprocess.run(
        [sys.executable, "-m", "source_influxdb"],
        input='{"spec": {}}\n',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    lines = [json.loads(line)["spec"] for line in result.stdout.splitlines()]

    assert len(lines) == 1
    assert lines[0]["protocol"] == 3032023
    assert snapshot("stdout.json") == lines


def test_validation_does_not_log_credentials():
    result = subprocess.run(
        [sys.executable, "-m", "source_influxdb"],
        input=json.dumps({"validate": {
            "name": "test/capture", "connectorType": "IMAGE",
            "config": {
                "url": "http://host:8086", "org": "estuary", "token": "made-up-token",
                "credentials": {
                    "credentials_title": "Username & Password", "username": "user", "password": "made-up-password",
                },
            },
        }}) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "union_tag_invalid" in result.stderr
    assert "made-up-token" not in result.stdout + result.stderr
    assert "made-up-password" not in result.stdout + result.stderr


def test_discover(http, config, monkeypatch, snapshot):
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    discovered = asyncio.run(connector.discover(
        LOG, request.Discover[EndpointConfig](connectorType="IMAGE", config=config)
    ))
    lines = discovered.model_dump(mode="json", by_alias=True, exclude_none=True)["bindings"]

    assert snapshot("stdout.json") == lines


@pytest.mark.skipif(os.getenv("TEST_DATABASE") != "yes", reason="TEST_DATABASE=yes is required")
def test_live_discover():
    result = subprocess.run(
        [sys.executable, "-m", "source_influxdb"],
        input=json.dumps({"discover": {
            "connectorType": "IMAGE",
            "config": {
                "url": os.getenv("INFLUXDB_URL", "http://localhost:8087"),
                "credentials": {
                    "credentials_title": "API Token",
                    "access_token": os.getenv("INFLUXDB_TOKEN", "made-up-token"),
                },
                "org": os.getenv("INFLUXDB_ORG", "estuary"),
                "start_date": "2020-01-01T00:00:00Z",
            },
        }}) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(lines) == 1
    bindings = {binding["recommendedName"]: binding for binding in lines[0]["discovered"]["bindings"]}
    for name in ("sensors/readings", "sensors/status"):
        bucket, measurement = name.split("/")
        assert bindings[name]["resourceConfig"] == {
            "name": name, "interval": "PT5M", "bucket": bucket, "measurement": measurement,
        }
        assert bindings[name]["resourcePath"] == [name]
        assert bindings[name]["key"] == ["/series", "/time"]
