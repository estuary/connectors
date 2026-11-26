import json
import subprocess


def test_discover(influxdb, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            str(influxdb),
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines


def test_spec(influxdb, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            str(influxdb),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
