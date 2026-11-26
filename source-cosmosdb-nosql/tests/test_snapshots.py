import json
import subprocess
from pathlib import Path

from pytest_insta import SnapshotFixture


def test_discover(cosmosdb: Path, snapshot: SnapshotFixture):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            str(cosmosdb),
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


def test_spec(cosmosdb: Path, snapshot: SnapshotFixture):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            str(cosmosdb),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
