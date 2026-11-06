import json
import subprocess

import pytest
from pytest_insta import SnapshotFixture


def test_discover(request: pytest.FixtureRequest, snapshot: SnapshotFixture):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            str(request.path.parent.parent / "test.flow.yaml"),
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines


def test_spec(request: pytest.FixtureRequest, snapshot: SnapshotFixture):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            str(request.path.parent.parent / "test.flow.yaml"),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines