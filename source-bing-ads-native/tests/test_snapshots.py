import json
import subprocess

import pytest
from pytest_insta import SnapshotFixture


# There's no capture test yet: Estuary's Microsoft OAuth app credentials aren't
# available, so config.yaml holds placeholders and can't authenticate. Add
# test_capture (copied from source-front) once a sops-encrypted config.yaml with
# real sandbox credentials exists.


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