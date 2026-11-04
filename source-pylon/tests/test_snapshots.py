import json
import subprocess
from typing import Any

import pytest


def test_discover(request: pytest.FixtureRequest, snapshot: Any):
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


def test_spec(request: pytest.FixtureRequest, snapshot: Any):
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
