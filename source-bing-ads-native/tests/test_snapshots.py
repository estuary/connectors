import json
import subprocess

import pytest
from pytest_insta import SnapshotFixture


# Copied from source-front. config.yaml holds sandbox credentials and sets
# advanced.is_sandbox.
def test_capture(request: pytest.FixtureRequest, snapshot: SnapshotFixture):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            str(request.path.parent.parent / "test.flow.yaml"),
            "--sessions",
            "1",
            "--delay",
            # ads is read last, after every campaign and ad group; with 10s
            # against the sandbox it emitted nothing.
            "60s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    assert snapshot("capture.stdout.json") == unique_stream_lines


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