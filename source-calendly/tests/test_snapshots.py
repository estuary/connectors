import json
import subprocess
from pathlib import Path

import pytest
from estuary_cdk.utils import compare_capture_records
from pydantic import JsonValue


def redact_nested_fields(
    input: list[JsonValue] | dict[str, JsonValue], fields: list[str]
):
    if isinstance(input, list):
        for element in input:
            if isinstance(element, (list, dict)):
                redact_nested_fields(element, fields)
    elif isinstance(input, dict):
        for key in list(input.keys()):
            if key in fields:
                input[key] = "redacted"
            elif isinstance(input[key], (list, dict)):
                redact_nested_fields(input[key], fields)


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    FIELDS_TO_REDACT = ["updated_at"]

    for l in lines:
        _collection, record = l[0], l[1]
        redact_nested_fields(record, FIELDS_TO_REDACT)

    snapshot_path = (
        Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__stdout.json"
    )
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        # Update snapshot or create initial baseline.
        assert snapshot("stdout.json") == lines
    else:
        # Compare capture snapshots. New fields are allowed, but missing or changed fields cause a failure.
        expected = json.loads(snapshot_path.read_text())
        errors = compare_capture_records(actual=lines, expected=expected)
        if errors:
            pytest.fail("Capture snapshots are different:\n" + "\n".join(errors))


def test_discover(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0

    lines = sorted(
        [json.loads(l) for l in result.stdout.splitlines()],
        key=lambda l: l["recommendedName"],
    )

    assert snapshot("stdout.json") == lines


def test_spec(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("stdout.json") == lines
