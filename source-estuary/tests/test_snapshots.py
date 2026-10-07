import json
import subprocess
from pathlib import Path

import pytest
from estuary_cdk.utils import compare_capture_records
from pydantic import JsonValue

# Personal data, and fields of the live control plane that change between runs.
FIELDS_TO_REDACT = [
    "email",
    "createdByEmail",
    "destination",
    "recipients",
    "userEmail",
    "userFullName",
    "userId",
    "lastModifiedBy",
    "updatedAt",
    "lastBuildId",
    "lastPubId",
    "lastUsedAt",
    "resolvedAt",
    "resolvedArguments",
    "controller",
    "connector",
    "summary",
]


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
            "raw",
            "preview-next",
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

    # The first document of each stream: the live control plane changes too
    # often to snapshot whole streams.
    unique_stream_lines = []
    seen = set()
    for line in lines:
        stream = line[0]
        if stream not in seen:
            redact_nested_fields(line[1], FIELDS_TO_REDACT)
            unique_stream_lines.append(line)
            seen.add(stream)

    snapshot_path = (
        Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__stdout.json"
    )
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        assert snapshot("stdout.json") == unique_stream_lines
    else:
        # New fields are allowed, but missing or changed fields cause a failure.
        expected = json.loads(snapshot_path.read_text())
        errors = compare_capture_records(actual=unique_stream_lines, expected=expected)
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
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines


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

    assert snapshot("capture.stdout.json") == lines