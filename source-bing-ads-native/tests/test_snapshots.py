import json
import subprocess

import pytest
from pydantic import JsonValue
from pytest_insta import SnapshotFixture

# Values that change without anyone touching the captured entity, so they
# would make the snapshot fail on every re-run. The fields stay in the
# snapshot; only their values are replaced.
FIELDS_TO_REDACT = [
    # Accounts: bumped by any edit to the account, including its settings.
    "LastModifiedTime",
    "TimeStamp",
    # Ads and asset groups: Microsoft recomputes each asset's label itself.
    "AssetPerformanceLabel",
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
            redact_nested_fields(line[1], FIELDS_TO_REDACT)
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