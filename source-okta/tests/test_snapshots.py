import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from estuary_cdk.utils import compare_capture_records

# Timestamps Okta moves on activity unrelated to the connector: a login, a
# membership change to the Everyone group, a password rotation.
FIELDS_TO_REDACT = [
    "lastLogin",
    "lastUpdated",
    "lastMembershipUpdated",
    "passwordChanged",
    "statusChanged",
]


def _redact_fields(rec: list | dict[str, Any]) -> None:
    if isinstance(rec, list):
        for element in rec:
            _redact_fields(element)
    elif isinstance(rec, dict):
        for key, value in rec.items():
            if key in FIELDS_TO_REDACT:
                rec[key] = "redacted"
            else:
                _redact_fields(value)


def _bound_streams(source: Path) -> set[str]:
    """The `target` of every binding in the test catalog."""
    return {
        line.split(":", 1)[1].strip()
        for line in source.read_text().splitlines()
        if line.strip().startswith("target:")
    }


def test_capture(request, snapshot):
    OMITTED_STREAMS = [
        # Okta retains System Log events for 90 days, so the oldest event, and
        # with it the snapshotted record, rotates daily once the sandbox org
        # is older than that.
        "acmeCo/system_log",
    ]

    source = Path(request.fspath.dirname) / ".." / "test.flow.yaml"
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            str(source),
            "--sessions",
            "1",
            # Long enough for application_users, which makes one request per
            # app, to land in the single transaction.
            "--delay",
            "90s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # A binding that emits nothing would otherwise vanish from the snapshot
    # instead of failing the test.
    assert {line[0] for line in lines} == _bound_streams(source)

    # Bindings run concurrently, so the snapshotted record for each stream is
    # the one with the lowest id rather than the first one seen.
    selected_by_stream: dict[str, list] = {}

    for line in lines:
        stream, rec = line[0], line[1]
        if stream in OMITTED_STREAMS:
            continue

        selected = selected_by_stream.get(stream)
        if selected is None or rec["id"] < selected[1]["id"]:
            selected_by_stream[stream] = line

    unique_stream_lines = list(selected_by_stream.values())

    for l in unique_stream_lines:
        rec = l[1]
        if "row_id" in rec["_meta"]:
            rec["_meta"]["row_id"] = 0
        _redact_fields(rec)

    sorted_unique_lines = sorted(unique_stream_lines, key=lambda l: l[0])

    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__capture.stdout.json"
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        assert snapshot("capture.stdout.json") == sorted_unique_lines
    else:
        # New fields are allowed; missing or changed fields fail.
        expected = json.loads(snapshot_path.read_text())
        errors = compare_capture_records(actual=sorted_unique_lines, expected=expected)
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
