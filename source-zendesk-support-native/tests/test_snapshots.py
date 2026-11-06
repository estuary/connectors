import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from estuary_cdk.utils import compare_capture_records


FIELDS_TO_REDACT = [
    "updated_at",
    "last_login_at",
    "assignee_updated_at",
    "generated_timestamp",
    "fields",
    "custom_fields",
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


def test_capture(request, snapshot):
    OMITTED_STREAMS = [
        "acmeCo/audit_logs",
        "acmeCo/tags",
        # The Zendesk API only returns the past 30 days of ticket_activities,
        # so we can't reliably include ticket_activities in the capture snapshot.
        "acmeCo/ticket_activities",
    ]

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
            "180s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # Some streams emit in no particular order, so the snapshotted record for each
    # stream is the one with the lowest id. Streams without ids keep their first record.
    selected_by_stream: dict[str, list] = {}

    for line in lines:
        stream, rec = line[0], line[1]
        if stream in OMITTED_STREAMS:
            continue

        selected = selected_by_stream.get(stream)
        if selected is None or ("id" in rec and rec["id"] < selected[1]["id"]):
            selected_by_stream[stream] = line

    unique_stream_lines = list(selected_by_stream.values())

    for l in unique_stream_lines:
        stream, rec = l[0], l[1]

        rec['_meta']['row_id'] = 0
        _redact_fields(rec)

    # Sort lines to keep a consistent ordering of captured bindings.
    sorted_unique_lines = sorted(unique_stream_lines, key=lambda l: l[0])

    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__capture.stdout.json"
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        # Update snapshot or create initial baseline.
        assert snapshot("capture.stdout.json") == sorted_unique_lines
    else:
        # Compare capture snapshots. New fields are allowed, but missing or changed fields cause a failure.
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

    # Sort lines to keep a consistent ordering of discovered bindings.
    sorted_lines = sorted(lines, key=lambda l: l["recommendedName"])

    assert snapshot("capture.stdout.json") == sorted_lines


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
