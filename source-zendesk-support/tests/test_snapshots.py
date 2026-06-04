import json
import subprocess
from pathlib import Path

import pytest

from estuary_cdk.utils import compare_capture_records


FIELDS_TO_REDACT = [
    "updated_at",
    "last_login_at",
    "assignee_updated_at",
    "generated_timestamp",
]


def redact_nested_fields(value: list | dict) -> None:
    """
    Recursively redact volatile timestamp/cursor fields wherever they appear so
    snapshots stay stable across captures. Zendesk surfaces `updated_at` on
    nested objects (not just the record root), and incremental exports stamp a
    fresh `generated_timestamp` on every run.
    """
    if isinstance(value, list):
        for element in value:
            redact_nested_fields(element)
    elif isinstance(value, dict):
        for key, nested in value.items():
            if key in FIELDS_TO_REDACT:
                value[key] = "redacted"
            else:
                redact_nested_fields(nested)


def test_capture(request, snapshot):
    OMITTED_STREAMS = [
        "acmeCo/tags",
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "27",
            "--delay",
            "10s",
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
        if stream not in seen and stream not in OMITTED_STREAMS:
            unique_stream_lines.append(line)
            seen.add(stream)

    for l in unique_stream_lines:
        stream, rec = l[0], l[1]

        rec["_meta"]["row_id"] = 0
        redact_nested_fields(rec)

        if stream == "acmeCo/ticket_audits":
            rec["id"] = "redacted"
            rec["created_at"] = "redacted"
            rec["events"] = "redacted"
            rec["ticket_id"] = "redacted"

    snapshot_path = (
        Path(request.fspath.dirname)
        / "snapshots"
        / "snapshots__capture__capture.stdout.json"
    )
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        # Update snapshot or create initial baseline.
        assert snapshot("capture.stdout.json") == unique_stream_lines
    else:
        # Compare capture snapshots. New fields are allowed, but missing or changed fields cause a failure.
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
