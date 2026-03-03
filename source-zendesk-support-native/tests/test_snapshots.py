import json
import subprocess
from pathlib import Path

import pytest

from estuary_cdk.utils import compare_capture_records


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
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "100s",
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

        rec['_meta']['row_id'] = 0
        if "updated_at" in rec:
            rec["updated_at"] = "redacted"
        if "last_login_at" in rec:
            rec["last_login_at"] = "redacted"

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
