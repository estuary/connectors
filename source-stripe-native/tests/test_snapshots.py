import json
import subprocess
from pathlib import Path

import pytest

from estuary_cdk.utils import compare_capture_records


def test_capture(request, snapshot):
    OMITTED_STREAMS = [
        # Only the past 30 days of events are available via the
        # Stripe API, so we cannot reliably include an event document
        # in the capture snapshot.
        "acmeCo/events",
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
            "280s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()[:50]]

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen and stream not in OMITTED_STREAMS:
            unique_stream_lines.append(line)
            seen.add(stream)

    for l in unique_stream_lines:
        typ, rec = l[0], l[1]

        if typ == "acmeCo/Charges":
            rec["receipt_url"] = "redacted"
        elif typ == "acmeCo/Invoices":
            rec["hosted_invoice_url"] = "redacted"
            rec["invoice_pdf"] = "redacted"
        elif typ == "acmeCo/CreditNotes":
            rec["pdf"] = "redacted"


    # Sort lines to keep a consistent ordering of captured bindings.
    sorted_unique_lines = sorted(unique_stream_lines, key=lambda l: l[0])

    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__stdout.json"
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        # Update snapshot or create initial baseline.
        assert snapshot("stdout.json") == sorted_unique_lines
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

    assert snapshot("stdout.json") == sorted_lines


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
