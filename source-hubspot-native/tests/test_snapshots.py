import json
import re
import subprocess
from pathlib import Path

import pytest

from estuary_cdk.utils import compare_capture_records


def test_capture(request, snapshot):
    FIELDS_TO_REDACT = [
        "url",
    ]

    PROPERTY_PATTERNS_TO_REDACT = [
        re.compile(r"^hs_time_in_"),
        re.compile(r"^hs_date_entered_"),
        re.compile(r"^hs_date_exited_"),
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
            "30s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    for l in lines:
        _collection, record = l[0], l[1]

        for key in ["properties", "propertiesWithHistory"]:
            if key in record:
                for prop in record[key].keys():
                    if any(
                        pattern.match(prop) for pattern in PROPERTY_PATTERNS_TO_REDACT
                    ):
                        record[key][prop] = "redacted"

        for field in FIELDS_TO_REDACT:
            if field in record:
                record[field] = "redacted"

    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__stdout.json"
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
    lines = [json.loads(l) for l in result.stdout.splitlines()]

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
