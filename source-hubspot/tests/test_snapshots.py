import json
import subprocess
from pathlib import Path

import pytest
import pytest_insta.format as insta_format

from estuary_cdk.utils import compare_capture_records, compare_values

insta_format.FmtJson.dump = lambda _self, path, value: path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")


def compare_discover_records(actual: list, expected: list) -> list[str]:
    """
    Compare discover output records. Extra keys in documentSchema.properties are allowed
    (for when HubSpot adds new fields), but all expected keys must exist with matching values.
    """
    errors: list[str] = []

    if len(actual) != len(expected):
        errors.append(f"Record count: got {len(actual)}, expected {len(expected)}")
        return errors

    for i, (actual_record, expected_record) in enumerate(zip(actual, expected)):
        name = expected_record.get("recommendedName", f"record[{i}]")
        errors.extend(compare_values(actual_record, expected_record, name))

    return errors


def test_capture(request, snapshot):
    PROPERTIES_TO_REDACT = [
        "hs_time_in_lead",
        "hs_time_in_opportunity",
        "hs_time_in_appointmentscheduled",
        "updatedAt",
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
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

    for l in unique_stream_lines:
        typ, rec = l[0], l[1]

        if typ == "acmeCo/property_history":
            rec["timestamp"] = "redacted"
            rec["value"] = "redacted"

        if "properties" in rec:
            for property in PROPERTIES_TO_REDACT:
                if property in rec["properties"]:
                    rec["properties"][property] = "redacted"


    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__capture.stdout.json"
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
            "--emit-raw"
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    snapshot_path = Path(request.fspath.dirname) / "snapshots" / "snapshots__discover__capture.stdout.json"
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        # Update snapshot or create initial baseline.
        assert snapshot("capture.stdout.json") == lines
    else:
        # Compare discover snapshots. New fields are allowed, but missing or changed fields cause a failure.
        expected = json.loads(snapshot_path.read_text())
        errors = compare_discover_records(actual=lines, expected=expected)
        if errors:
            pytest.fail("Discover snapshots are different:\n" + "\n".join(errors))


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
