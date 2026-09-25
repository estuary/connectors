import json
import subprocess
from pathlib import Path

import pytest

from estuary_cdk.utils import compare_capture_records


FIELDS_TO_REDACT = [
    "available_product_features",
    "created_at",
    "last_calculation",
    "last_seen_at",
    "pending_version",
    "updated_at",
    "version",
]


def redact_nested_fields(value: list | dict) -> None:
    """
    Recursively redact volatile fields wherever they appear so snapshots stay
    stable across captures. PostHog embeds organization and team objects inside
    other records, so redacting only the top level leaves the nested copies to
    churn.
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
            "30s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # One document per stream keeps the snapshot small and resilient to churn
    # in the dev account. The first document a stream emits is its oldest,
    # since incremental streams are walked in cursor order, so it is the least
    # likely to be displaced as new data arrives.
    unique_stream_lines = []
    seen = set()

    for l in lines:
        stream = l[0]
        if stream not in seen:
            unique_stream_lines.append(l)
            seen.add(stream)

    for l in unique_stream_lines:
        _collection, record = l[0], l[1]

        redact_nested_fields(record)

    # Sort lines to keep a consistent ordering of captured bindings. The
    # comparison below is positional, so a reordered run would otherwise
    # report every stream as mismatched rather than as a reordering.
    lines = sorted(unique_stream_lines, key=lambda l: l[0])

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
