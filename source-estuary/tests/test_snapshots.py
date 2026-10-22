import json
import subprocess
from pathlib import Path

import pytest

CATALOG_STATS_STREAMS = [
    "acmeCo/catalog_stats_hourly",
    "acmeCo/catalog_stats_daily",
    "acmeCo/catalog_stats_monthly",
]


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            # Run for a fixed window rather than one transaction: a single
            # transaction can close before the slower streams emit.
            "--timeout",
            "60s",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # The capture reads the live control plane, whose values change with
    # every publication, alert and stats bucket. Snapshot each stream's
    # top-level field names instead: those are fixed by the GraphQL selections
    # and models, so a dropped or renamed field, or a stream that stops
    # emitting, still fails the test.
    fields: dict[str, set[str]] = {}
    for stream, doc in lines:
        fields.setdefault(stream, set()).update(doc.keys())
    shapes = {stream: sorted(names) for stream, names in sorted(fields.items())}

    # UInt64 counters arrive as decimal strings and must be emitted as integers.
    for stream, doc in lines:
        if stream in CATALOG_STATS_STREAMS:
            summary = doc["statsSummary"]
            assert isinstance(summary["writtenToMe"]["docsTotal"], int), doc
            assert isinstance(summary["usageSeconds"], int), doc

    snapshot_path = (
        Path(request.fspath.dirname) / "snapshots" / "snapshots__capture__stdout.json"
    )
    insta_mode = request.config.getoption("--insta", default=None)

    if insta_mode == "update" or not snapshot_path.exists():
        assert snapshot("stdout.json") == shapes
    else:
        # A stream that later gains data (e.g. service_accounts) may appear;
        # every snapshotted stream must still emit exactly its fields.
        expected = json.loads(snapshot_path.read_text())
        errors = [
            f"{stream}: expected fields {names}, got {shapes.get(stream)}"
            for stream, names in expected.items()
            if shapes.get(stream) != names
        ]
        if errors:
            pytest.fail("Capture shapes are different:\n" + "\n".join(errors))


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