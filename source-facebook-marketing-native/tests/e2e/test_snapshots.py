"""
End-to-end tests using flowctl to verify connector spec and discover.

These tests run the actual connector through flowctl and compare output
against stored snapshots. They require flowctl to be installed and the
connector to be properly configured with test.flow.yaml.
"""

import json
import subprocess


def test_discover(request, snapshot):
    """Test that flowctl discover returns expected bindings.

    Runs flowctl raw discover against the connector and verifies the
    discovered bindings match the stored snapshot.
    """
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../../test.flow.yaml",
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
    """Test that flowctl spec returns expected connector specification.

    Runs flowctl raw spec against the connector and verifies the
    specification matches the stored snapshot.
    """
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../../test.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
