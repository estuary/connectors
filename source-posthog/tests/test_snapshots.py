"""Snapshot tests for PostHog connector using flowctl."""

import json
import subprocess


def test_spec(request, snapshot):
    """Test connector spec output matches snapshot."""
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../test.flow.local.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"flowctl spec failed: {result.stderr}"
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("spec.json") == lines


def test_discover(request, snapshot):
    """Test connector discover output matches snapshot."""
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.local.yaml",
            "-o",
            "json",
            "--emit-raw",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"flowctl discover failed: {result.stderr}"
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("discover.json") == lines
