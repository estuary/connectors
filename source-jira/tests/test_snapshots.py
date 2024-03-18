import json
import subprocess


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()[:50]]

    for line in lines:
        line[1]["ts"] = "redacted-timestamp"

    snapshot.assert_match(str(lines), "capture.stdout.json")


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
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    snapshot.assert_match(str(lines), "capture.stdout.json")


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
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    snapshot.assert_match(str(lines), "capture.stdout.json")
