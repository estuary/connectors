import json
import subprocess
from pathlib import Path


def test_capture(local_catalog):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            str(local_catalog),
            "--sessions",
            "1",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    expected = [
        json.loads(line)
        for line in (Path(__file__).parent / "fixtures" / "capture.jsonl").read_text().splitlines()
    ]
    assert len(lines) == len(expected) == 5
    assert sorted(lines, key=lambda line: line[1]["id"]) == expected


def test_discover(snapshot, local_catalog):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            str(local_catalog),
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]

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
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
