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
    stdout = []
    for line in result.stdout.splitlines():
        stdout.append(json.loads(line))

    assert snapshot("stdout.json") == json.loads(
        json.dumps(
            stdout,
            indent=2,
            sort_keys=True,
        )
    )


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
    stdout = []
    for line in result.stdout.splitlines():
        stdout.append(json.loads(line))

    assert snapshot("stdout.json") == json.loads(
        json.dumps(
            stdout,
            indent=2,
            sort_keys=True,
        )
    )


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
    stdout = []
    for line in result.stdout.splitlines():
        stdout.append(json.loads(line))

    assert snapshot("stdout.json") == json.loads(
        json.dumps(
            stdout,
            indent=2,
            sort_keys=True,
        )
    )
