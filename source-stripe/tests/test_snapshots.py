import json
import subprocess


def test_capture(flow_path, snapshot):
    snapshot.snapshot_dir = "tests/snapshots"

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            flow_path,
            "--sessions",
            "1",
            "--delay",
            "10s",
            "--output",
            "json",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0

    stdout = []
    for line in result.stdout.splitlines()[:50]:
        stdout.append(json.loads(line))

    snapshot.assert_match(
        json.dumps(stdout, indent=2),
        "capture.stdout.json",
    )


def test_discover(flow_path, snapshot):
    snapshot.snapshot_dir = "tests/snapshots"

    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            flow_path,
            "--output",
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

    snapshot.assert_match(
        json.dumps(stdout, indent=2),
        "discover.stdout.json",
    )


def test_spec(flow_path, snapshot):
    snapshot.snapshot_dir = "tests/snapshots"
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--output",
            "json",
            "--source",
            flow_path,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0

    snapshot.assert_match(
        json.dumps(json.loads(result.stdout), indent=2),
        "spec.stdout.json",
    )
