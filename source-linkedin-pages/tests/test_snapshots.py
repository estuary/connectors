import json
import subprocess


def test_capture(request):
    """
    This test doesn't have a snapshot
    because the output data changes too frequently.
    Almost all field values changes between test executions.
    """
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
            "--output",
            "json",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0


def test_discover(request, snapshot):
    snapshot.snapshot_dir = request.fspath.dirname + "/snapshots"

    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
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


def test_spec(request, snapshot):
    snapshot.snapshot_dir = request.fspath.dirname + "/snapshots"

    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--output",
            "json",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0

    snapshot.assert_match(
        json.dumps(json.loads(result.stdout), indent=2),
        "spec.stdout.json",
    )
