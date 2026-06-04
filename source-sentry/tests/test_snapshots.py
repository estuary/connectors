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
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # Keep only the first captured document per stream so the snapshot stays
    # small and stable regardless of how many rows a stream returns.
    unique_stream_lines = []
    seen = set()
    for line in lines:
        stream = line[0]
        if stream in seen:
            continue
        seen.add(stream)
        line[1]["stats"] = "redacted-object"
        # Sentry toggles org/project feature flags server-side over time, so
        # the `features` list drifts independently of connector behavior.
        if "features" in line[1]:
            line[1]["features"] = "redacted-list"
        unique_stream_lines.append(line)

    unique_stream_lines.sort(key=lambda l: l[0])

    assert snapshot("stdout.json") == unique_stream_lines


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
