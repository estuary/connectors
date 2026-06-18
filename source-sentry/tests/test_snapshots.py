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

    # These fields drift independently of connector behavior: `stats` and
    # `features` are server-side aggregates/flags Sentry toggles over time, and
    # the event-aggregate timestamps move as seeded events age out of the
    # retention window. Redact every occurrence at any nesting depth (e.g.
    # `stats` also appears under `lifetime`).
    FIELDS_TO_REDACT = {
        "stats",
        "features",
        "firstEvent",
        "lastEvent",
        "firstSeen",
        "lastSeen",
    }

    def redact(value):
        if isinstance(value, dict):
            return {
                key: "redacted" if key in FIELDS_TO_REDACT else redact(val)
                for key, val in value.items()
            }
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value

    # Keep only the first captured document per stream so the snapshot stays
    # small and stable regardless of how many rows a stream returns.
    unique_stream_lines = []
    seen = set()
    for line in lines:
        stream = line[0]
        if stream in seen:
            continue
        seen.add(stream)
        line[1] = redact(line[1])
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
