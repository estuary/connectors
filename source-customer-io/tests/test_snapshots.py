import json
import subprocess

# Three reasons a field is redacted here.
#
# Volatile -- differs between two runs of the same capture, so leaving it in
# would fail this snapshot for reasons unrelated to any change in the connector.
#
# Identifying -- carries real addresses or ids from the test workspace, and this
# snapshot is committed to a public repository.
#
# Bulky -- `body` alone is ten thousand characters of rendered HTML, which would
# be two thirds of the snapshot and tells a reviewer nothing.
FIELDS_TO_REDACT = [
    # volatile
    "created",
    "created_at",
    "deduplicate_id",
    "first_started",
    "metrics",
    "updated",
    "updated_at",
    # identifying
    "cio_id",
    "customer_id",
    "customer_identifiers",
    "recipient",
    "subject",
    # bulky
    "body",
    "body_amp",
    "body_plain",
]


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

    # One document per collection keeps the snapshot readable and stops a busy
    # workspace from rewriting it on every run.
    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream, record = line[0], line[1]

        for field in FIELDS_TO_REDACT:
            if field in record:
                record[field] = "redacted"

        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    assert snapshot("capture.stdout.json") == unique_stream_lines


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
