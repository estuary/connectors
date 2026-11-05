import json
import subprocess

from pydantic import JsonValue

# Aircall only serves six months of calls, so the test account's seeded calls
# eventually age out of the API and the stream goes quiet.
STREAMS_EXCLUDED_FROM_CAPTURE = {"acmeCo/calls"}

FIELDS_TO_REDACT = [
    # Live user and number status.
    "availability",
    "availability_status",
    "substatus",
    "available",
    "open",
    # Company counters.
    "users_count",
    "numbers_count",
]


def redact_nested_fields(
    input: list[JsonValue] | dict[str, JsonValue], fields: list[str]
):
    if isinstance(input, list):
        for element in input:
            if isinstance(element, (list, dict)):
                redact_nested_fields(element, fields)
    elif isinstance(input, dict):
        for key in list(input.keys()):
            if key in fields:
                input[key] = "redacted"
            elif isinstance(input[key], (list, dict)):
                redact_nested_fields(input[key], fields)


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
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

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen and stream not in STREAMS_EXCLUDED_FROM_CAPTURE:
            redact_nested_fields(line[1], FIELDS_TO_REDACT)
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
