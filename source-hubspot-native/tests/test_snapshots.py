import json
import re
import subprocess


def test_capture(request, snapshot):
    FIELDS_TO_REDACT = [
        "url",
    ]

    PROPERTY_PATTERNS_TO_REDACT = [
        re.compile(r"^hs_time_in_"),
        re.compile(r"^hs_date_entered_"),
        re.compile(r"^hs_date_exited_"),
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "30s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    for l in lines:
        _collection, record = l[0], l[1]

        for key in ["properties", "propertiesWithHistory"]:
            if key in record:
                for prop in record[key].keys():
                    if any(
                        pattern.match(prop) for pattern in PROPERTY_PATTERNS_TO_REDACT
                    ):
                        record[key][prop] = "redacted"

        for field in FIELDS_TO_REDACT:
            if field in record:
                record[field] = "redacted"

    assert snapshot("stdout.json") == lines


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
