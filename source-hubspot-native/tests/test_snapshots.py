import json
import subprocess


def test_capture(request, snapshot):
    FIELDS_TO_REDACT = [
        "url",
    ]

    PROPERTIES_TO_REDACT = [
        "hs_time_in_lead",
        "hs_time_in_opportunity",
        "hs_time_in_appointmentscheduled",
        "hs_time_in_1",
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
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    for l in lines:
        _collection, record = l[0], l[1]

        for m in ["properties", "propertiesWithHistory"]:
            for prop in PROPERTIES_TO_REDACT:
                if m in record and prop in record[m]:
                    record[m][prop] = "redacted"

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
