import json
import subprocess

from source_salesforce_native.supported_standard_objects import SUPPORTED_STANDARD_OBJECTS



def test_capture(request, snapshot):
    TIMESTAMP_FIELDS = [
        "SystemModstamp",
        "LastModifiedDate",
        "CreatedDate",
        "LoginTime",
        "LastLoginDate",
        "LastPasswordChangeDate",
        "LastReferenceDate",
        "LastViewedDate",
        "PasswordExpirationDate",
        "my_time_field__c",
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

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    for l in unique_stream_lines:
        stream, rec = l[0], l[1]

        for field in TIMESTAMP_FIELDS:
            if field in rec:
                rec[field] = "redacted"

    # Sort lines to keep a consistent ordering of captured bindings.
    sorted_unique_lines = sorted(unique_stream_lines, key=lambda l: l[0])

    assert snapshot("capture.stdout.json") == sorted_unique_lines


def test_discover(request, snapshot):
    enabled_bindings: list[str] = []
    for name in SUPPORTED_STANDARD_OBJECTS.keys():
        if SUPPORTED_STANDARD_OBJECTS[name].get("enabled_by_default", False):
            enabled_bindings.append(name)

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
    enabled_lines = [l for l in lines if l["recommendedName"] in enabled_bindings]

    assert snapshot("capture.stdout.json") == enabled_lines


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
