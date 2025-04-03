import json
import subprocess

import pytest_insta.format as insta_format
insta_format.FmtJson.dump = lambda _self, path, value: path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")

def test_capture(request, snapshot):
    PROPERTIES_TO_REDACT = [
        "hs_time_in_lead",
        "hs_time_in_opportunity",
        "hs_time_in_appointmentscheduled",
        "updatedAt",
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
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
        typ, rec = l[0], l[1]

        if typ == "acmeCo/property_history":
            rec["timestamp"] = "redacted"
            rec["value"] = "redacted"

        if "properties" in rec:
            for property in PROPERTIES_TO_REDACT:
                if property in rec["properties"]:
                    rec["properties"][property] = "redacted"


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
            "--emit-raw"
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
