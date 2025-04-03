import json
import subprocess


def test_capture(request, snapshot):
    OMITTED_STREAMS = [
        "acmeCo/tags",
    ]

    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "27",
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
        if stream not in seen and stream not in OMITTED_STREAMS:
            unique_stream_lines.append(line)
            seen.add(stream)

    for l in unique_stream_lines:
        stream, rec = l[0], l[1]

        rec['_meta']['row_id'] = 0
        if "updated_at" in rec:
            rec["updated_at"] = "redacted"
        if "last_login_at" in rec:
            rec["last_login_at"] = "redacted"

        if stream == "acmeCo/ticket_audits":
            rec["id"] = "redacted"
            rec["created_at"] = "redacted"
            rec["events"] = "redacted"
            rec["ticket_id"] = "redacted"

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
