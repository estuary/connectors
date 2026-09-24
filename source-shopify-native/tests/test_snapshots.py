import json
import subprocess
import sys


def sanitize_tokens(data):
    if isinstance(data, dict):
        for key, value in list(data.items()):
            if isinstance(value, str):
                if "?token=" in value:
                    parts = value.split("?token=", 1)
                    if len(parts) > 1:
                        remaining = parts[1].split("&", 1)
                        if len(remaining) > 1:
                            data[key] = f"{parts[0]}?token=REDACTED&{remaining[1]}"
                        else:
                            data[key] = f"{parts[0]}?token=REDACTED"
            if isinstance(value, (dict, list)):
                sanitize_tokens(value)
    elif isinstance(data, list):
        for item in data:
            sanitize_tokens(item)


def test_capture(request, snapshot):
    FIELDS_TO_REDACT = [
        "updatedAt"
    ]

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
            "250s",
            # Task logs are written to stderr as JSON at the task's log level. Without this flag,
            # flowctl filters them at WARN, which drops the connector's info logs checked below.
            "--log-json",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    task_logs = []
    for line in result.stderr.splitlines():
        try:
            log = json.loads(line)
        except json.JSONDecodeError:
            log = None
        if isinstance(log, dict) and "message" in log:
            task_logs.append(log)
            if log.get("level") not in ("warn", "error"):
                continue
        # Pass warnings, errors, and flowctl's own output through, which is what this test showed
        # before it captured stderr.
        sys.stderr.write(line + "\n")

    assert result.returncode == 0

    # Shopify must keep the marker comment the connector adds to each bulk query, since that's how
    # the connector finds its own jobs to cancel on restart. The connector warns when it doesn't.
    # The first check makes sure bulk jobs ran and their logs were read, so the second can't pass
    # vacuously.
    messages = [log["message"] for log in task_logs]
    assert any("has completed." in m for m in messages)
    marker_warnings = [m for m in messages if "did not keep the bulk query marker" in m]
    assert not marker_warnings, marker_warnings

    lines = [json.loads(line) for line in result.stdout.splitlines()]

    # Keep one representative document per stream, preserving first-appearance order.
    RETURNS_STREAM = "acmeCo/order_returns"
    chosen: dict[str, list] = {}
    order: list[str] = []

    for line in lines:
        stream, record = line[0], line[1]
        if stream not in chosen:
            chosen[stream] = line
            order.append(stream)
        elif (
            stream == RETURNS_STREAM
            and not chosen[stream][1].get("returns")
            and record.get("returns")
        ):
            # Prefer the first order that actually has returns so the snapshot
            # covers the nested return reassembly.
            chosen[stream] = line

    unique_stream_lines = []
    for stream in order:
        record = chosen[stream][1]
        sanitize_tokens(record)

        for field in FIELDS_TO_REDACT:
            if field in record:
                record[field] = "redacted"

        unique_stream_lines.append(chosen[stream])

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
    lines = [json.loads(line) for line in result.stdout.splitlines()]

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
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines
