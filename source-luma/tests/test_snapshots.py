import json
import subprocess

# Fields redacted from capture snapshots: guests and calendar admins are real
# people. Anything listed here must also be redacted in the bruno collection's
# after-response script (bruno/opencollection.yml) with the same marker so the
# two stay in sync.
REDACTED = "REDACTED"
REDACTED_FIELDS = {"email", "user_email", "phone_number"}


def _redact(value):
    if isinstance(value, dict):
        return {
            k: (REDACTED if k in REDACTED_FIELDS else _redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


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
            # The guests snapshot lists every event's guests one event at a
            # time, so it needs well over the default 10s to finish a sweep.
            "--delay",
            "60s",
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
            unique_stream_lines.append(_redact(line))
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
