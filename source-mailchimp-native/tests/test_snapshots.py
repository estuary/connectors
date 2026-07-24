import json
import subprocess

# Fields redacted from capture snapshots. Anything listed here must also be
# redacted in the bruno collection's saved examples (see the redaction note in
# bruno/opencollection.yml) so the two stay in sync. The bruno list
# additionally carries `_links`: captured documents no longer have it
# (`fetch_collection_page` excludes it server-side), but bruno's raw probe
# responses still do.
REDACTED = "REDACTED"
REDACTED_FIELDS = {"stats"}


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

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    assert snapshot("capture.stdout.json") == _redact(unique_stream_lines)


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
