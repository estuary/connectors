import json
import subprocess

# Contact details of real sandbox users; the snapshot is committed to a public
# repository, so they are replaced with a marker rather than recorded.
FIELDS_TO_REDACT = [
    "emailAddress",
    "emailAliases",
    "phoneNumber",
    "personalMeetingUrls",
    "meetingConsentPageUrl",
    "trustedEmailAddress",
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

    # One representative document per stream, chosen deterministically so the
    # snapshot does not churn on document ordering between runs.
    by_stream: dict[str, list] = {}
    for stream, doc in lines:
        by_stream.setdefault(stream, []).append(doc)

    unique_stream_lines = []
    for stream in sorted(by_stream):
        docs = sorted(by_stream[stream], key=lambda d: json.dumps(d, sort_keys=True))
        doc = docs[0]
        for field in FIELDS_TO_REDACT:
            if field in doc:
                doc[field] = "redacted"
        unique_stream_lines.append([stream, doc])

    assert snapshot("stdout.json") == unique_stream_lines


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

    lines = sorted(
        [json.loads(l) for l in result.stdout.splitlines()],
        key=lambda l: l["recommendedName"],
    )

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
