import json
import subprocess

SUBSCRIPTION_FIELDS_TO_REDACT = [
    'billing_period_end_date',
    'billing_period_start_date',
    'next_billing_date',
    'paid_through_date',
    'updated_at',
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
            "25s",
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

        if 'evidence' in rec:
            evidence: list = rec['evidence']
            for e in evidence:
                if 'url' in e:
                    e['url'] = 'redacted'
        if stream == 'acmeCo/subscriptions':
            for field in SUBSCRIPTION_FIELDS_TO_REDACT:
                rec[field] = 'redacted'
            rec['current_billing_cycle'] = 0

            rec['status_history'] = [rec['status_history'][-1]]
            rec['transactions'] = [rec['transactions'][-1]]

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
