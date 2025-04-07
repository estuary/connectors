import json
import subprocess


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
            "120s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()[:50]]

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    for l in unique_stream_lines:
        typ, rec = l[0], l[1]

        if typ == "acmeCo/Charges":
            rec["receipt_url"] = "redacted"
        elif typ == "acmeCo/Invoices":
            rec["hosted_invoice_url"] = "redacted"
            rec["invoice_pdf"] = "redacted"
        elif typ == "acmeCo/CreditNotes":
            rec["pdf"] = "redacted"
            

    # Sort lines to keep a consistent ordering of captured bindings.
    sorted_unique_lines = sorted(unique_stream_lines, key=lambda l: l[0])

    assert snapshot("stdout.json") == sorted_unique_lines


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

    # Sort lines to keep a consistent ordering of discovered bindings.
    sorted_lines = sorted(lines, key=lambda l: l["recommendedName"])

    assert snapshot("stdout.json") == sorted_lines


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
