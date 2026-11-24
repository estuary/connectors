import json
import subprocess
from typing import Callable


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
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(line) for line in result.stdout.splitlines()]

    # Keep one representative document per stream, preserving first-appearance order.
    # For the streams below, prefer the first document that has data in its nested lists, so the
    # snapshot shows their items rather than empty lists.
    def has_refund_details(record: dict) -> bool:
        refunds = record.get("refunds", [])
        return any(r.get("orderAdjustments") for r in refunds) and any(
            r.get("refundShippingLines") for r in refunds
        )

    PREFERRED: dict[str, Callable[[dict], bool]] = {
        "acmeCo/order_returns": lambda r: bool(r.get("returns")),
        "acmeCo/order_shipping_lines": lambda r: bool(r.get("shippingLines")),
        "acmeCo/order_refunds": has_refund_details,
    }
    chosen: dict[str, list] = {}
    order: list[str] = []

    for line in lines:
        stream, record = line[0], line[1]
        prefer = PREFERRED.get(stream)
        if stream not in chosen:
            chosen[stream] = line
            order.append(stream)
        elif prefer and not prefer(chosen[stream][1]) and prefer(record):
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
