import json
import subprocess


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
    RETURNS_STREAM = "acmeCo/order_returns"
    VARIANTS_STREAM = "acmeCo/product_variants"
    chosen: dict[str, list] = {}
    order: list[str] = []

    def has_variant_media(record) -> bool:
        return any(variant.get("media") for variant in record.get("variants", []))

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
        elif (
            stream == VARIANTS_STREAM
            and not has_variant_media(chosen[stream][1])
            and has_variant_media(record)
        ):
            # Prefer the first product whose variants carry media. Most products have
            # none, and media is nested two connections deep, so this is the only
            # document that covers attaching it to the right variant by __parentId.
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
