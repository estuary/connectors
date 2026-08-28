"""Measures the sidecar's per-batch CPU cost of an append, old framing vs new.

The old framing carried rows inside the request, so the reader parsed a whole
batch into dicts which the SDK's encoder immediately re-serialized to the NDJSON
its native core wanted. The new framing carries them as an opaque payload which
_decode_rows turns into one msgspec.Raw per row, and the encoder writes those out
verbatim. Both are measured here, over the same rows, as the sidecar performs
them: everything from the request arriving to the bytes the FFI channel is handed.

This matters beyond the CPU saved. Parsing happens on the sidecar's single reader
thread, so before the change that one thread — not the per-channel workers — was
what capped the whole sidecar's throughput.

Run it with the SDK's msgspec available:

    PYTHONPATH=path/to/sidecar python3 tests/bench_append.py
"""

import json
import time
from typing import List

import msgspec
from msgspec import Raw

from snowpipe_sidecar.channels import _decode_rows

ROWS = 10_000
REPS = 5


def document(i):
    """A flow_document of the shape a materialized collection actually has, which
    is where the round trip cost the most: nested objects and arrays reach the
    connector already encoded, so parsing them into Python objects and rebuilding
    them was pure waste."""
    return {
        "id": f"order-{i}",
        "created_at": "2026-08-03T18:00:00Z",
        "status": "shipped",
        "customer": {
            "id": i,
            "name": f"Customer {i}",
            "email": f"c{i}@example.com",
            "address": {"line1": "123 Main St", "city": "Springfield", "zip": "01101", "country": "US"},
        },
        "items": [
            {"sku": f"sku-{i}-{j}", "qty": j + 1, "price": 9.99 * (j + 1), "tags": ["a", "b", "c"]}
            for j in range(4)
        ],
        "totals": {"subtotal": 123.45, "tax": 9.87, "shipping": 4.99, "grand": 138.31},
        "meta": {"source": "web", "campaign": None, "flags": {"gift": False, "expedite": True}},
    }


def measure(fn):
    def once():
        started = time.process_time()
        fn()
        return time.process_time() - started

    return min(once() for _ in range(REPS))


def main():
    encoder = msgspec.json.Encoder()
    rows = [{"ID": i, "TS": "2026-08-03T18:00:00Z", "FLOW_DOCUMENT": document(i)} for i in range(ROWS)]
    encoded = [encoder.encode(row) for row in rows]

    old_request = json.dumps({
        "id": 1,
        "op": "append",
        "params": {"channel": "ch", "start_token": "1", "end_token": str(ROWS), "rows": rows},
    }) + "\n"
    payload = b"[" + b",".join(encoded) + b"]"

    def old():
        encoder.encode_lines(json.loads(old_request)["params"]["rows"])

    def new():
        encoder.encode_lines(_decode_rows(payload, ROWS))

    # Same bytes reach the SDK either way, which is what makes the two comparable.
    assert encoder.encode_lines(json.loads(old_request)["params"]["rows"]) == encoder.encode_lines(
        _decode_rows(payload, ROWS)
    )

    old_ms, new_ms = measure(old) * 1000, measure(new) * 1000
    print(f"{ROWS} rows, {len(payload) / ROWS:.0f} B/row, {len(payload) / 2**20:.2f} MiB")
    print(f"  request-carried rows (old): {old_ms:7.2f} ms")
    print(f"  opaque payload       (new): {new_ms:7.2f} ms  ({old_ms / new_ms:.1f}x cheaper)")


if __name__ == "__main__":
    main()
