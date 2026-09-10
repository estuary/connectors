"""Pins the SDK behaviour the append path leans on.

The sidecar hands `append_rows` one `msgspec.Raw` per row so that a row's bytes
are never materialized as a Python object. Snowflake documents `append_rows` as
taking `List[Dict[str, Any]]` and enumerates the value types it accepts;
`msgspec.Raw` is not among them. The route works because `append_rows` forwards
`rows` untouched to a `msgspec.json.Encoder`, whose `Raw` support is documented
as emitting pre-encoded messages verbatim. That is an SDK internal, so these
tests drive the installed SDK — with a stub in place of its Rust FFI channel, so
no credentials are needed — and fail loudly on a version that closes the route
rather than letting it silently change what reaches Snowflake.

Run with SNOWPIPE_SDK_GUARD=required to turn a missing SDK into a failure, which
is what keeps the image build from passing over the guard it exists to enforce.
"""

import inspect
import os
from typing import List

import pytest

try:
    import msgspec
    from msgspec import Raw
    from snowflake.ingest.streaming._python_ffi import PyBinaryInputFormat
    from snowflake.ingest.streaming.streaming_ingest_channel import (
        StreamingIngestChannel,
    )
except ImportError as err:  # pragma: no cover - depends on the environment
    if os.environ.get("SNOWPIPE_SDK_GUARD") == "required":
        raise
    pytest.skip(f"snowpipe-streaming SDK not installed: {err}", allow_module_level=True)


ROWS = [
    {"KEY": 1, "VAL": "hello", "DOC": None},
    {"KEY": 2, "VAL": 'quotes "and" \\backslashes\\', "DOC": {"nested": [1, 2, {"deep": True}]}},
    {"KEY": 3, "VAL": "你好 🌟", "DOC": {"a": [], "b": {}, "c": None}},
    {"KEY": 4, "VAL": 1.7976931348623157e308, "DOC": [1, 2, 3]},
]


class StubFFIChannel:
    """Stands in for the SDK's Rust FFI channel, recording what reaches it."""

    def __init__(self):
        self.calls = []

    def append_rows(self, json_bytes, num_rows, wait_for_ack, start_token, end_token):
        self.calls.append((json_bytes, num_rows, start_token, end_token))

    def close(self, drop=False, wait_for_flush=True, timeout_seconds=None):
        pass


def append(rows):
    """Append through the real SDK wrapper, returning what its FFI channel saw."""
    ffi = StubFFIChannel()
    channel = StreamingIngestChannel(ffi, {}, _internal=True)
    try:
        channel.append_rows(rows, "start", "end")
    finally:
        # The wrapper's __del__ closes the channel it still holds, which would
        # reach the stub after the test that owns it has finished.
        channel._channel = None
    assert len(ffi.calls) == 1
    return ffi.calls[0]


def encoded(rows):
    encoder = msgspec.json.Encoder()
    return [Raw(encoder.encode(row)) for row in rows]


def test_per_row_raw_matches_the_documented_dict_path():
    from_dicts = append(ROWS)
    from_raw = append(encoded(ROWS))
    assert from_raw == from_dicts
    # Not vacuously equal: the rows really did reach the FFI channel as the
    # NDJSON block its native core wants, one line per row.
    payload, num_rows, start, end = from_raw
    assert num_rows == len(ROWS)
    assert (start, end) == ("start", "end")
    assert payload.count(b"\n") == len(ROWS)
    assert payload.startswith(b'{"KEY":1,')


def test_only_per_row_raw_round_trips():
    # The near misses, all silent rather than loud: JSON lines handed over as
    # str are ingested as quoted strings, as bytes they are base64-encoded, and
    # the whole NDJSON block as a single Raw arrives with the bytes right but a
    # row count of 1 — which would make Snowflake's offset tokens account for a
    # batch of one row.
    lines = [bytes(raw) for raw in encoded(ROWS)]
    expected, _, _, _ = append(ROWS)

    as_str, _, _, _ = append([line.decode() for line in lines])
    assert as_str != expected and as_str.startswith(b'"{')

    as_bytes, _, _, _ = append(lines)
    assert as_bytes != expected and not as_bytes.startswith(b'{"KEY"')

    block, num_rows, _, _ = append([Raw(b"\n".join(lines))])
    assert block == expected and num_rows == 1


def test_rows_are_forwarded_without_a_per_row_walk():
    # A channel whose binary input format is not BASE64 rebuilds every row dict
    # to re-encode its bytes values, which a Raw cannot survive. BASE64 is the
    # format the wrapper defaults to and the one open_channel passes on, so the
    # forwarding the Raw path needs is the default; assert it, since a changed
    # default would fail every append instead.
    default = inspect.signature(StreamingIngestChannel.__init__).parameters[
        "binary_input_format"
    ].default
    assert default is PyBinaryInputFormat.BASE64


def test_malformed_and_non_object_rows_are_the_callers_problem():
    # The encoder writes a Raw out verbatim, so neither malformed JSON nor a row
    # which is not an object is caught here. That is why the sidecar validates
    # the payload itself.
    payload, num_rows, _, _ = append([Raw(b"{not json"), Raw(b"42")])
    assert payload == b"{not json\n42\n" and num_rows == 2


def test_decode_to_raw_scans_rows_without_building_them():
    # The payload framing: one decode validates every row's syntax while
    # producing views onto the payload rather than Python objects.
    encoder = msgspec.json.Encoder()
    payload = b"[" + b",".join(encoder.encode(row) for row in ROWS) + b"]"
    rows = msgspec.json.Decoder(List[Raw]).decode(payload)
    assert [bytes(row) for row in rows] == [encoder.encode(row) for row in ROWS]

    with pytest.raises(msgspec.DecodeError):
        msgspec.json.Decoder(List[Raw]).decode(b'[{"a":1},{oops}]')
