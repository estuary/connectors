import json
import logging

from estuary_cdk.logger import LogFormatter


def _fmt(record: logging.LogRecord) -> dict:
    return json.loads(LogFormatter().format(record))


def test_percent_args_are_rendered():
    # Lazy %-style logging (as used by gunicorn and much of the stdlib) must be
    # interpolated into `msg`, not emitted as a raw template.
    r = logging.LogRecord(
        "flow", logging.INFO, __file__, 1, "pid: %s", (1234,), None
    )
    out = _fmt(r)
    assert out["msg"] == "pid: 1234"
    # Raw args are still retained as a structured field.
    assert out["fields"]["args"] == [1234]  # pydantic serializes the tuple to a JSON array


def test_fstring_message_unchanged():
    # An already-formatted message with no args passes through verbatim.
    r = logging.LogRecord(
        "flow", logging.INFO, __file__, 1, "already formatted", None, None
    )
    assert _fmt(r)["msg"] == "already formatted"


def test_malformed_percent_args_fall_back_to_raw_template():
    # Several connectors pass a context dict positionally (e.g.
    # log.info(f"...", {"errors": ...})). When the (already-rendered) message
    # contains a stray '%', getMessage() would raise on `msg % args`; we must
    # fall back to the raw template rather than let logging drop the line.
    r = logging.LogRecord(
        "flow", logging.INFO, __file__, 1,
        "query failed: LIKE '%x%'", ({"errors": ["boom"]},), None,
    )
    out = _fmt(r)
    assert out["msg"] == "query failed: LIKE '%x%'"
