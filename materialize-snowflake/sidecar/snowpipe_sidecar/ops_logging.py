"""Structured logging in the Flow ops-log shape, one JSON object per stderr line.

Mirrors estuary-cdk's logger format ({"level", "msg", "fields": {...}}) so the
Go connector can relay these lines verbatim into its own stderr stream, which
the runtime collects as ops logs.
"""

import json
import logging
import os
import sys
import traceback

_LEVELS = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "fatal",
}

_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime"}


class OpsLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        fields = {
            "source": record.name,
            "file": f"{record.pathname}:{record.lineno}",
        }
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                fields[k] = v
        if record.exc_info:
            fields["traceback"] = traceback.format_exception(*record.exc_info)

        return json.dumps(
            {
                "level": _LEVELS.get(record.levelno, "info"),
                "msg": record.getMessage(),
                "fields": fields,
            },
            default=str,
        )


def setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(OpsLogFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    return logging.getLogger("snowpipe_sidecar")
