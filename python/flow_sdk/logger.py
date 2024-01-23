import logging.config
import os
from contextlib import contextmanager

import orjson

logging_context_vars = {}
dynamic_logging_context_vars = {}


def set_context(**kwargs):
    global logging_context_vars

    logging_context_vars = kwargs


@contextmanager
def logging_context(**kwargs):
    global dynamic_logging_context_vars

    old_logging_context_vars = dynamic_logging_context_vars.copy()
    dynamic_logging_context_vars.update(kwargs)

    yield

    dynamic_logging_context_vars = old_logging_context_vars


class LogFormatter(logging.Formatter):
    # Keys which are present in all LogRecord instances.
    # We use this set to identify _novel_ keys which should be included as structured, logged fields.
    LOGGING_RECORD_KEYS = logging.LogRecord(
        "", 0, "", 0, None, None, None
    ).__dict__.keys()

    def format(self, record: logging.LogRecord) -> str:
        global dynamic_logging_context_vars

        # Attach any extra keywords which are not ordinarily in a LogRecord as fields.
        fields = {
            k: getattr(record, k)
            for k in record.__dict__.keys()
            if hasattr(record, k) and k not in self.LOGGING_RECORD_KEYS
        }
        if record.args:
            fields["args"] = record.args

        fields["source"] = record.name
        fields["file"] = f"{record.pathname}:{record.lineno}"

        # Attach any included stack traces.
        if record.exc_info:
            fields["traceback"] = self.formatException(record.exc_info).splitlines()
        elif record.stack_info:
            fields["stack"] = self.formatStack(record.stack_info).splitlines()

        # attach global context entries, but only if they are not already present in the record
        for k, v in logging_context_vars.items():
            if k not in fields:
                fields[k] = v

        # attach any dyanmic context, but only if they are not already present in the record
        for k, v in dynamic_logging_context_vars.items():
            if k not in fields:
                fields[k] = v

        return str(
            orjson.dumps(
                {
                    "level": record.levelname,
                    "msg": record.msg,
                    "fields": fields,
                },
                # Map non-string dict keys into str, and prefer 'Z' over '+00:00'.
                option=orjson.OPT_NON_STR_KEYS | orjson.OPT_UTC_Z,
                # Map unhandled JSON types through __str__() if available, or repr().
                default=str,
            ),
            encoding="utf-8",
            errors="ignore",
        )


def init_logger():
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "flow": {
                "()": LogFormatter,
                "format": "",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "formatter": "flow",
            },
        },
        "root": {
            "handlers": ["console"],
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)

    logger = logging.getLogger("flow")
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

    return logger
