"""Logging for the sidecar, in the ops-log shape estuary-cdk's formatter emits."""

import logging
import os

from estuary_cdk.logger import init_logger


def setup_logging() -> logging.Logger:
    init_logger()
    # init_logger leaves the root logger at its default level, which would drop
    # this package's info-level records before they reach the formatter.
    logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    return logging.getLogger("snowpipe_sidecar")
