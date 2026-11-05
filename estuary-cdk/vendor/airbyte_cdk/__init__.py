#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any
import requests

# Importing `.entrypoint` is load-bearing beyond the re-export: at module scope it runs
# `init_logger("airbyte")`, which raises the "airbyte" logger to INFO. Without it the CDK's
# own retry/backoff logging sits below the default WARNING threshold and is silently dropped.
from .entrypoint import AirbyteEntrypoint
from .logger import AirbyteLogger

__all__ = ["AirbyteEntrypoint", "AirbyteLogger"]

# Airbyte's CDK does not set a timeout for HTTP requests, so we patch it to always have a timeout.
# The patch is on `requests.Session` rather than `HttpStream._send` because connectors also call
# `requests` directly. It lives here so that importing anything from `airbyte_cdk` applies it.

DEFAULT_TIMEOUT = 60 * 60 * 2 # 2 hours

lib_send = requests.Session.send

def send(*args: Any, **kwargs: Any):
    if kwargs.get("timeout", None) is None:
        kwargs["timeout"] = DEFAULT_TIMEOUT

    return lib_send(*args, **kwargs)

requests.Session.send = send
