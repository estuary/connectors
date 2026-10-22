from typing import Any
import requests

# Airbyte's CDK does not set a timeout for HTTP requests, so we patch it to always have a timeout.

DEFAULT_TIMEOUT = 60 * 60 * 2 # 2 hours

lib_send = requests.Session.send

def send(*args: Any, **kwargs: Any):
    if kwargs.get("timeout", None) is None:
        kwargs["timeout"] = DEFAULT_TIMEOUT

    return lib_send(*args, **kwargs)

requests.Session.send = send
