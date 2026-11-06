#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import base64
from typing import Any, Mapping

from .core import HttpAuthenticator


class TokenAuthenticator(HttpAuthenticator):
    def __init__(self, token: str, auth_method: str = "Bearer", auth_header: str = "Authorization"):
        self.auth_method = auth_method
        self.auth_header = auth_header
        self._token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {self.auth_header: f"{self.auth_method} {self._token}"}


class BasicHttpAuthenticator(TokenAuthenticator):
    """
    Builds auth based off the basic authentication scheme as defined by RFC 7617, which transmits credentials as USER ID/password pairs, encoded using bas64
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme
    """

    def __init__(self, username: str, password: str, auth_method: str = "Basic", auth_header: str = "Authorization"):
        auth_string = f"{username}:{password}".encode("utf8")
        b64_encoded = base64.b64encode(auth_string).decode("utf8")
        super().__init__(b64_encoded, auth_method, auth_header)
