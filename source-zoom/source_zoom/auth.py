import requests

from typing import Tuple

from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator

class BaseAuth(Oauth2Authenticator):
    def refresh_access_token(self) -> Tuple[str, int]:
        data = {
            "client_id": (None, self.client_id),
            "client_secret": (None, self.client_secret),
            "grant_type": (None, "refresh_token"),
            "refresh_token": (None, self.refresh_token),
        }

        response = requests.post(self.token_refresh_endpoint, files = data)
        response.raise_for_status()
        response_body = response.json()
        return response_body["access_token"], response_body["expires_in"]
