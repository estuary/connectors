from functools import lru_cache

from msal import ConfidentialClientApplication
from msal.exceptions import MsalServiceError
from office365.graph_client import GraphClient

class BaseClient:
    def __init__(self, config):
        self.config = config
        self._client = None

    @property
    @lru_cache(maxsize = None)
    def msal_app(self):
        return ConfidentialClientApplication(
            self.config.credentials.client_id,
            authority = f"https://login.microsoftonline.com/{self.config.tenant_id}",
            client_credential = self.config.credentials.client_secret,
        )

    @property
    def client(self):
        if not self.config:
            raise ValueError("Configuration is missing; cannot create the Office365 graph client.")
        if not self._client:
            self._client = GraphClient(self._get_access_token)
        return self._client

    def _get_access_token(self):
        scope = ["https://graph.microsoft.com/.default"]
        refresh_token = self.config.credentials.refresh_token if hasattr(self.config.credentials, "refresh_token") else None

        if refresh_token:
            result = self.msal_app.acquire_token_by_refresh_token(refresh_token, scopes=scope)
        else:
            result = self.msal_app.acquire_token_for_client(scopes = scope)

        if "access_token" not in result:
            error_description = result.get("error_description", "No error description provided.")
            raise MsalServiceError(error = result.get("error"), error_description = error_description)

        return result