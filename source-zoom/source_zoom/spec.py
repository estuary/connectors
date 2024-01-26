from copy import deepcopy
from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field

class CredentialsSpec(BaseModel):
    account_id: str = Field(
        title = "Developer Token",
        description = "The account ID for your Zoom account. You can find this in the Zoom Marketplace under the 'Manage' tab for your app.",
        type = "string",
        order = 0,
    )
    client_id: str = Field(
        title = "Client ID",
        description = "The Client ID of your Zoom developer application.",
        airbyte_secret = True,
        type = "string",
        order = 1,
    )
    client_secret: str = Field(
        title = "Client Secret",
        description = "The Client Secret of your Zoom developer application.",
        airbyte_secret = True,
        type = "string",
        order = 2,
    )
    refresh_token: str = Field(
        title = "Refresh Token",
        description = "Refresh Token to renew the expired Access Token.",
        airbyte_secret = True,
        type = "string",
        order = 3,
    )

class BaseSpec(BaseModel):
    credentials: CredentialsSpec = Field(
        title = "Credentials",
        description = "Credentials for connecting to the Zoom application",
        airbyte_secret = True,
        type = "object",
        order = 0,
    )
    
    @classmethod
    def documentation_url(cls) -> str:
        return "https://docs.airbyte.com/integrations/sources/zoom"

    @classmethod
    def schema(cls, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        schema = super().schema(*args, **kwargs)
        transformed_schema = deepcopy(schema)

        return transformed_schema