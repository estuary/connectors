import dpath.util

from typing import Dict, Any, Optional, List
from copy import deepcopy
from pydantic import BaseModel, Field

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig

class CredentialsSpec(BaseModel):
    auth_method: str = Field("oauth2.0", const = True, order = 1)
    auth_type: str = Field("Client", const = True, order = 1)
    developer_token: str = Field(
        title = "Developer Token",
        description = "Developer token associated with user. See more info <a href=\"https://docs.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13#get-developer-token\"> in the docs</a>.",
        airbyte_secret = True,
        type = "string",
        order = 2,
    )
    client_id: str = Field(
        title = "Client ID",
        description = "The Client ID of your Microsoft Advertising developer application.",
        airbyte_secret = True,
        type = "string",
        order = 3,
    )
    client_secret: str = Field(
        title = "Client Secret",
        description = "The Client Secret of your Microsoft Advertising developer application.",
        airbyte_secret = True,
        type = "string",
        order = 4,
    )
    refresh_token: str = Field(
        title = "Refresh Token",
        description = "Refresh Token to renew the expired Access Token.",
        airbyte_secret = True,
        type = "string",
        order = 5,
    )

class BaseSpec(AbstractFileBasedSpec):
    tenant_id: Optional[str] = Field(
        title = "Tenant ID",
        description = "Tenant ID of the Microsoft OneDrive user",
        airbyte_secret = True,
        type = "string",
        order = 1,
    )
    credentials: CredentialsSpec = Field(
        title = "Credentials",
        description = "Credentials for connecting to the One Drive API",
        airbyte_secret = True,
        type = "object",
        order = 2,
    )
    drive_name: Optional[str] = Field(
        title = "Drive Name",
        description = "Name of the Microsoft Drive where the file(s) exist.",
        default = "Sharepoint",
        order = 3
    )
    folder_share_link: Optional[str] = Field(
        title = "Folder Share Link",
        description = "Share Link of the Microsoft Drive where the file(s) exist.",
        order = 5
    )
    streams: Optional[List[FileBasedStreamConfig]] = Field(
        title = "The list of streams to sync",
        description = 'Each instance of this configuration defines a <a href="https://docs.airbyte.com/cloud/core-concepts#stream">stream</a>. Use this to define which files belong in the stream, their format, and how they should be parsed and validated. When sending data to warehouse destination such as Snowflake or BigQuery, each stream is a separate table.',
        order = 10,
    )
    
    @classmethod
    def documentation_url(cls) -> str:
        return "https://docs.airbyte.com/integrations/sources/sharepoint"
    
    @classmethod
    def schema(cls, *args, **kwargs) -> Dict[str, Any]:
        schema = super().schema(*args, **kwargs)
        transformed_schema = deepcopy(schema)
        
        dpath.util.delete(transformed_schema, "properties/streams/items/properties/format/oneOf/1/properties/inference_type/airbyte_hidden")
        dpath.util.delete(transformed_schema, "properties/streams/items/properties/legacy_prefix/airbyte_hidden")
        dpath.util.delete(transformed_schema, "properties/start_date/pattern_descriptor")
        
        return transformed_schema