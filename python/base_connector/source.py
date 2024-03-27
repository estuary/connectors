import yaml

from typing import Any, List, Mapping, Tuple, Union

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .stream import BaseStream
from .auth import Oauth2Authenticator

class BaseSource(AbstractSource):
    check_stream = None
    schema_streams = []
    content_type = None
    use_body = None
    refresh_token_fields = []

    def __init__(self, connector: str):
        super().__init__()
        self.__class__.__module__ = connector.replace("-", "_")
        self.connector = connector

        with open(f"{connector}/{self.__class__.__module__}/estuary-manifest.yaml", "r") as file:
            manifest = yaml.safe_load(file)
        
        self.base_endpoint = manifest["definitions"]["url_base"]
        self.token_refresh_endpoint = manifest["definitions"]["url_refresh_token"]
        self.grant_type = manifest["definitions"]["grant_type"]
        
        self.paginator = manifest["paginator"]
        self.streams_dict = manifest["streams"]
        self.check_stream = manifest["check_stream"]
        self.use_base64 = "use_base64" in manifest["definitions"] and manifest["definitions"]["use_base64"] is True
        
        if "content_type" in manifest["definitions"]:
            self.content_type = manifest["definitions"]["content_type"]

        if "use_body" in manifest["definitions"]:
            self.use_body = manifest["definitions"]["use_body"]

        if "refresh_token_fields" in manifest["definitions"]:
            self.refresh_token_fields = manifest["definitions"]["refresh_token_fields"]
        
    def get_args(self, config: Mapping[str, Any]):
        args = {
            "connector": self.connector,
            "authenticator": self._get_authenticator(config),
            "paginator": self.paginator,
            "url_base": self.base_endpoint
        }
        
        return args
    
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            check_stream = BaseStream(
                config_dict = self.streams_dict[self.check_stream],
                **self.get_args(config)
            )

            next(check_stream.read_records(sync_mode = SyncMode.full_refresh))
            return True, None
        except Exception as e:
            return False, e
    
    def _get_authenticator(self, config: dict) -> Union[TokenAuthenticator, Oauth2Authenticator]:
        if "access_token" in config:
            return TokenAuthenticator(token = config["access_token"])

        creds = config.get("credentials")

        if "personal_access_token" in creds:
            return TokenAuthenticator(token = creds["personal_access_token"])
        else:
            return Oauth2Authenticator(
                token_refresh_endpoint = self.token_refresh_endpoint,
                grant_type = self.grant_type,
                client_id = creds["client_id"],
                client_secret = creds["client_secret"],
                refresh_token = creds["refresh_token"],
                use_base64 = self.use_base64,
                content_type = self.content_type,
                use_body = self.use_body,
                refresh_token_fields = self.refresh_token_fields,
            )
            
    def generate_stream(self, config_dict, args):
        stream = next((x for x in self.schema_streams if x.name == config_dict["name"]), None)
        if stream is not None:
            return stream
        
        stream = BaseStream(config_dict = config_dict, **args)
        if "schema_name" in config_dict:
            self.schema_streams.append(stream)
            
        return stream
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:    
        args = self.get_args(config)
        
        for _, config_dict in self.streams_dict.items():
            if "parent_streams" in config_dict:
                for index, parent_config_dict in enumerate(config_dict["parent_streams"]):
                    parent_stream = self.generate_stream(self.streams_dict[parent_config_dict["name"]], args)
                    config_dict["parent_streams"][index]["instance"] = parent_stream
            
            self.generate_stream(config_dict, args)

        return self.schema_streams