import re

from typing import Any, Mapping, List
from pydantic.error_wrappers import ValidationError

from airbyte_cdk.models import AdvancedAuth, ConnectorSpecification, OAuthConfigSpecification
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from airbyte_cdk.sources.file_based.exceptions import ConfigValidationError, FileBasedSourceError
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DefaultFileBasedCursor

from .stream import BaseStreamReader
from .spec import BaseSpec

def slugify(s):
    s = s.lower().strip()
    s = re.sub(r'[^\w\s-]', '-', s)
    s = re.sub(r'[\s_-]+', '-', s)
    s = re.sub(r'^-+|-+$', '', s)
    return s

def get_globs(formats: List[str]) -> List[str]:
    globs = []
    for format in formats:
        globs.append(f"*.{format}")
    return globs

class BaseSource(FileBasedSource):
    def __init__(self, catalog_path: str | None = None):
        super().__init__(
            stream_reader = BaseStreamReader(),
            spec_class = BaseSpec,
            catalog_path = catalog_path,
            cursor_cls = DefaultFileBasedCursor,
        )
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        try:
            config["streams"] = []
            parsed_config = self.spec_class(**config)
            self.stream_reader.config = parsed_config
            streams: List[Stream] = []
            
            file_formats = ["csv"]

            files = self.stream_reader.get_matching_files(globs = get_globs(file_formats), prefix = None)

            for file in files:
                stream_config = FileBasedStreamConfig(
                    name = slugify(file.name),
                    primary_key = "ID",
                    file_type = file.type,
                    globs = [file.name],
                    schemaless = False
                )

                self._validate_input_schema(stream_config)
                stream_schema = self.stream_schemas.get(stream_config.name)
                if stream_schema is None:
                    stream_schema = stream_config.get_input_schema()

                raise ValueError(stream_schema)
                streams.append(
                    DefaultFileBasedStream(
                        config = stream_config,
                        catalog_schema = self.stream_schemas.get(stream_config.name),
                        stream_reader = self.stream_reader,
                        availability_strategy = self.availability_strategy,
                        discovery_policy = self.discovery_policy,
                        parsers = self.parsers,
                        validation_policy = self._validate_and_get_validation_policy(stream_config),
                        cursor = self.cursor_cls(stream_config),
                    )
                )

            return streams

        except ValidationError as exc:
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR) from exc
        
    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl = self.spec_class.documentation_url(),
            connectionSpecification = self.spec_class.schema(),
            advanced_auth = AdvancedAuth(
                auth_flow_type = "oauth2.0",
                predicate_key = ["credentials", "auth_type"],
                predicate_value = "Client",
                oauth_config_specification = OAuthConfigSpecification(
                    complete_oauth_output_specification = {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"refresh_token": {"type": "string", "path_in_connector_config": ["credentials", "refresh_token"]}},
                    },
                    complete_oauth_server_input_specification = {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"client_id": {"type": "string"}, "client_secret": {"type": "string"}},
                    },
                    complete_oauth_server_output_specification = {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "path_in_connector_config": ["credentials", "client_id"]},
                            "client_secret": {"type": "string", "path_in_connector_config": ["credentials", "client_secret"]},
                        },
                    },
                ),
            ),
        )