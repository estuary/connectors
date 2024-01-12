from typing import Any, Iterable, Mapping, MutableMapping, Optional, Type
from requests import Response
import json

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream

class BaseStream(HttpStream):
    url_base = ""
    primary_key = None
    use_cache = False
    name = "base_stream"
    raise_on_http_errors = True
    parent_streams_configs = None

    @property
    def StreamType(self) -> Type:
        return self.__class__
    
    def __init__(self, connector: str, config_dict: dict, paginator: dict, url_base: str, **kwargs):
        super().__init__(**kwargs)
        
        self.connector = connector
        self.page_size = paginator["page_size"]
        self.paginator_opt_field_name = paginator["opt_field_name"]
        self.paginator_request_field_name = paginator["request_field_name"]
        self.has_pagination = config_dict["has_pagination"] if "has_pagination" in config_dict else False
        
        self.url_path = config_dict["path"]
        self.url_base = url_base
        self.primary_key = config_dict["primary_key"] if "primary_key" in config_dict else None
        self.use_cache = "use_cache" in config_dict or "schema_name" not in config_dict
        self.name = config_dict['name']
        self.schema_name = config_dict["schema_name"] if "schema_name" in config_dict else None
        self.record_extractor = config_dict["record_extractor"] if "record_extractor" in config_dict else None
        self.extra_opt = config_dict["extra_opt"] if "extra_opt" in config_dict else None
        
        self.raise_on_http_errors = "ignore_error" not in config_dict
            
        if "parent_streams" in config_dict:
            self.parent_streams_configs = config_dict["parent_streams"]
            
        if "use_sync_token" in config_dict:
            self.sync_token = None
        
    def get_json_schema(self) -> Mapping[str, Any]:
        with open(f"{self.connector}/{self.connector.replace('-', '_')}/schemas/{self.schema_name}", "r") as file:
            try:
                json_schema = json.load(file)
            except json.JSONDecodeError as error:
                raise ValueError(f"Could not read json spec file: {error}. Please ensure that it is a valid JSON.")

        return json_schema
        
    def path(self, stream_slice = None, **kwargs: Mapping[str, Any]) -> str:
        if self.parent_streams_configs is not None:
            path = self.url_path

            for parent_stream_configs in self.parent_streams_configs:
                partition_field = parent_stream_configs["partition_field"]
                if partition_field in stream_slice:
                    path = path.format(**{ partition_field: stream_slice[partition_field] })
            
            return path
            
        return self.url_path
        
    def next_page_token(self, response: Response) -> Optional[Mapping[str, Any]]:
        if not self.has_pagination:
            return None

        decoded_response = response.json()

        if self.check_use_sync():
            last_sync = decoded_response.get("sync")

            if last_sync:
                return { "sync": last_sync }

        next_page = decoded_response.get(self.paginator_request_field_name)

        if next_page:
            return { "offset": next_page["offset"] }
    
    def stream_slices(self, iteration = 0, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        if self.parent_streams_configs is not None:
            yield from self.read_slices_from_records(self.parent_streams_configs[iteration])
            if iteration < len(self.parent_streams_configs):
                self.stream_slices(iteration = iteration + 1, **kwargs)
        else:
            yield [None]
    
    def parse_response(self, response: Response, **kwargs: Mapping[str, Any]) -> Iterable[Mapping]:
        response_json = response.json()
        
        if self.check_use_sync() and response.status_code == 412:
            if "sync" in response_json:
                self.sync_token = response_json["sync"]
            else:
                self.sync_token = None
        else:
            if "code" in response_json:
                return
            
            if "sync" in response_json:
                self.sync_token = response_json["sync"]

            if self.record_extractor is not None:
                section_data = response_json.get(self.record_extractor, [])
            else:
                section_data = [response.json()]

            if isinstance(section_data, dict):
                yield section_data
            elif isinstance(section_data, list):
                yield from section_data
    
    def request_params(self, next_page_token: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargs: Mapping[str, Any]) -> MutableMapping[str, Any]:
        params = { self.paginator_opt_field_name: self.page_size }
        params.update(self.get_opt_fields())
                
        if self.extra_opt is not None:
            for key, value in self.extra_opt.items():
                if value in stream_slice:
                    params.update({ key: stream_slice[value] })
                else:
                    params.update({ key: value })

        if next_page_token:
            params.update(next_page_token)

        return params
    
    def _handle_object_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        return f"{prop}.id"

    def _handle_array_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        if "type" in value and "object" in value["type"]:
            return self._handle_object_type(prop, value)

        return prop
    
    def get_opt_fields(self) -> MutableMapping[str, str]:
        if self.schema_name is None:
            return { "opt_fields": "" }

        opt_fields = list()
        schema = self.get_json_schema()

        for prop, value in schema["properties"].items():
            if "object" in value["type"]:
                opt_fields.append(self._handle_object_type(prop, value))
            elif "array" in value["type"]:
                opt_fields.append(self._handle_array_type(prop, value.get("items", [])))
            else:
                opt_fields.append(prop)

        return { "opt_fields": ",".join(opt_fields) } if opt_fields else dict()
    
    def read_slices_from_records(self, stream) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_instance = stream["instance"]
        stream_slices = stream_instance.stream_slices(sync_mode = SyncMode.full_refresh)

        for stream_slice in stream_slices:
            for record in stream_instance.read_records(sync_mode = SyncMode.full_refresh, stream_slice = stream_slice):
                yield { stream["partition_field"]: record[stream["parent_key"]] }
                
    def read_records(self, *args, **kwargs):
        if self.check_use_sync() and self.sync_token is not None:
            kwargs["next_page_token"] = { "sync": self.sync_token }

        yield from super().read_records(*args, **kwargs)
        
        if self.check_use_sync():
            self.sync_token = self.get_latest_sync_token()
                
    def get_latest_sync_token(self) -> str:
        latest_sync_token = self.state.get("last_sync_token")

        if latest_sync_token is None:
            return None

        return latest_sync_token["sync"]
    
    def check_use_sync(self):
        return "sync_token" in self.__dict__