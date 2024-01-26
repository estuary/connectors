import json

from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Type, Union
from requests import Response

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator

from .auth import BaseAuth

class BaseStream(HttpStream, ABC):
    url_base = "https://api.zoom.us/v2/"
    primary_key = "id"
    page_size = 30
    raise_on_http_errors = True
    schema_name = None

    @property
    def StreamType(self) -> Type:
        return self.__class__
    
    def __init__(self, authenticator: Union[BaseAuth, HttpAuthenticator] = None):
        super().__init__(authenticator = authenticator)
        
    def get_json_schema(self) -> Mapping[str, Any]:
        with open(f"source-zoom/source_zoom/schemas/{self.schema_name}", "r") as file:
            try:
                json_schema = json.load(file)
            except json.JSONDecodeError as error:
                raise ValueError(f"Could not read json spec file: {error}. Please ensure that it is a valid JSON.")

        return json_schema
    
    def next_page_token(self, response: Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        next_page = decoded_response.get("next_page")
        if next_page:
            return {"offset": next_page["offset"]}
        
    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = { "page_size": self.page_size }
        params.update(self.get_opt_fields())
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
            return {}

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
    
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()

        if "code" in response_json:
            return

        yield from [response_json]
    
    def read_slices_from_records(self, stream_class: StreamType, slice_field: str, parent_field: str = "id") -> Iterable[Optional[Mapping[str, Any]]]:
        stream = stream_class(authenticator = self.authenticator)
        stream_slices = stream.stream_slices(sync_mode = SyncMode.full_refresh)

        for stream_slice in stream_slices:
            for record in stream.read_records(sync_mode = SyncMode.full_refresh, stream_slice = stream_slice):
                yield {slice_field: record[parent_field]}
                
class Users(BaseStream):
    use_cache = True
    schema_name = "users.json"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "users"
    
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("users", [])

class MeetingsCompact(BaseStream):
    use_cache = True

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"users/{parent_id}/meetings"

    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = Users, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("meetings", [])
        
class Meetings(BaseStream):
    schema_name = "meetings.json"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"meetings/{parent_id}"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id")
        
class MeetingsRegistrants(BaseStream):
    schema_name = "meeting_registrants.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"meetings/{parent_id}/registrants"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("registrants", [])

class MeetingsPolls(BaseStream):
    schema_name = "meeting_polls.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"meetings/{parent_id}/polls"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("polls", [])

class MeetingsPollResults(BaseStream):
    schema_name = "meeting_poll_results.json"
    raise_on_http_errors = False
    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"past_meetings/{parent_id}/polls"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id", parent_field = "uuid")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("questions", [])
        
class MeetingsRegistrationQuestions(BaseStream):
    schema_name = "meeting_registration_questions.json"
    raise_on_http_errors = False
    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"past_meetings/{parent_id}/registrants/questions"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id")
        
class WebinarsCompact(BaseStream):
    use_cache = True
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"users/{parent_id}/webinars"

    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = Users, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("webinars", [])
        
class Webinars(BaseStream):
    schema_name = "webinars.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
class WebinarPanelists(BaseStream):
    schema_name = "webinar_panelists.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/panelists"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("panelists", [])

class WebinarRegistrants(BaseStream):
    schema_name = "webinar_registrants.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/registrants"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("registrants", [])

class WebinarAbsentees(BaseStream):
    schema_name = "webinar_absentees.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/absentees"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("registrants", [])

class WebinarPolls(BaseStream):
    schema_name = "webinar_polls.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/polls"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")

class WebinarPollResults(BaseStream):
    schema_name = "webinar_poll_results.json"
    raise_on_http_errors = False
    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"past_webinars/{parent_id}/polls"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("polls", [])

class WebinarRegistrationQuestions(BaseStream):
    schema_name = "webinar_registration_questions.json"
    raise_on_http_errors = False
    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/registrants/questions"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")

class WebinarTrackingSources(BaseStream):
    schema_name = "webinar_tracking_sources.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"webinars/{parent_id}/tracking_sources"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("tracking_sources", [])

class WebinarQnaResults(BaseStream):
    schema_name = "webinar_qna_results.json"
    raise_on_http_errors = False
    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"past_webinars/{parent_id}/qa"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id", parent_field = "uuid")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("questions", [])

class ReportMeetings(BaseStream):
    schema_name = "report_meetings.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"report/meetings/{parent_id}"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id", parent_field = "uuid")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("tracking_sources", [])

class ReportMeetingParticipants(BaseStream):
    schema_name = "report_meeting_participants.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"report/meetings/{parent_id}/participants"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = MeetingsCompact, slice_field = "parent_id", parent_field = "uuid")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("participants", [])

class ReportWebinars(BaseStream):
    schema_name = "report_webinars.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"report/webinars/{parent_id}"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id", parent_field = "uuid")

class ReportWebinarParticipants(BaseStream):
    schema_name = "report_webinar_participants.json"
    raise_on_http_errors = False

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        parent_id = stream_slice["parent_id"]
        return f"report/webinars/{parent_id}/participants"
    
    def request_params(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        params["parent_id"] = stream_slice["parent_id"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        yield from self.read_slices_from_records(stream_class = WebinarsCompact, slice_field = "parent_id", parent_field = "uuid")
        
    def parse_response(self, response: Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("participants", [])