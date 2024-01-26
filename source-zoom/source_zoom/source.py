from typing import Any, List, Mapping, Tuple, Union

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import AdvancedAuth, ConnectorSpecification, OAuthConfigSpecification
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .auth import BaseAuth
from .spec import BaseSpec

from .streams import Users
from .streams import Meetings
from .streams import MeetingsRegistrants
from .streams import MeetingsPolls
from .streams import MeetingsPollResults
from .streams import MeetingsRegistrationQuestions
from .streams import Webinars
from .streams import WebinarPanelists
from .streams import WebinarRegistrants
from .streams import WebinarAbsentees
from .streams import WebinarPolls
from .streams import WebinarPollResults
from .streams import WebinarRegistrationQuestions
from .streams import WebinarTrackingSources
from .streams import WebinarQnaResults
from .streams import ReportMeetings
from .streams import ReportMeetingParticipants
from .streams import ReportWebinars
from .streams import ReportWebinarParticipants

class BaseSource(AbstractSource):
    spec_class = BaseSpec

    def get_args(self, config: Mapping[str, Any]):
        args = {
            "authenticator": self._get_authenticator(config),
            "paginator": self.paginator,
            "url_base": self.base_endpoint
        }
        
        return args
    
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            workspaces_stream = Users(authenticator=self._get_authenticator(config))
            next(workspaces_stream.read_records(sync_mode=SyncMode.full_refresh))
            return True, None
        except Exception as e:
            return False, e
        
    def _get_authenticator(self, config: dict) -> Union[TokenAuthenticator, BaseAuth]:
        if "access_token" in config:
            return TokenAuthenticator(token = config["access_token"])

        creds = config.get("credentials")

        if "personal_access_token" in creds:
            return TokenAuthenticator(token = creds["personal_access_token"])
        else:
            return BaseAuth(
                client_id = creds["client_id"],
                client_secret = creds["client_secret"],
                refresh_token = creds["refresh_token"],
                token_refresh_endpoint = "https://zoom.us/oauth/token",
            )
            
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        args = { "authenticator": self._get_authenticator(config) }

        streams = [
            Users(**args),
            Meetings(**args),
            MeetingsRegistrants(**args),
            MeetingsPolls(**args),
            MeetingsPollResults(**args),
            MeetingsRegistrationQuestions(**args),
            Webinars(**args),
            WebinarPanelists(**args),
            WebinarRegistrants(**args),
            WebinarAbsentees(**args),
            WebinarPolls(**args),
            WebinarPollResults(**args),
            WebinarRegistrationQuestions(**args),
            WebinarTrackingSources(**args),
            WebinarQnaResults(**args),
            ReportMeetings(**args),
            ReportMeetingParticipants(**args),
            ReportWebinars(**args),
            ReportWebinarParticipants(**args),
        ]

        return streams
    
    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl = self.spec_class.documentation_url(),
            connectionSpecification = self.spec_class.schema(),
            advanced_auth = AdvancedAuth(
                auth_flow_type = "oauth2.0",
                predicate_key = ["credentials"],
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