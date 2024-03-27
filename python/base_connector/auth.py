from requests import post
import base64

from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator as BaseOauth2Authenticator


class Oauth2Authenticator(BaseOauth2Authenticator):
    grant_type = "refresh_token"

    def __init__(self, use_base64, content_type, use_body, refresh_token_fields, **kwargs):
        super().__init__(**kwargs)
        self.token_refresh_endpoint = kwargs["token_refresh_endpoint"]
        self.grant_type = kwargs["grant_type"]
        self.client_id = kwargs["client_id"]
        self.client_secret = kwargs["client_secret"]
        self.refresh_token = kwargs["refresh_token"]

        self.use_base64 = use_base64
        self.content_type = content_type
        self.use_body = use_body
        self.refresh_token_fields = refresh_token_fields

    def refresh_access_token(self):
        auth_request = {
            "headers": {},
            "data":  {}
        }
        
        if self.use_body is True:
            for token_field in self.refresh_token_fields:
                auth_request["data"][token_field] = self.__dict__[token_field]
        
        if self.content_type is not None:
            auth_request["headers"]["Content-Type"] = self.content_type
        
        if self.use_base64 is not None:
            auth_hash = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode('ascii')).decode('ascii')
            auth_request["headers"]["Authorization"] = f"Basic {auth_hash}"
            
            response = post(self.token_refresh_endpoint, **auth_request)
            response_body = response.json()
            
            if response.status_code >= 400:
                raise Exception(response_body)
            
            return response_body["access_token"], response_body["expires_in"]