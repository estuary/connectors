import abc
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, NonNegativeInt, PositiveInt, Field
from typing import Any, Literal, TypeVar, Generic, Literal

from .pydantic_polyfill import GenericModel

# The type of this invoked connector.
ConnectorType = Literal[
    "IMAGE",  # We're running with the context of a container image.
    "LOCAL",  # We're running directly on the host as a local process.
]

# Generic type of a connector's endpoint configuration.
EndpointConfig = TypeVar("EndpointConfig")

# Generic type of a connector's resource configuration.
ResourceConfig = TypeVar("ResourceConfig", bound=BaseModel)

# Generic type of a connector's resource-level state.
ConnectorState = TypeVar("ConnectorState", bound=BaseModel)


class CollectionSpec(BaseModel):
    name: str
    key: list[str]
    writeSchema: dict[str, Any]
    readSchema: dict[str, Any] | None = None


class CaptureBinding(GenericModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    resourcePath: list[str]
    stateKey: str
    backfill: NonNegativeInt = 0


class CaptureSpec(GenericModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    intervalSeconds: NonNegativeInt
    bindings: list[CaptureBinding[ResourceConfig]] = []


class RangeSpec(BaseModel):
    keyBegin: NonNegativeInt = 0
    keyEnd: PositiveInt = 0xFFFFFFFF
    rClockBegin: NonNegativeInt = 0
    rClockEnd: PositiveInt = 0xFFFFFFFF


class UUIDParts(BaseModel):
    node: str
    clock: str


class CheckpointSource(BaseModel):
    readThrough: str
    producers: list[Any]


class Checkpoint(BaseModel):
    sources: dict[str, CheckpointSource]
    ackIntents: dict[str, str]


class OAuth2TokenFlowSpec(BaseModel):
    accessTokenResponseMap: dict[str, str]
    accessTokenUrlTemplate: str


class OAuth2Spec(BaseModel):
    provider: str
    accessTokenBody: str
    authUrlTemplate: str
    accessTokenHeaders: dict[str, str]
    accessTokenResponseMap: dict[str, str]
    accessTokenUrlTemplate: str


class OAuth2RotatingTokenSpec(OAuth2Spec):
    additionalTokenExchangeBody: dict[str, str | int] | None


class ConnectorSpec(BaseModel):
    configSchema: dict
    resourceConfigSchema: dict
    documentationUrl: str
    resourcePathPointers: list[str]
    oauth2: OAuth2Spec | None = None
    protocol: int = 0


class ConnectorStateUpdate(GenericModel, Generic[ConnectorState]):
    updated: ConnectorState
    mergePatch: bool


class AccessToken(BaseModel):
    credentials_title: Literal["Private App Credentials"] = Field(
        default="Private App Credentials",
        json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="Access Token",
        json_schema_extra={"secret": True},
    )


class BasicAuth(BaseModel):
    credentials_title: Literal["Username & Password"] = Field(
        default="Username & Password",
        json_schema_extra={"type": "string"}
    )
    username: str
    password: str = Field(
        title="Password",
        json_schema_extra={"secret": True},
    )


@dataclass
class ValidationError(Exception):
    """ValidationError is an exception type for one or more structured,
    user-facing validation errors.

    ValidationError is caught and pretty-printed without a traceback."""

    errors: list[str]


class ResourceOwnerPasswordOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["OAuth Credentials"] = Field(
        default="OAuth Credentials",
        json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )


class ClientCredentialsOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["OAuth Credentials"] = Field(
        default="OAuth Credentials",
        json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )


class AuthorizationCodeFlowOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["OAuth Credentials"] = Field(
        default="OAuth Credentials", json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )

    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...

    @staticmethod
    def for_provider(
        provider: str,
    ) -> type["AuthorizationCodeFlowOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(AuthorizationCodeFlowOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials


class LongLivedClientCredentialsOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["OAuth Credentials"] = Field(
        default="OAuth Credentials",
        json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )
    access_token: str = Field(
        title="Access Token",
        json_schema_extra={"secret": True}
    )
    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...

    @staticmethod
    def for_provider(provider: str) -> type["LongLivedClientCredentialsOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(LongLivedClientCredentialsOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials


class BaseOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["OAuth Credentials"] = Field(
        default="OAuth Credentials",
        json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )
    refresh_token: str = Field(
        title="Refresh Token",
        json_schema_extra={"secret": True},
    )

    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...

    @staticmethod
    def for_provider(provider: str) -> type["BaseOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(BaseOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials


class RotatingOAuth2Credentials(BaseOAuth2Credentials):
    access_token: str = Field(
        title="Access Token",
        json_schema_extra={"secret": True}
    )
    access_token_expires_at: datetime = Field(
        title="Access token expiration time.",
    )
    @staticmethod
    def for_provider(provider: str) -> type["RotatingOAuth2Credentials"]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        class _OAuth2Credentials(RotatingOAuth2Credentials):
            model_config = ConfigDict(
                json_schema_extra={"x-oauth2-provider": provider},
                title="OAuth",
            )

            def _you_must_build_oauth2_credentials_for_a_provider(self): ...

        return _OAuth2Credentials
