import abc
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum, auto
from typing import Any, ClassVar, Generic, Literal, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

from .pydantic_polyfill import GenericModel

# The type of this invoked connector.
ConnectorType = Literal[
    "IMAGE",  # We're running with the context of a container image.
    "LOCAL",  # We're running directly on the host as a local process.
]


class OAuth2ClientCredentialsPlacement(StrEnum):
    """
    Placement of client id and client secret during the OAuth2 token exchange step.
    """

    HEADERS = auto()
    FORM = auto()


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
    additionalTokenExchangeBody: dict[str, str | int] = {}


class OAuth2Spec(BaseModel):
    provider: str
    accessTokenBody: str
    authUrlTemplate: str
    accessTokenHeaders: dict[str, str]
    accessTokenResponseMap: dict[str, str]
    accessTokenUrlTemplate: str

    # additionalTokenExchangeBody pertains to internal connector token exchanges
    # and should be excluded from spec responses
    additionalTokenExchangeBody: dict[str, str | int] = Field(default={}, exclude=True)


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
        default="Private App Credentials", json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="Access Token",
        json_schema_extra={"secret": True},
    )


class BasicAuth(BaseModel):
    credentials_title: Literal["Username & Password"] = Field(
        default="Username & Password", json_schema_extra={"type": "string"}
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


class _BaseOAuth2CredentialsData(BaseModel):
    """
    Abstract base class containing common OAuth2 credential fields.
    """

    client_credentials_placement: ClassVar[OAuth2ClientCredentialsPlacement] = (
        OAuth2ClientCredentialsPlacement.FORM
    )

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

    # This configuration provides a "title" annotation for the UI to display
    # instead of the class name.
    model_config = ConfigDict(
        title="OAuth",
    )

    @classmethod
    def with_client_credentials_placement(
        cls,
        placement: OAuth2ClientCredentialsPlacement,
    ) -> type[Self]:
        """
        Returns a subclass with a custom client credentials placement.
        """

        return type(
            str(cls.__name__), (cls,), {"client_credentials_placement": placement}
        )  # pyright: ignore[reportReturnType]

    @classmethod
    def for_provider(cls, provider: str) -> type[Self]:
        """
        Builds an OAuth2Credentials model for the given OAuth2 `provider`.
        This routine is only available in Pydantic V2 environments.
        """
        from pydantic import ConfigDict

        return type(  # pyright: ignore[reportReturnType]
            cls.__name__,
            (cls,),
            {
                "model_config": ConfigDict(
                    json_schema_extra={"x-oauth2-provider": provider},
                    title="OAuth",
                ),
                "_you_must_build_oauth2_credentials_for_a_provider": lambda _: None,  # pyright: ignore[reportUnknownLambdaType]
            },
        )


class ResourceOwnerPasswordOAuth2Credentials(_BaseOAuth2CredentialsData):
    grant_type: ClassVar[str] = "password"


class ClientCredentialsOAuth2Credentials(_BaseOAuth2CredentialsData):
    grant_type: ClassVar[str] = "client_credentials"
    client_credentials_placement: ClassVar[OAuth2ClientCredentialsPlacement] = (
        OAuth2ClientCredentialsPlacement.HEADERS
    )


class AuthorizationCodeFlowOAuth2Credentials(
    _BaseOAuth2CredentialsData, metaclass=abc.ABCMeta
):
    grant_type: ClassVar[str] = "authorization_code"

    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...


class LongLivedClientCredentialsOAuth2Credentials(
    _BaseOAuth2CredentialsData, metaclass=abc.ABCMeta
):
    access_token: str = Field(title="Access Token", json_schema_extra={"secret": True})

    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...


class BaseOAuth2Credentials(_BaseOAuth2CredentialsData, metaclass=abc.ABCMeta):
    grant_type: ClassVar[str] = "refresh_token"

    refresh_token: str = Field(
        title="Refresh Token",
        json_schema_extra={"secret": True},
    )

    @abc.abstractmethod
    def _you_must_build_oauth2_credentials_for_a_provider(self): ...


class RotatingOAuth2Credentials(BaseOAuth2Credentials, metaclass=abc.ABCMeta):
    access_token: str = Field(title="Access Token", json_schema_extra={"secret": True})
    access_token_expires_at: datetime = Field(
        title="Access token expiration time.",
    )


class GoogleServiceAccountSpec(BaseModel):
    scopes: list[str]


class GoogleServiceAccount(BaseModel):
    credentials_title: Literal["Google Service Account"] = Field(
        default="Google Service Account",
        json_schema_extra={"type": "string", "order": 0},
    )
    service_account: str = Field(
        title="Google Service Account",
        description="Service account JSON key",
        json_schema_extra={"secret": True, "multiline": True, "order": 1},
    )
