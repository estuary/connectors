import abc
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt

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


class InferenceExists(Enum):
    INVALID = "INVALID"
    MUST = "MUST"
    MAY = "MAY"
    IMPLICIT = "IMPLICIT"
    CANNOT = "CANNOT"


class InferenceString(BaseModel):
    content_type: str | None = None
    format: str | None = None
    content_encoding: str | None = None
    max_length: int | None = None


class Inference(BaseModel):
    types: list[str] | None = None
    string: InferenceString | None = None
    title: str | None = None
    description: str | None = None
    defaultJson: Any | None = None
    exists: InferenceExists


class Projection(BaseModel):
    ptr: str | None = None
    field: str
    explicit: bool | None = False
    isPrimarykey: bool | None = False
    inference: Inference

    def is_root_document_projection(self) -> bool:
        return self.ptr == ""


class CollectionSpec(BaseModel):
    name: str
    key: list[str]
    writeSchema: dict[str, Any]
    readSchema: dict[str, Any] | None = None
    projections: list[Projection]


class CaptureBinding(GenericModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    resourcePath: list[str]
    stateKey: str
    backfill: NonNegativeInt = 0


class NetworkPort(GenericModel):
    number: PositiveInt
    protocol: str
    public: bool


class CaptureSpec(GenericModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    intervalSeconds: NonNegativeInt
    bindings: list[CaptureBinding[ResourceConfig]] = []
    networkPorts: list[NetworkPort] | None = None


class FieldSelection(BaseModel):
    keys: list[str] | None = None
    values: list[str] | None = None
    document: str | None = None
    fieldConfigJsonMap: dict[str, Any] | None = None

    def all_fields(self) -> tuple[str, ...]:
        out: list[str] = []

        if self.keys is not None:
            out.extend(self.keys)

        if self.values is not None:
            out.extend(self.values)

        if self.document is not None:
            out.append(self.document)

        return tuple(out)


class MaterializationBinding(BaseModel, Generic[ResourceConfig]):
    collection: CollectionSpec
    resourceConfig: ResourceConfig
    resourcePath: list[str]
    fieldSelection: FieldSelection
    deltaUpdates: bool
    stateKey: str
    backfill: NonNegativeInt = 0


class MaterializationSpec(BaseModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    bindings: list[MaterializationBinding[ResourceConfig]] = []
    networkPorts: list[NetworkPort] | None = None


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


class OAuth2Spec(BaseModel):
    provider: str
    accessTokenBody: str
    authUrlTemplate: str
    accessTokenHeaders: dict[str, str]
    accessTokenResponseMap: dict[str, str]
    accessTokenUrlTemplate: str


class ConnectorSpec(BaseModel):
    configSchema: dict
    resourceConfigSchema: dict
    documentationUrl: str
    resourcePathPointers: list[str] | None = None
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


class BaseOAuth2Credentials(abc.ABC, BaseModel):
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
