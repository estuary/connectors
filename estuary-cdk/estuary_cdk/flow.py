import abc
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum, auto
from typing import Any, ClassVar, Generic, Literal, Self, Sequence, TypeVar, cast

import pydantic
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

from .pydantic_polyfill import GenericModel

# Is pydantic's own API that of v2? `pydantic_polyfill.VERSION` can't say:
# releases of 1.10 which include a `pydantic.v1` alias also report "v2".
_PYDANTIC_V2 = pydantic.VERSION.startswith("2.")

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

# Generic type of a connector's state. A capture's is a model, and a
# derivation's may be a plain `dict`.
ConnectorState = TypeVar("ConnectorState")


class Inference(BaseModel):
    """Inference of a projected location, from the collection's schema."""

    class String(BaseModel):
        contentType: str = ""
        format: str = ""
        contentEncoding: str = ""
        maxLength: NonNegativeInt = 0
        strMinimum: str = ""
        strMaximum: str = ""

    class Numeric(BaseModel):
        hasMinimum: bool = False
        minimum: float = 0
        hasMaximum: bool = False
        maximum: float = 0

    class Array(BaseModel):
        minItems: NonNegativeInt = 0
        hasMaxItems: bool = False
        maxItems: NonNegativeInt = 0
        itemTypes: list[str] = []

    types: list[str] = []
    string: String | None = None
    title: str = ""
    description: str = ""
    default: Any = None
    secret: bool = False
    exists: Literal["INVALID", "MUST", "MAY", "IMPLICIT", "CANNOT"] = "INVALID"
    numeric: Numeric | None = None
    array: Array | None = None
    enum: list[Any] = []
    reduce: Literal[
        "REDUCE_UNSET",
        "REDUCE_MULTIPLE",
        "REDUCE_APPEND",
        "REDUCE_FIRST_WRITE_WINS",
        "REDUCE_LAST_WRITE_WINS",
        "REDUCE_MAXIMIZE",
        "REDUCE_MERGE",
        "REDUCE_MINIMIZE",
        "REDUCE_SET",
        "REDUCE_SUM",
        "REDUCE_JSON_SCHEMA_MERGE",
    ] = "REDUCE_UNSET"
    redact: Literal[
        "REDACT_UNSET", "REDACT_MULTIPLE", "REDACT_BLOCK", "REDACT_SHA256"
    ] = "REDACT_UNSET"
    contentMediaType: str = ""


class Projection(BaseModel):
    """A document location, projected as a named field of a collection."""

    ptr: str = ""
    field: str
    explicit: bool = False
    isPartitionKey: bool = False
    isPrimaryKey: bool = False
    inference: Inference = Inference()


class CollectionSpec(BaseModel):
    name: str
    key: list[str]
    writeSchema: dict[str, Any]
    readSchema: dict[str, Any] | None = None
    partitionFields: list[str] = []
    projections: list[Projection] = []


# A binding or transform of a message names its collection in one of two forms.
# In the inline form, it carries its own `collection`. In the indirect form,
# its message has a `linkedCollections` table, and `collectionIndex` locates
# the collection within it. The form is a property of the message as a whole:
# it's indirect if and only if `linkedCollections` is non-empty.
#
# Each message which may be indirect links its bindings upon being parsed.
# A binding's `collection` then references its entry of `linkedCollections`,
# so that the bindings of one collection share one parsed CollectionSpec, and
# `collection` is set in either form. A linked message re-serializes inline.
#
# `collection` defaults to a placeholder, as it's absent from the indirect
# form, and linking always replaces it.
UNLINKED = cast(CollectionSpec, None)


def link_bindings(bindings: Sequence[Any], linked_collections: list[CollectionSpec]) -> None:
    """Link each of `bindings` (or transforms) to its collection within
    `linked_collections`, or else require that it carries its own."""
    for index, binding in enumerate(bindings):
        if not linked_collections:
            if binding.collection is UNLINKED:
                raise ValueError(f"binding {index} is missing its collection")
            continue
        if binding.collectionIndex >= len(linked_collections):
            raise ValueError(
                f"binding {index} has collectionIndex {binding.collectionIndex}, "
                f"but there are {len(linked_collections)} linked collections"
            )
        binding.collection = linked_collections[binding.collectionIndex]


class Linked:
    """Mixin of a message which links the collections of its bindings,
    as each instance is parsed or constructed."""

    def _link(self) -> None:
        raise NotImplementedError()

    if _PYDANTIC_V2:

        def model_post_init(self, context: Any, /) -> None:
            self._link()

    else:

        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            self._link()


class CaptureBinding(GenericModel, Generic[ResourceConfig]):
    collection: CollectionSpec = UNLINKED
    collectionIndex: NonNegativeInt = 0
    resourceConfig: ResourceConfig
    resourcePath: list[str]
    stateKey: str
    backfill: NonNegativeInt = 0


class CaptureSpec(Linked, GenericModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    intervalSeconds: NonNegativeInt
    bindings: list[CaptureBinding[ResourceConfig]] = []
    linkedCollections: list[CollectionSpec] = []

    def _link(self) -> None:
        link_bindings(self.bindings, self.linkedCollections)


# JSON type of a component of a derivation's shuffle key.
ShuffleType = Literal["BOOLEAN", "INTEGER", "STRING"]


class DerivationTransform(GenericModel, Generic[ResourceConfig]):
    name: str
    collection: CollectionSpec = UNLINKED
    collectionIndex: NonNegativeInt = 0
    # The transform's `lambda`, which is null if omitted.
    lambdaConfig: ResourceConfig
    shuffleKey: list[str] = []
    readOnly: bool = False
    backfill: NonNegativeInt = 0

    # Derivations parse only under pydantic v2.
    if _PYDANTIC_V2:
        from pydantic import field_validator

        @field_validator("lambdaConfig", mode="before")
        @classmethod
        def _omitted_lambda(cls, value: Any) -> Any:
            return {} if value is None else value

        del field_validator


class Derivation(Linked, GenericModel, Generic[EndpointConfig, ResourceConfig]):
    config: EndpointConfig
    transforms: list[DerivationTransform[ResourceConfig]] = []
    shuffleKeyTypes: list[ShuffleType] = []
    linkedCollections: list[CollectionSpec] = []

    def _link(self) -> None:
        link_bindings(self.transforms, self.linkedCollections)


class DerivedCollectionSpec(CollectionSpec, Generic[EndpointConfig, ResourceConfig]):
    derivation: Derivation[EndpointConfig, ResourceConfig]


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
