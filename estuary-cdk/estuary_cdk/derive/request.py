from typing import Any, Generic, TypeVar

from pydantic import BaseModel, NonNegativeInt

from ..flow import (
    UNLINKED,
    CollectionSpec,
    ConnectorState,
    DerivedCollectionSpec,
    EndpointConfig,
    Linked,
    RangeSpec,
    ResourceConfig,
    ShuffleType,
    UUIDParts,
    link_bindings,
)

# Generic type of a document read by a transform.
SourceDocument = TypeVar("SourceDocument")


class ValidateTransform(BaseModel, Generic[ResourceConfig]):
    name: str
    collection: CollectionSpec = UNLINKED
    collectionIndex: NonNegativeInt = 0
    # The transform's `lambda`, which is null if omitted.
    lambdaConfig: ResourceConfig
    backfill: NonNegativeInt = 0

    from pydantic import field_validator

    @field_validator("lambdaConfig", mode="before")
    @classmethod
    def _omitted_lambda(cls, value: Any) -> Any:
        return {} if value is None else value

    del field_validator


class Validate(Linked, BaseModel, Generic[EndpointConfig, ResourceConfig]):
    config: EndpointConfig
    # The derived collection.
    collection: CollectionSpec
    transforms: list[ValidateTransform[ResourceConfig]] = []
    shuffleKeyTypes: list[ShuffleType] = []
    linkedCollections: list[CollectionSpec] = []
    # The last published collection, if any. Its derivation isn't modeled,
    # as its configuration may not parse as the current types.
    lastCollection: CollectionSpec | None = None
    lastVersion: str = ""

    def _link(self) -> None:
        link_bindings(self.transforms, self.linkedCollections)


class Open(BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    collection: DerivedCollectionSpec[EndpointConfig, ResourceConfig]
    version: str
    range: RangeSpec
    state: ConnectorState


class Shuffle(BaseModel):
    # Shuffle key of the document, as a JSON array.
    key: list[Any] = []
    hash: int = 0


class Read(BaseModel, Generic[SourceDocument]):
    # Index of the transform which read the document.
    transform: NonNegativeInt = 0
    uuid: UUIDParts
    shuffle: Shuffle = Shuffle()
    doc: SourceDocument


class Flush(BaseModel):
    # Aggregated connector state patches contributed by all participating
    # shards in the previous Flush iteration of the current transaction.
    # It's empty on the first iteration.
    statePatches: list[Any] = []


class Reset(BaseModel):
    pass
