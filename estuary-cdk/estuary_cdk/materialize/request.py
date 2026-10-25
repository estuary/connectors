from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, NonNegativeInt

from ..flow import (
    UNLINKED,
    CollectionSpec,
    ConnectorState,
    ConnectorType,
    EndpointConfig,
    Linked,
    MaterializationSpec,
    RangeSpec,
    RawResourceConfig,
    ResourceConfig,
    link_bindings,
)

# Generic type of a binding's key, as an array of its group-by fields.
Key = TypeVar("Key")
# Generic type of a binding's stored documents.
Document = TypeVar("Document")

# A last-applied materialization, whose configurations may not parse as the
# current types and are left unmodeled.
LastMaterialization = MaterializationSpec[dict[str, Any], RawResourceConfig]


class ValidateBinding(BaseModel, Generic[ResourceConfig]):
    resourceConfig: ResourceConfig
    collection: CollectionSpec = UNLINKED
    collectionIndex: NonNegativeInt = 0
    fieldConfig: dict[str, Any] = {}
    backfill: NonNegativeInt = 0
    # Ordered group-by fields, which are the binding's key.
    groupBy: list[str] = []


class Validate(Linked, BaseModel, Generic[EndpointConfig, ResourceConfig]):
    name: str
    connectorType: ConnectorType
    config: EndpointConfig
    bindings: list[ValidateBinding[ResourceConfig]] = []
    linkedCollections: list[CollectionSpec] = []
    lastMaterialization: LastMaterialization | None = None
    lastVersion: str = ""

    def _link(self) -> None:
        link_bindings(self.bindings, self.linkedCollections)


class Apply(BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    materialization: MaterializationSpec[EndpointConfig, ResourceConfig]
    version: str
    lastMaterialization: LastMaterialization | None = None
    lastVersion: str = ""
    # Connector state is omitted if it's empty.
    state: ConnectorState = Field(default_factory=dict, validate_default=True)


class Open(BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    materialization: MaterializationSpec[EndpointConfig, ResourceConfig]
    version: str
    range: RangeSpec
    state: ConnectorState = Field(default_factory=dict, validate_default=True)


class Load(BaseModel, Generic[Key]):
    # Index of the binding whose key is loaded.
    binding: NonNegativeInt = 0
    key: Key


class BackfillBoundary(BaseModel):
    binding: NonNegativeInt = 0
    # Documents published before this time were superseded by the backfill.
    timestamp: datetime


class Flush(BaseModel):
    # Aggregated connector state patches of all shards' prior-transaction
    # Acknowledged responses, including this shard's own.
    statePatches: list[Any] = []
    backfillBegins: list[BackfillBoundary] = []
    # A stored document of a completed backfill's binding which was published
    # before its `timestamp` is stale, and may be deleted.
    backfillCompletes: list[BackfillBoundary] = []


class Store(BaseModel, Generic[Key, Document]):
    # Index of the binding whose document is stored.
    binding: NonNegativeInt = 0
    key: Key
    # Selected non-key fields, in the order of the binding's field selection.
    values: list[Any] = []
    doc: Document
    # Has this key been loaded or stored before?
    exists: bool = False
    # Is this document a deletion? A deletion also `exists`.
    delete: bool = False


class StartCommit(BaseModel):
    # Runtime checkpoint of this transaction, which a remote-authoritative
    # connector commits with its own transaction and returns upon Open.
    runtimeCheckpoint: dict[str, Any] | None = None
    # Aggregated connector state patches of all shards' current-transaction
    # Flushed responses.
    statePatches: list[Any] = []


class Acknowledge(BaseModel):
    # Aggregated connector state patches of all shards' just-committed
    # StartedCommit responses.
    statePatches: list[Any] = []
