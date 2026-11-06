from typing import Generic, TypeVar

from pydantic import BaseModel

from ..flow import ConnectorState, ConnectorStateUpdate

# Generic type of a document published by the derivation.
Document = TypeVar("Document")


class Opened(BaseModel):
    pass


class Published(BaseModel, Generic[Document]):
    doc: Document


class Flushed(BaseModel, Generic[ConnectorState]):
    # Connector state update to contribute this transaction. It's aggregated
    # across shards and fed back through `Request.Flush.statePatches`.
    state: ConnectorStateUpdate[ConnectorState] | None = None
    # Request a further Flush iteration this transaction.
    more: bool = False


class ValidatedTransform(BaseModel):
    # Does this transform never publish documents? A read-only
    # transform needn't wait for prior transactions to commit.
    readOnly: bool = False


class Validated(BaseModel):
    # Validated transforms, in the order of `Request.Validate.transforms`.
    transforms: list[ValidatedTransform]
