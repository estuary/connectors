from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, NonNegativeInt

from ..flow import ConnectorState, ConnectorStateUpdate, SerPolicy

# Generic type of a binding's loaded documents.
Document = TypeVar("Document")

ConstraintType = Literal[
    # A field of this name must be materialized.
    "FIELD_REQUIRED",
    # A field projecting this location must be materialized.
    "LOCATION_REQUIRED",
    # The field may be materialized.
    "FIELD_OPTIONAL",
    # The field may never be materialized.
    "FIELD_FORBIDDEN",
    # The field may be materialized only after a backfill of the binding.
    "INCOMPATIBLE",
]


class Constraint(BaseModel):
    type: ConstraintType
    # User-facing reason for the constraint.
    reason: str = ""
    # A folded field name, for endpoints which can't represent the field's own.
    foldedField: str = ""


class ProjectionConstraint(BaseModel):
    field: str
    constraint: Constraint


class ValidatedBinding(BaseModel):
    resourcePath: list[str]
    deltaUpdates: bool = False
    serPolicy: SerPolicy | None = None
    # Are fields which differ only in case the same field of the endpoint?
    caseInsensitiveFields: bool = False
    # Projections which have no constraint are forbidden.
    projectionConstraints: list[ProjectionConstraint] = []


class Validated(BaseModel):
    # Validated bindings, in the order of `Request.Validate.bindings`.
    bindings: list[ValidatedBinding]


class Applied(BaseModel, Generic[ConnectorState]):
    # Description of the actions taken, which is empty if there were none.
    actionDescription: str = ""
    # Transactional update of connector state.
    state: ConnectorStateUpdate[ConnectorState] | None = None


class Opened(BaseModel):
    # Checkpoint of a remote-authoritative connector, to begin from.
    runtimeCheckpoint: dict[str, Any] | None = None
    # Send a Load for every key, even keys the runtime knows were never stored.
    disableLoadOptimization: bool = False


class Loaded(BaseModel, Generic[Document]):
    # Index of the binding of the loaded document.
    binding: NonNegativeInt = 0
    doc: Document


class Flushed(BaseModel, Generic[ConnectorState]):
    # Update of connector state, which is durable before the first Store.
    state: ConnectorStateUpdate[ConnectorState] | None = None


class StartedCommit(BaseModel, Generic[ConnectorState]):
    # Transactional update of connector state, which commits with the
    # transaction's runtime checkpoint.
    state: ConnectorStateUpdate[ConnectorState] | None = None


class Acknowledged(BaseModel, Generic[ConnectorState]):
    # Non-transactional update of connector state. It may be discarded, and
    # the connector must tolerate a repeated Acknowledge.
    state: ConnectorStateUpdate[ConnectorState] | None = None
