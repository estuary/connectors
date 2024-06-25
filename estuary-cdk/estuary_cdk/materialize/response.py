from enum import IntEnum
from typing import Any, Generic

from pydantic import BaseModel, NonNegativeInt

from ..flow import ConnectorState, ConnectorStateUpdate


class ValidatedConstraintType(IntEnum):
    INVALID = 0
    FIELD_REQUIRED = 1
    LOCATION_REQUIRED = 2
    LOCATION_RECOMMENDED = 3
    FIELD_OPTIONAL = 4
    FIELD_FORBIDDEN = 5
    UNSATISFIABLE = 6


class ValidatedConstraint(BaseModel):
    type: ValidatedConstraintType
    reason: str


class ValidatedBinding(BaseModel):
    constraints: dict[str, ValidatedConstraint]
    resourcePath: list[str]
    deltaUpdates: bool


class Validated(BaseModel):
    bindings: list[ValidatedBinding]


class Applied(BaseModel):
    actionDescription: str | None = None


class Opened(BaseModel):
    runtimeCheckpoint: dict[Any, Any] | None = None


# We'll just use orjson for this
# class Loaded(BaseModel):
#     binding: NonNegativeInt
#     docJson: str


class Flushed(BaseModel, Generic[ConnectorState]):
    """Optional update to ConnectorState.
    This update is durably written before the connector receives a following
    Store or StartCommit request."""

    state: ConnectorStateUpdate[ConnectorState] | None = None


class StartedCommit(BaseModel, Generic[ConnectorState]):
    """Optional *transactional* update to ConnectorState.
    This update commits atomically with the Flow recovery log checkpoint."""

    state: ConnectorStateUpdate[ConnectorState] | None = None


class Acknowledged(BaseModel, Generic[ConnectorState]):
    """Optional *non-transactional* update to ConnectorState.
    This update is not transactional and the connector must tolerate a future,
    duplicate Request.Acknowledge of this same checkpoint and connector state,
    even after having previously responded with Acknowledged and a (discarded)
    connector state update."""

    state: ConnectorStateUpdate[ConnectorState] | None = None
