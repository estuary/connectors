import abc
from logging import Logger
from typing import Coroutine, Generic, TypeVar

from pydantic import BaseModel

from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.materialize import response
from estuary_cdk.materialize.request import Apply, Open, Spec, Validate
from estuary_cdk.materialize.response import (
    Applied,
    Opened,
    Validated,
    ValidatedBinding,
)

from ..flow import ConnectorState as _BaseConnectorState
from ..flow import EndpointConfig, ResourceConfig
from . import BaseMaterializationConnector, LoadIterator, StoreIterator


class BaseResourceState(abc.ABC, BaseModel, extra="forbid"):
    """
    BaseResourceState is a base class for ResourceState classes.
    """

    pass


_BaseResourceState = TypeVar("_BaseResourceState", bound=BaseResourceState)


class ConnectorState(BaseModel, Generic[_BaseResourceState], extra="forbid"):
    bindings: dict[str, _BaseResourceState] = {}


class DocumentStream(
    BaseMaterializationConnector[EndpointConfig, ResourceConfig, _BaseConnectorState]
):
    def loads_wait_for_acknowledge(self) -> bool:
        return False

    async def validate(
        self, log: Logger, validate: Validate[EndpointConfig, ResourceConfig]
    ) -> Validated:
        validated: list[ValidatedBinding] = []

        for binding in validate.bindings:
            constraints: dict[str, response.ValidatedConstraint] = {}

            for p in binding.collection.projections:
                if p.isPrimarykey:
                    constraints[p.field] = response.ValidatedConstraint(
                        type=response.ValidatedConstraintType.LOCATION_REQUIRED,
                        reason="All locations that are part of the collections key are required",
                    )
                elif p.is_root_document_projection():
                    constraints[p.field] = response.ValidatedConstraint(
                        type=response.ValidatedConstraintType.LOCATION_REQUIRED,
                        reason="The root document must be materialized",
                    )
                else:
                    constraints[p.field] = response.ValidatedConstraint(
                        type=response.ValidatedConstraintType.FIELD_FORBIDDEN,
                        reason="Only the root document and collection keys may be materialized",
                    )

            validated.append(
                response.ValidatedBinding(
                    constraints=constraints, resourcePath=[], deltaUpdates=True
                )
            )

        return response.Validated(bindings=validated)

    async def apply(
        self, log: Logger, apply: Apply[EndpointConfig, ResourceConfig]
    ) -> Applied:
        return Applied()

    async def load(
        self,
        log: Logger,
        loads: LoadIterator[EndpointConfig, ResourceConfig, _BaseConnectorState],
    ) -> _BaseConnectorState | None:
        raise RuntimeError(
            "document stream materialization should not receive load requests"
        )

    async def acknowledge(self, log: Logger) -> _BaseConnectorState | None:
        return None

    @abc.abstractmethod
    async def spec(self, log: Logger, _: Spec) -> ConnectorSpec: ...

    @abc.abstractmethod
    async def open(
        self,
        log: Logger,
        open: Open[EndpointConfig, ResourceConfig, _BaseConnectorState],
    ) -> Opened: ...

    @abc.abstractmethod
    async def store(
        self,
        log: Logger,
        stores: StoreIterator[EndpointConfig, ResourceConfig, _BaseConnectorState],
    ) -> tuple[_BaseConnectorState | None, Coroutine[None, None, None] | None]: ...
