import abc
from logging import Logger
from typing import AsyncGenerator, Coroutine, Generic

from estuary_cdk.flow import (
    EndpointConfig,
    MaterializationBinding,
    ResourceConfig,
)
from estuary_cdk.flow import (
    ConnectorState,
)
from estuary_cdk.materialize import (
    BaseMaterializationConnector,
    LoadIterator,
    StoreIterator,
    response,
)
from estuary_cdk.materialize.request import Apply, Open, Store, Validate
from estuary_cdk.materialize.response import (
    Applied,
    Opened,
    Validated,
    ValidatedBinding,
)


class DocumentStream(
    BaseMaterializationConnector[EndpointConfig, ResourceConfig, ConnectorState],
    Generic[EndpointConfig, ResourceConfig, ConnectorState],
):
    bindings: list[MaterializationBinding[ResourceConfig]] = []

    @abc.abstractmethod
    async def store_transaction(
        self,
        log: Logger,
        binding_stores: AsyncGenerator[
            tuple[MaterializationBinding[ResourceConfig], AsyncGenerator[Store, None]], None
        ],
    ) -> None: ...

    @classmethod
    def loads_wait_for_acknowledge(cls) -> bool:
        return False

    async def open(
        self,
        log: Logger,
        open: Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> Opened:
        self.bindings = open.materialization.bindings
        return Opened()

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
                    constraints=constraints, resourcePath=[binding.collection.name], deltaUpdates=True
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
        loads: LoadIterator[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> ConnectorState | None:
        raise RuntimeError(
            "document stream materialization should not receive load requests"
        )
    
    async def store(
        self,
        log: Logger,
        stores: StoreIterator[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[ConnectorState | None, Coroutine[None, None, None] | None]:
        await self.store_transaction(log, stores_by_binding(self.bindings, stores))

        return None, None

    async def acknowledge(self, log: Logger) -> ConnectorState | None:
        return None


async def stores_by_binding(
    bindings: list[MaterializationBinding[ResourceConfig]],
    stores: StoreIterator[EndpointConfig, ResourceConfig, ConnectorState],
) -> AsyncGenerator[
    tuple[MaterializationBinding[ResourceConfig], AsyncGenerator[Store, None]], None
]:
    ref_store = await anext(stores)
    finished = False

    async def gen() -> AsyncGenerator[Store, None]:
        nonlocal ref_store
        nonlocal finished
        yield ref_store

        async for store in stores:
            if store.binding != ref_store.binding:
                ref_store = store
                return

            yield store

        finished = True

    while not finished:
        yield bindings[ref_store.binding or 0], gen()
