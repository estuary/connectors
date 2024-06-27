from __future__ import annotations
from logging import Logger
import time
from typing import  Any, AsyncGenerator, Coroutine
from isal import igzip, igzip_threaded
import base64
import gzip

from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.materialize import (
    BaseMaterializationConnector,
    LoadIterator,
    Request,
    StoreIterator,
    request,
    response,
)
import msgspec
import orjson
import pybase64

from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)


class Connector(
    BaseMaterializationConnector[EndpointConfig, ResourceConfig, ConnectorState]
):
    field_list: tuple[str, ...] = ()


    def request_class(self):  # type: ignore
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    def loads_wait_for_acknowledge(self) -> bool:
        return False

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://estuary.dev",
            configSchema=EndpointConfig.model_json_schema(),
            resourceConfigSchema=ResourceConfig.model_json_schema(),
        )

    async def apply(
        self, log: Logger, apply: request.Apply[EndpointConfig, ResourceConfig]
    ) -> response.Applied:
        return response.Applied(
            actionDescription="\n".join(
                [
                    f"applied collection {b.collection.name} with select fields {b.fieldSelection}"
                    for b in apply.materialization.bindings
                ]
            )
        )

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        validated = [
            response.ValidatedBinding(
                constraints={
                    p.field: response.ValidatedConstraint(
                        type=response.ValidatedConstraintType.LOCATION_RECOMMENDED,
                        reason="test materialization recommends everything",
                    )
                    for p in binding.collection.projections
                },
                resourcePath=[binding.collection.name],
                deltaUpdates=True,
            )
            for binding in validate.bindings
        ]

        return response.Validated(bindings=validated)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> response.Opened:
        self.field_list = open.materialization.bindings[0].fieldSelection.all_fields()

        return response.Opened()

    async def load(
        self, log: Logger, loads: LoadIterator[EndpointConfig, ResourceConfig, ConnectorState]
    ) -> ConnectorState | None:
        async for _ in loads:
            pass

    async def store(
        self, log: Logger, stores: StoreIterator[EndpointConfig, ResourceConfig, ConnectorState]
    ) -> tuple[ConnectorState | None, Coroutine[None, None, None] | None]:


        start_time = time.time()
        count = 0
        async for store in stores:
            count += 1
        took = time.time() - start_time
        # 254k per second with only jsons
        # 85k per second with only packed
        log.info(f"got {count} stores in {took} seconds ({count / took} per second)")

        return ConnectorState(
            last_runtime_cp_json=orjson.dumps(stores.get_runtime_checkpoint()).decode()
        ), self._do_commit(log)


    async def _do_commit(self, log: Logger):
        pass

    async def acknowledge(self, log: Logger) -> ConnectorState | None:
        pass
