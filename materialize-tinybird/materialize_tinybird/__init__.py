import asyncio
import gzip
import io
from logging import Logger
from typing import AsyncGenerator, Coroutine

from estuary_cdk.materialize import BaseMaterializationConnector, LoadIterator, Request, StoreIterator, common
from estuary_cdk.materialize.response import Applied, Opened, Validated
import orjson
from estuary_cdk.flow import AccessToken, ConnectorSpec, MaterializationBinding
from estuary_cdk.http import HTTPMixin, TokenSource
from estuary_cdk.materialize.request import Apply, Open, Store, Validate

from .models import ConnectorState, EndpointConfig, EventsApiResponse, ResourceConfig

API = "https://api.tinybird.co/v0/events"

# Tinybird docs state a request size limit of 10MB. It is not clear if this is
# before or after compression. It is easier to keep track of the request size on
# an uncompressed basis so we'll go with that.
REQUEST_SIZE_LIMIT = 10 * 1000 * 1000


class Connector(
    BaseMaterializationConnector[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
    bindings: list[MaterializationBinding[ResourceConfig]] = []

    @classmethod
    def request_class(cls):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    @classmethod
    def spec(cls) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/materialize-tinybird",
            configSchema=EndpointConfig.model_json_schema(),
            resourceConfigSchema=ResourceConfig.model_json_schema(),
        )

    async def connect(self, endpoint_config: EndpointConfig) -> None:
        self.token_source = TokenSource(credentials=AccessToken(access_token=endpoint_config.token))

    # TODO: Maybe some of this as a DeltaUpdates mixin?
    @classmethod
    def loads_wait_for_acknowledge(cls) -> bool:
        return False
    
    async def load(self, log: Logger, loads: LoadIterator[EndpointConfig, ResourceConfig, ConnectorState]) -> ConnectorState | None:
        raise RuntimeError("connector only supports delta updates")
    ######

    async def validate(self, log: Logger, validate: Validate[EndpointConfig, ResourceConfig]) -> Validated:
        # Make sure all the proposed field types are compatible with the existing column types.
        # If the backfill counter is being incremented, don't worry about it.
        # Do single scalar types, strings formatted as numbers or integers (integers can be gigantic in this case), flow_document as a string i guess?
        # A thing with a default should be considered not nullable
        # You can add columns but probably not make them nullable

        # Going from nullable to not nullable is disallowed. You have to set a default value to allow this.

        raise NotImplementedError

    async def apply(self, log: Logger, apply: Apply[EndpointConfig, ResourceConfig]) -> Applied:
        raise NotImplementedError

    async def open(self, log: Logger, open: Open[EndpointConfig, ResourceConfig, ConnectorState]) -> Opened:
        self.bindings = open.materialization.bindings
        return Opened()

    async def store(self, log: Logger, stores: StoreIterator[EndpointConfig, ResourceConfig, ConnectorState]) -> tuple[ConnectorState | None, Coroutine[None, None, None] | None]:
        sem = asyncio.Semaphore(3)
        batch = bytearray()
        written = 0

        async def _post_data(batch: bytes, datasource_name: str) -> None:
            nonlocal sem

            got = EventsApiResponse.model_validate_json(await self.request(
                log,
                url=API,
                method="POST",
                params={"name": datasource_name, "wait": "true"},
                headers={"Content-Encoding": "gzip"},
                data=io.BytesIO(gzip.compress(batch, 1)),
            ))

            assert got.quarantined_rows == 0
            sem.release()

        async def _send_batch(tg: asyncio.TaskGroup, datasource_name: str) -> None:
            nonlocal sem, batch, written

            await sem.acquire()
            to_send = bytes(batch)
            tg.create_task(_post_data(to_send, datasource_name))
            batch.clear()
            written = 0


        async with asyncio.TaskGroup() as tg:
            async for binding, _stores in common.stores_by_binding(self.bindings, stores):
                async for store in _stores:
                    assert store.doc is not None

                    batch.extend(orjson.dumps(store.doc, option=orjson.OPT_APPEND_NEWLINE))

                    if written >= REQUEST_SIZE_LIMIT:
                        await _send_batch(tg, binding.resourceConfig.name)

                if written:
                    await _send_batch(tg, binding.resourceConfig.name)

        return None, None

    async def acknowledge(self, log: Logger) -> ConnectorState | None:
        return None
