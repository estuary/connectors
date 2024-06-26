import gzip
import io
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.materialize import Request
import orjson
from estuary_cdk.flow import AccessToken, ConnectorSpec, MaterializationBinding
from estuary_cdk.http import HTTPMixin, TokenSource
from estuary_cdk.materialize.common import DocumentStream
from estuary_cdk.materialize.request import Store

from .models import ConnectorState, EndpointConfig, EventsApiResponse, ResourceConfig

API = "https://api.tinybird.co/v0/events"

# Tinybird docs state a request size limit of 10MB. It is not clear if this is
# before or after compression. It is easier to keep track of the request size on
# an uncompressed basis so we'll go with that.
REQUEST_SIZE_LIMIT = 10 * 1000 * 1000


class Connector(
    DocumentStream[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
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

    async def store_transaction(
        self,
        log: Logger,
        binding_stores: AsyncGenerator[
            tuple[MaterializationBinding[ResourceConfig], AsyncGenerator[Store, None]], None
        ],
    ) -> None:
        buf = bytearray()

        async def _send_batch(datasource_name: str) -> None:
            nonlocal buf

            got = EventsApiResponse.model_validate_json(await self.request(
                log,
                url=API,
                method="POST",
                params={"name": datasource_name, "wait": "true"},
                headers={"Content-Encoding": "gzip"},
                data=io.BytesIO(gzip.compress(buf, 1)),
            ))

            assert got.quarantined_rows == 0
            buf.clear()

        async for binding, stores in binding_stores:
            async for store in stores:
                assert store.doc is not None

                buf.extend(orjson.dumps(store.doc, option=orjson.OPT_APPEND_NEWLINE))

                if len(buf) >= REQUEST_SIZE_LIMIT:
                    await _send_batch(binding.resourceConfig.name)

            if len(buf):
                await _send_batch(binding.resourceConfig.name)
