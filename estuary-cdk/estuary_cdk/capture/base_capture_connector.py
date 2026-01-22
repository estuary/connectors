import abc
import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, BinaryIO, Callable, Generic

from pydantic import BaseModel

from estuary_cdk.capture.connector_status import ConnectorStatus

from .. import BaseConnector, Stopped
from ..flow import (
    ConnectorSpec,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
    RotatingOAuth2Credentials,
)
from ..flow import (
    ConnectorState as GeneralConnectorState,
)
from ..http import HTTPMixin, TokenSource
from ..logger import FlowLogger
from ..utils import format_error_message, sort_dict
from . import Request, Response, Task, request, response
from ._emit import emit_bytes
from .common import _ConnectorState

# Default encryption service URL, can be overridden via ENCRYPTION_URL environment variable
# for local testing with docker gateway addresses like http://172.18.0.1:port
ENCRYPTION_URL = os.getenv("ENCRYPTION_URL") or "https://config-encryption.estuary.dev/v1/encrypt-config"


class BaseCaptureConnector(
    BaseConnector[Request[EndpointConfig, ResourceConfig, _ConnectorState]],
    HTTPMixin,
    Generic[EndpointConfig, ResourceConfig, _ConnectorState],
):
    output: BinaryIO = sys.stdout.buffer

    @abc.abstractmethod
    async def spec(self, log: FlowLogger, _: request.Spec) -> ConnectorSpec:
        raise NotImplementedError()

    @abc.abstractmethod
    async def discover(
        self,
        log: FlowLogger,
        discover: request.Discover[EndpointConfig],
    ) -> response.Discovered[ResourceConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def validate(
        self,
        log: FlowLogger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        raise NotImplementedError()

    async def apply(
        self,
        log: FlowLogger,
        apply: request.Apply[EndpointConfig, ResourceConfig],
    ) -> response.Applied:
        return response.Applied(actionDescription="")

    @abc.abstractmethod
    async def open(
        self,
        log: FlowLogger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        raise NotImplementedError()

    async def acknowledge(self, acknowledge: request.Acknowledge) -> None:
        return None  # No-op.

    async def handle(
        self,
        log: FlowLogger,
        request: Request[EndpointConfig, ResourceConfig, _ConnectorState],
    ) -> None:

        if spec := request.spec:
            response = await self.spec(log, spec)
            response.protocol = 3032023
            await self._emit(Response(spec=response))

        elif discover := request.discover:
            await self._emit(Response(discovered=await self.discover(log, discover)))

        elif validate := request.validate_:
            await self._emit(Response(validated=await self.validate(log, validate)))

        elif apply := request.apply:
            await self._emit(Response(applied=await self.apply(log, apply)))

        elif open := request.open:
            opened, capture = await self.open(log, open)
            await self._emit(Response(opened=opened))

            stopping = Task.Stopping(asyncio.Event())

            async def periodic_stop() -> None:
                await asyncio.sleep(24 * 60 * 60)  # 24 hours
                stopping.event.set()

            # Rotate OAuth2 tokens for credentials with periodically expiring tokens.
            if isinstance(self.token_source, TokenSource) and isinstance(
                self.token_source.credentials, RotatingOAuth2Credentials
            ):
                await self._rotate_oauth2_tokens(log, open)

            # Gracefully exit after a moderate period of time.
            # We don't do this within the TaskGroup because we don't
            # want to block on it.
            asyncio.create_task(periodic_stop())

            async with asyncio.TaskGroup() as tg:

                task = Task(
                    log.getChild("capture"),
                    ConnectorStatus(log, stopping),
                    "capture",
                    self.output,
                    stopping,
                    tg,
                )
                log.event.status("Capture started")
                await capture(task)

            # When capture() completes, the connector exits.
            if stopping.first_error:
                msg = format_error_message(stopping.first_error)

                raise Stopped(f"Task {stopping.first_error_task}: {msg}")
            else:
                raise Stopped(None)

        elif acknowledge := request.acknowledge:
            await self.acknowledge(acknowledge)

        else:
            raise RuntimeError("malformed request", request)

    async def _emit(
        self, response: Response[EndpointConfig, ResourceConfig, GeneralConnectorState]
    ):
        data = response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        await emit_bytes(data + b"\n", self.output)

    async def _checkpoint(self, state: GeneralConnectorState, merge_patch: bool = True):
        r = Response[Any, Any, GeneralConnectorState](
            checkpoint=response.Checkpoint(
                state=ConnectorStateUpdate(updated=state, mergePatch=merge_patch)
            )
        )

        await self._emit(r)

    async def _encrypt_config(
        self,
        log: FlowLogger,
        config: EndpointConfig,
    ) -> dict[str, Any]:
        assert isinstance(config, BaseModel)

        # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
        # include=config.model_fields_set ensures only the fields that are explicitly set on the
        # model. This means any default values that are set are included, but any fields that are unset
        # & fallback to some default value are left unset.
        unencrypted_config = config.model_dump(
            mode="json", include=config.model_fields_set
        )

        body = {
            # Flow always sorts object properties lexicographically. This keeps a consistent property
            # ordering when encrypting, and `sops` relies on encountered property order when computing
            # the HMAC portion of the `sops` stanza. To align with Flow's sorting behavior, connectors
            # also sort the config's properties before submitting the config to be encrypted.
            "config": sort_dict(unencrypted_config),
            "schema": config.model_json_schema(),
        }

        encrypted_config = await self.request(
            log, ENCRYPTION_URL, "POST", json=body, with_token=False
        )

        return json.loads(encrypted_config.decode("utf-8"))

    async def _rotate_oauth2_tokens(
        self,
        log: FlowLogger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ):
        assert isinstance(self.token_source, TokenSource)
        assert isinstance(self.token_source.credentials, RotatingOAuth2Credentials)

        now = datetime.now(tz=UTC)
        access_token_expiration = self.token_source.credentials.access_token_expires_at

        # Rotate tokens if the access token has expired / will expire in the next 5 minutes.
        if access_token_expiration < now + timedelta(minutes=5):
            # Exchange for new access token & refresh token.
            log.info("Attempting to rotate OAuth2 tokens.")
            token_exchange_response = await self.token_source.initialize_oauth2_tokens(
                log, self
            )

            # Replace tokens in the config.
            credentials = self.token_source.credentials
            credentials.access_token = token_exchange_response.access_token
            credentials.refresh_token = token_exchange_response.refresh_token
            credentials.access_token_expires_at = now + timedelta(
                seconds=token_exchange_response.expires_in
            )

            # Encrypt the updated config and emit an event telling the control plane to publish a new spec.
            encrypted_config = await self._encrypt_config(log, open.capture.config)
            log.event.config_update(
                "Rotating OAuth2 tokens in endpoint config.", encrypted_config
            )
