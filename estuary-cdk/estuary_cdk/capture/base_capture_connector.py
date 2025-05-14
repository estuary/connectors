from estuary_cdk.capture.connector_status import ConnectorStatus
from pydantic import BaseModel
from typing import Generic, Awaitable, Any, BinaryIO, Callable
from logging import Logger
import abc
import asyncio
import json
import sys

from . import request, response, Request, Response, Task
from .. import BaseConnector, Stopped
from .common import ConnectorState, _ConnectorState
from ..flow import (
    ConnectorSpec,
    ConnectorState as GeneralConnectorState,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
    RotatingOAuth2Credentials,
)
from ..http import HTTPError, HTTPMixin, TokenSource
from ..logger import FlowLogger
from ..utils import sort_dict

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
            self._emit(Response(spec=response))

        elif discover := request.discover:
            self._emit(Response(discovered=await self.discover(log, discover)))

        elif validate := request.validate_:
            self._emit(Response(validated=await self.validate(log, validate)))

        elif apply := request.apply:
            self._emit(Response(applied=await self.apply(log, apply)))

        elif open := request.open:
            opened, capture = await self.open(log, open)
            self._emit(Response(opened=opened))

            stopping = Task.Stopping(asyncio.Event())

            async def periodic_stop() -> None:
                await asyncio.sleep(24 * 60 * 60)  # 24 hours
                stopping.event.set()

            # Rotate refresh tokens for credentials with periodically expiring refresh tokens.
            if isinstance(self.token_source, TokenSource) and isinstance(self.token_source.credentials, RotatingOAuth2Credentials):
                await self._rotate_refresh_tokens(log, open)

            # Gracefully exit after a moderate period of time.
            # We don't do this within the TaskGroup because we don't
            # want to block on it.
            asyncio.create_task(periodic_stop())

            async with asyncio.TaskGroup() as tg:

                task = Task(
                    log.getChild("capture"),
                    ConnectorStatus(log, stopping, tg),
                    "capture",
                    self.output,
                    stopping,
                    tg,
                )
                log.info("Capture started", {"eventType": "connectorStatus"})
                await capture(task)

            # When capture() completes, the connector exits.
            if stopping.first_error:
                raise Stopped(
                    f"Task {stopping.first_error_task}: {stopping.first_error}"
                )
            else:
                raise Stopped(None)

        elif acknowledge := request.acknowledge:
            await self.acknowledge(acknowledge)

        else:
            raise RuntimeError("malformed request", request)

    def _emit(self, response: Response[EndpointConfig, ResourceConfig, GeneralConnectorState]):
        self.output.write(
            response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self.output.write(b"\n")
        self.output.flush()

    def _checkpoint(
        self,
        state: GeneralConnectorState,
        merge_patch: bool = True
    ):
        r = Response[Any, Any, GeneralConnectorState](
            checkpoint=response.Checkpoint(
                state=ConnectorStateUpdate(updated=state, mergePatch=merge_patch)
            )
        )

        self._emit(r)

    async def _encrypt_config(
        self,
        log: FlowLogger,
        config: EndpointConfig,
    ) -> dict[str, Any]:
        assert isinstance(config, BaseModel)
        ENCRYPTION_URL = "https://config-encryption.estuary.dev/v1/encrypt-config"

        # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
        # include=config.model_fields_set ensures only the fields that are explicitly set on the
        # model. This means any default values that are set are included, but any fields that are unset
        # & fallback to some default value are left unset.
        unencrypted_config = config.model_dump(mode="json", include=config.model_fields_set)

        body = {
            # Flow always sorts object properties lexicographically. This keeps a consistent property
            # ordering when encrypting, and `sops` relies on encountered property order when computing
            # the HMAC portion of the `sops` stanza. To align with Flow's sorting behavior, connectors
            # also sort the config's properties before submitting the config to be encrypted.
            "config": sort_dict(unencrypted_config),
            "schema": config.model_json_schema(),
        }

        encrypted_config = await self.request(log, ENCRYPTION_URL, "POST", json=body, _with_token = False)

        return json.loads(encrypted_config.decode('utf-8'))

    async def _rotate_refresh_tokens(
        self,
        log: FlowLogger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ):
        assert isinstance(self.token_source, TokenSource)
        assert isinstance(self.token_source.credentials, RotatingOAuth2Credentials)

        try:
            response = await self.token_source.initialize_oauth2_tokens(log, self)

            # Update the state with the refresh token we received to ensure the
            # connector's state always has a valid refresh token.
            self._checkpoint(
                state=ConnectorState(
                    refresh_token=response.refresh_token,
                )
            )

        except HTTPError as err:
            # TODO(bair): Allow connectors to specify a more specific error message / code when a refresh token is invalid.
            if  500 > err.code >= 400 and "invalid_grant" in err.message:
                assert isinstance(open.state.refresh_token, str)

                # Set token source to use the token from the connector's state.
                self.token_source.credentials.refresh_token = open.state.refresh_token

                # Get a new token for the config.
                new_config_token = (await self.token_source.initialize_oauth2_tokens(
                    log,
                    self,
                )).refresh_token

                # Get a new token for the state. This is done second to ensure the state 
                # token will still be valid if the config token is expired.
                new_state_token = (await self.token_source.initialize_oauth2_tokens(
                    log,
                    self,
                )).refresh_token

                # Emit a checkpoint with the new state token.
                self._checkpoint(
                    state=ConnectorState(
                        refresh_token=new_state_token,
                    )
                )

                # Update the connector's config with the new config token.
                self.token_source.credentials.refresh_token = new_config_token

                # Encrypt the updated config and emit an event telling the control plane to publish a new spec.
                encrypted_config = await self._encrypt_config(log, open.capture.config)
                log.event.config_update("Rotating refresh token in endpoint config.", encrypted_config)
            else:
                raise
