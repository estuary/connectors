from pydantic import BaseModel
from typing import Generic, Awaitable, Any, BinaryIO, Callable
from logging import Logger
import abc
import asyncio
import sys

from . import request, response, Request, Response, Task
from .. import BaseConnector, Stopped
from .common import ConnectorState, _ConnectorState
from ..flow import (
    ConnectorSpec,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
    RotatingOAuth2Credentials,
)
from ..http import HTTPError, HTTPMixin, TokenSource


class BaseCaptureConnector(
    BaseConnector[Request[EndpointConfig, ResourceConfig, _ConnectorState]],
    HTTPMixin,
    Generic[EndpointConfig, ResourceConfig, _ConnectorState],
):
    output: BinaryIO = sys.stdout.buffer

    @abc.abstractmethod
    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        raise NotImplementedError()

    @abc.abstractmethod
    async def discover(
        self,
        log: Logger,
        discover: request.Discover[EndpointConfig],
    ) -> response.Discovered[ResourceConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        raise NotImplementedError()

    async def apply(
        self,
        log: Logger,
        apply: request.Apply[EndpointConfig, ResourceConfig],
    ) -> response.Applied:
        return response.Applied(actionDescription="")

    @abc.abstractmethod
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        raise NotImplementedError()

    async def acknowledge(self, acknowledge: request.Acknowledge) -> None:
        return None  # No-op.

    async def handle(
        self,
        log: Logger,
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

            # Gracefully exit after a moderate period of time.
            # We don't do this within the TaskGroup because we don't
            # want to block on it.
            asyncio.create_task(periodic_stop())

            # Rotate refresh tokens for credentials with periodically expiring refresh tokens.
            if isinstance(self.token_source, TokenSource) and isinstance(self.token_source.credentials, RotatingOAuth2Credentials):
                await self._rotate_refresh_tokens(log, open)

            async with asyncio.TaskGroup() as tg:

                task = Task(
                    log.getChild("capture"),
                    "capture",
                    self.output,
                    stopping,
                    tg,
                )
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

    def _emit(self, response: Response[EndpointConfig, ResourceConfig, ConnectorState]):
        self.output.write(
            response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self.output.write(b"\n")
        self.output.flush()

    def _checkpoint(
        self,
        state: ConnectorState,
        merge_patch: bool = True
    ):
        r = Response[Any, Any, ConnectorState](
            checkpoint=response.Checkpoint(
                state=ConnectorStateUpdate(updated=state, mergePatch=merge_patch)
            )
        )

        self._emit(r)

    async def _encrypt_config(
        self,
        log: Logger,
        config: EndpointConfig,
    ) -> bytes:
        assert isinstance(config, BaseModel)
        ENCRYPTION_URL = "https://config-encryption.estuary.dev/v1/encrypt-config"

        body = {
            # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
            "config": config.model_dump(mode="json"),
            "schema": config.model_json_schema(),
        }

        encrypted_config = await self.request(log, ENCRYPTION_URL, "POST", json=body)

        return encrypted_config

    async def _rotate_refresh_tokens(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ):
        assert isinstance(self.token_source, TokenSource)
        assert isinstance(self.token_source.credentials, RotatingOAuth2Credentials)

        try:
            response = await self.token_source.initialize_oauth_tokens(log, self)

            # Update the state with the refresh token we received to ensure the
            # connector's state always has a valid refresh token.
            self._checkpoint(
                state=ConnectorState(
                    refresh_token=response.refresh_token,
                )
            )

        except HTTPError as err:
            # Is there a better way to detect if the config's refresh token is invalid based on the connector?
            # Maybe allow connectors to specify an error code & message for expired refresh tokens?
            if  500 > err.code >= 400 and "invalid_grant" in err.message:
                assert isinstance(open.state.refresh_token, str)
                # assert isinstance(open.capture.config.credentials, RotatingOAuth2Credentials) # type: ignore

                # Set token source to use decrypted token from state.
                self.token_source.credentials.refresh_token = open.state.refresh_token

                # Get new config token.
                new_config_token = (await self.token_source.initialize_oauth_tokens(
                    log,
                    self,
                )).refresh_token

                # Get new state token.
                new_state_token = (await self.token_source.initialize_oauth_tokens(
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
                # open.capture.config.credentials.refresh_token = new_config_token # type: ignore
                self.token_source.credentials.refresh_token = new_config_token
                log.info("Reassigned token source's refresh token to new config token.", {
                    "open.capture.config.credentials.refresh_token": open.capture.config.credentials.refresh_token, #type: ignore
                    "self.token_source.credentials.refresh_token": self.token_source.credentials.refresh_token,
                })

                # Encrypt the updated config and emit an event telling the control plane to publish a new spec.
                encrypted_config = await self._encrypt_config(log, open.capture.config)
                log.info("Some structured log with the encrypted config", {
                    # Name might need to be different. Need to coordinate with what the control plane expects.
                    "newConfig": encrypted_config,
                    # Need to confirm what eventType this should be.
                    "eventType": "configUpdate",
                })
            else:
                raise
