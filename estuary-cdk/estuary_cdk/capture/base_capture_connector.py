from typing import Generic, Awaitable, Any, BinaryIO, Callable
from logging import Logger
import abc
import asyncio
import sys

from . import request, response, Request, Response, Task
from .. import BaseConnector, Stopped
from ..flow import (
    ConnectorSpec,
    ConnectorState,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
)


class BaseCaptureConnector(
    BaseConnector[Request[EndpointConfig, ResourceConfig, ConnectorState]],
    Generic[EndpointConfig, ResourceConfig, ConnectorState],
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
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        raise NotImplementedError()

    async def acknowledge(self, acknowledge: request.Acknowledge) -> None:
        return None  # No-op.

    async def handle(
        self,
        log: Logger,
        request: Request[EndpointConfig, ResourceConfig, ConnectorState],
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
