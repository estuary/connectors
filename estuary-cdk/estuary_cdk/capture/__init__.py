from dataclasses import dataclass
from pydantic import Field
from typing import Generic, Awaitable, Any, BinaryIO, Callable
from logging import Logger
import abc
import asyncio
import shutil
import sys
import tempfile
import traceback
import xxhash

from . import request, response
from .. import BaseConnector, Stopped
from ..flow import (
    ConnectorSpec,
    ConnectorState,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
)
from ..pydantic_polyfill import GenericModel


class Request(GenericModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    spec: request.Spec | None = None
    discover: request.Discover[EndpointConfig] | None = None
    validate_: request.Validate[EndpointConfig, ResourceConfig] | None = Field(
        default=None, alias="validate"
    )
    apply: request.Apply[EndpointConfig, ResourceConfig] | None = None
    open: request.Open[EndpointConfig, ResourceConfig, ConnectorState] | None = None
    acknowledge: request.Acknowledge | None = None


class Response(GenericModel, Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    spec: ConnectorSpec | None = None
    discovered: response.Discovered[ResourceConfig] | None = None
    validated: response.Validated | None = None
    applied: response.Applied | None = None
    opened: response.Opened | None = None
    captured: response.Captured | None = None
    checkpoint: response.Checkpoint[ConnectorState] | None = None


@dataclass
class Task:
    """
    Task bundles a capture coroutine and its associated context.

    It facilitates the task in queuing any number of captured documents and
    emitting them upon a checkpoint. Each instance of Task manages an independent
    internal buffer (backed by memory or disk) which is only written to the
    connector's output upon a call to checkpoint(). This allows concurrent
    Tasks to capture consistent checkpoints without trampling one another.

    Task also facilitates logging and graceful stop of a capture coroutine.
    """

    log: Logger
    """Attached Logger of this Task instance, to use for scoped logging."""

    @dataclass
    class Stopping:
        """
        Stopping coordinates the graceful exit of capture Tasks.

        Its `event` is set when Tasks should gracefully stop.
        The Task's coroutine should monitor this event and exit when it's set AND
        it has no more immediate work to do (for example, no further documents are
        currently ready to be captured).
        """

        event: asyncio.Event
        first_error: Exception | None = None
        first_error_task: str | None = None

    stopping: Stopping

    _buffer: tempfile.SpooledTemporaryFile
    _hasher: xxhash.xxh3_128
    _name: str
    _output: BinaryIO
    _tg: asyncio.TaskGroup

    MAX_BUFFER_MEM: int = 1_000_000
    """Maximum amount of memory to use for captured documents between checkpoints,
    before spilling to disk."""

    def __init__(
        self,
        log: Logger,
        name: str,
        output: BinaryIO,
        stopping: Stopping,
        tg: asyncio.TaskGroup,
    ):
        self._buffer = tempfile.SpooledTemporaryFile(max_size=self.MAX_BUFFER_MEM)
        self._hasher = xxhash.xxh3_128()
        self._name = name
        self._output = output
        self._tg = tg
        self.log = log
        self.stopping = stopping

    def captured(self, binding: int, document: Any):
        """Enqueue the document to be captured under the given binding.
        Documents are not actually captured until checkpoint() is called.
        Or, reset() will discard any queued documents."""

        b = Response(
            captured=response.Captured(binding=binding, doc=document)
        ).model_dump_json(by_alias=True, exclude_unset=True)

        self._buffer.write(b.encode())
        self._buffer.write(b"\n")
        self._hasher.update(b)

    def pending_digest(self) -> str:
        """pending_digest returns the digest of captured() documents
        since the last checkpoint() or reset()"""

        return self._hasher.digest().hex()

    def checkpoint(self, state: ConnectorState, merge_patch: bool = True):
        """Emit previously-queued, captured documents follows by a checkpoint"""

        self._emit(
            Response[Any, Any, ConnectorState](
                checkpoint=response.Checkpoint(
                    state=ConnectorStateUpdate(updated=state, mergePatch=merge_patch)
                )
            )
        )

    def reset(self):
        """Discard any captured documents, resetting to an empty state."""
        self._buffer.truncate(0)
        self._buffer.seek(0)
        self._hasher.reset()

    def spawn_child(
        self, name_suffix: str, child: Callable[["Task"], Awaitable[None]]
    ) -> asyncio.Task:
        """
        Spawn a child Task of this Task, using the given name suffix and coroutine.
        The child coroutine will be invoked with a child Task and be polled concurrently.
        """

        child_name = f"{self._name}.{name_suffix}"
        child_log = self.log.getChild(name_suffix)

        async def run_task(parent: Task):
            async with asyncio.TaskGroup() as child_tg:
                try:
                    t = Task(
                        child_log,
                        child_name,
                        parent._output,
                        parent.stopping,
                        child_tg,
                    )
                    await child(t)
                except Exception as exc:
                    child_log.error("".join(traceback.format_exception(exc)))

                    if parent.stopping.first_error is None:
                        parent.stopping.first_error = exc
                        parent.stopping.first_error_task = child_name

                    parent.stopping.event.set()

        task = self._tg.create_task(run_task(self))
        task.set_name(child_name)
        return task

    def _emit(self, response: Response[EndpointConfig, ResourceConfig, ConnectorState]):
        self._buffer.write(
            response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self._buffer.write(b"\n")
        self._buffer.seek(0)
        shutil.copyfileobj(self._buffer, self._output)
        self._output.flush()
        self.reset()


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

            async def stop_on_elapsed_interval(interval: int) -> None:
                await asyncio.sleep(interval)
                stopping.event.set()

            # Gracefully exit after the capture interval has elapsed.
            # We don't do this within the TaskGroup because we don't
            # want to block on it.
            asyncio.create_task(stop_on_elapsed_interval(open.capture.intervalSeconds))

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
