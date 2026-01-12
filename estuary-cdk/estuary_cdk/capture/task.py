from dataclasses import dataclass
import decimal
from estuary_cdk.capture.connector_status import ConnectorStatus
from pydantic import Field
from typing import Generic, Awaitable, Any, BinaryIO, Callable
import orjson
import base64
from logging import Logger
import asyncio
import tempfile
import traceback
import xxhash

from . import request, response
from ._emit import emit_from_buffer
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
    sourcedSchema: response.SourcedSchema | None = None
    checkpoint: response.Checkpoint[ConnectorState] | None = None


def orjson_default(obj):
    # Pydantic automatically serializes Decimals as strings, but orjson doesn't
    # know about that. In order to handle this, we must provide this as
    # the default= kwarg to orjson.dumps
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode('utf-8')
    raise TypeError


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

    connector_status: ConnectorStatus
    """Shared ConnectorStatus instance of the entire capture."""

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
        connector_status: ConnectorStatus,
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
        self.connector_status = connector_status
        self.stopping = stopping

    def captured(self, binding: int, document: Any):
        """Enqueue the document to be captured under the given binding.
        Documents are not actually captured until checkpoint() is called.
        Or, reset() will discard any queued documents."""

        if isinstance(document, dict):
            b = orjson.dumps({
                "captured": {
                    "binding": binding,
                    "doc": document,
                }
            }, default=orjson_default)
        else:
            b = Response(
                captured=response.Captured(binding=binding, doc=document)
            ).model_dump_json(by_alias=True, exclude_unset=True).encode()

        self._buffer.write(b)
        self._buffer.write(b"\n")
        self._hasher.update(b)

    def pending_digest(self) -> str:
        """pending_digest returns the digest of captured() documents
        since the last checkpoint() or reset()"""

        return self._hasher.digest().hex()

    def sourced_schema(self, binding_index: int, schema: dict[str, Any]):
        """Write a SourcedSchema message for the given binding to the buffer.
        SourcedSchema messages won't be emitted until checkpoint() is called."""

        b = Response(
            sourcedSchema=response.SourcedSchema(binding=binding_index, schema_json=schema)
        ).model_dump_json(by_alias=True, exclude_unset=True).encode()

        self._buffer.write(b)
        self._buffer.write(b"\n")

    async def checkpoint(self, state: ConnectorState, merge_patch: bool = True):
        """Emit previously-queued, captured documents followed by a checkpoint."""
        await self._emit(
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
                        parent.connector_status,
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

    async def _emit(self, response: Response[EndpointConfig, ResourceConfig, ConnectorState]):
        self._buffer.write(
            response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self._buffer.write(b"\n")
        self._buffer.seek(0)
        await emit_from_buffer(self._buffer, self._output)
        self.reset()
