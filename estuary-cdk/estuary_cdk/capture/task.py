import asyncio
import base64
import decimal
import tempfile
import traceback
from collections.abc import Awaitable
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, BinaryIO, Callable, Generic

import orjson
import xxhash
from pydantic import Field

from estuary_cdk.capture.connector_status import ConnectorStatus

from ..flow import (
    ConnectorSpec,
    ConnectorState,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
)
from ..pydantic_polyfill import GenericModel
from . import request, response
from .transactor import Transactor


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

        `webhook_event` is a separate event for the webhook server. It is set
        after all non-webhook tasks have completed, signaling the webhook server
        to reject new requests with 503 and clean up.
        """

        event: asyncio.Event = field(default_factory=asyncio.Event)
        webhook_event: asyncio.Event = field(default_factory=asyncio.Event)
        webhook_task: asyncio.Task[None] | None = None
        first_error: Exception | None = None
        first_error_task: str | None = None

    stopping: Stopping

    transactor: Transactor
    """Shared Transactor that coordinates stdout writes and Acknowledge pairing."""

    requires_ack: bool
    """When True, checkpoint() blocks until Flow ACKs the emitted checkpoint.
    Inherited by children via spawn_child."""

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
        transactor: Transactor,
        requires_ack: bool = False,
    ):
        self._buffer = tempfile.SpooledTemporaryFile(max_size=self.MAX_BUFFER_MEM)
        self._hasher = xxhash.xxh3_128()
        self._name = name
        self._output = output
        self._tg = tg
        self.log = log
        self.connector_status = connector_status
        self.stopping = stopping
        self.transactor = transactor
        self.requires_ack = requires_ack

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

    async def _emit_checkpoint(
        self, state: ConnectorState, merge_patch: bool = True
    ) -> asyncio.Future[None] | None:
        checkpoint = Response[Any, Any, ConnectorState](
            checkpoint=response.Checkpoint(
                state=ConnectorStateUpdate(updated=state, mergePatch=merge_patch)
            )
        )
        self._buffer.write(
            checkpoint.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self._buffer.write(b"\n")

        completion = await self.transactor.commit(
            self._buffer, wait_for_ack=self.requires_ack
        )
        self.reset()

        return completion

    async def checkpoint(self, state: ConnectorState, merge_patch: bool = True):
        """Emit previously-queued, captured documents followed by a checkpoint.

        When this Task's `requires_ack` is True, the call blocks on Flow's
        Acknowledge for the emitted checkpoint before returning.

        Captures are at-least-once: once the buffer has been written to stdout
        the documents may be durable, regardless of whether this coroutine
        returns. Cancellation between the write and the ACK can therefore
        produce a duplicate when the sender retries on the resulting failure."""

        completion = await self._emit_checkpoint(state, merge_patch)

        if completion is None:
            return

        try:
            await completion
        except asyncio.CancelledError:
            self.log.warning(
                (
                    "checkpoint cancelled while awaiting ACK;"
                    " documents may be durable but caller will see failure"
                ),
                {"task": self._name},
            )
            raise

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
                        parent.transactor,
                        parent.requires_ack,
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
