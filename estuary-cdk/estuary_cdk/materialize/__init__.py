import abc
import asyncio
import base64
import sys
import fdb.tuple # type: ignore
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    BinaryIO,
    Coroutine,
    Generic,
)

import orjson
from pydantic import BaseModel, Field, NonNegativeInt

from .. import BaseConnector
from ..flow import (
    ConnectorSpec,
    ConnectorState,
    ConnectorStateUpdate,
    EndpointConfig,
    ResourceConfig,
)
from . import request, response


class Request(
    BaseModel, Generic[EndpointConfig, ResourceConfig, ConnectorState], extra="forbid"
):
    spec: request.Spec | None = None
    validate_: request.Validate[EndpointConfig, ResourceConfig] | None = Field(
        default=None, alias="validate"
    )
    apply: request.Apply[EndpointConfig, ResourceConfig] | None = None
    open: request.Open[EndpointConfig, ResourceConfig, ConnectorState] | None = None
    load: request.Load | None = None
    flush: request.Flush | None = None
    store: request.Store | None = None
    startCommit: request.StartCommit | None = None
    acknowledge: request.Acknowledge | None = None


class Response(BaseModel, Generic[ConnectorState], extra="forbid"):
    spec: ConnectorSpec | None = None
    validated: response.Validated | None = None
    applied: response.Applied | None = None
    opened: response.Opened | None = None
    flushed: response.Flushed[ConnectorState] | None = None
    startedCommit: response.StartedCommit[ConnectorState] | None = None
    acknowledged: response.Acknowledged[ConnectorState] | None = None


class ExceptEOF(Exception):
    pass


class LoadIterator(Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    reqs: AsyncGenerator[Request[EndpointConfig, ResourceConfig, ConnectorState], None]

    def __init__(
        self,
        reqs: AsyncGenerator[Request[EndpointConfig, ResourceConfig, ConnectorState], None],
    ):
        self.reqs = reqs

    def __aiter__(self) -> "LoadIterator[EndpointConfig, ResourceConfig, ConnectorState]":
        return self

    async def __anext__(self) -> request.Load:
        try:
            n = await anext(self.reqs)
        except StopAsyncIteration:
            raise ExceptEOF
        except Exception as e:
            raise e

        if load := n.load:
            self._decode_tuples(load)
            return load
        elif _ := n.flush:
            raise StopAsyncIteration
        else:
            raise Exception(f"expected either load or flush but got {n}")

    def _decode_tuples(self, s: request.Load):
        # TODO: The stdlib base64 library is pretty slow, but long-term we should strive to not have
        # to do this tuple conversion in the connector, since fdb tuple decoding is also slow.

        if not s.keyJson:
            assert s.keyPacked is not None
            s.keyJson = fdb.tuple.unpack(base64.standard_b64decode(s.keyPacked))  # type: ignore


class StoreIterator(Generic[EndpointConfig, ResourceConfig, ConnectorState]):
    reqs: AsyncGenerator[Request[EndpointConfig, ResourceConfig, ConnectorState], None]
    runtime_checkpoint: dict[Any, Any] | None = None

    def __init__(
        self,
        reqs: AsyncGenerator[Request[EndpointConfig, ResourceConfig, ConnectorState], None],
    ):
        self.reqs = reqs

    def __aiter__(self) -> "StoreIterator[EndpointConfig, ResourceConfig, ConnectorState]":
        return self

    async def __anext__(self) -> request.Store:
        req = await anext(self.reqs)
        if store := req.store:
            self._decode_tuples(store)
            return store
        elif start_commit := req.startCommit:
            self.runtime_checkpoint = start_commit.runtimeCheckpoint
            raise StopAsyncIteration
        else:
            raise Exception(f"expected either store or startCommit but got {req}")

    # TODO: Other useful aggregated properties, like total documents iterated etc.
    def get_runtime_checkpoint(self) -> dict[Any, Any]:
        assert self.runtime_checkpoint is not None
        return self.runtime_checkpoint

    def _decode_tuples(self, s: request.Store):
        # TODO: The stdlib base64 library is pretty slow, but long-term we should strive to not have
        # to do this tuple conversion in the connector, since fdb tuple decoding is also slow.

        if not s.keyJson:
            assert s.keyPacked is not None
            s.keyJson = fdb.tuple.unpack(base64.standard_b64decode(s.keyPacked))  # type: ignore

        if not s.valuesJson:
            assert s.valuesPacked is not None
            s.valuesJson = fdb.tuple.unpack(  # type: ignore
                base64.standard_b64decode(s.valuesPacked)
            )

class BaseMaterializationConnector(
    BaseConnector[Request[EndpointConfig, ResourceConfig, ConnectorState]],
    Generic[EndpointConfig, ResourceConfig, ConnectorState],
):
    output: BinaryIO = sys.stdout.buffer

    # TODO: Ack delay

    @abc.abstractmethod
    def loads_wait_for_acknowledge(self) -> bool: ...

    @abc.abstractmethod
    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec: ...

    @abc.abstractmethod
    async def validate(
        self, log: Logger, validate: request.Validate[EndpointConfig, ResourceConfig]
    ) -> response.Validated: ...

    @abc.abstractmethod
    async def apply(
        self, log: Logger, apply: request.Apply[EndpointConfig, ResourceConfig]
    ) -> response.Applied: ...

    @abc.abstractmethod
    async def load(self, log: Logger, loads: LoadIterator[EndpointConfig, ResourceConfig, ConnectorState]) -> ConnectorState | None: ...

    @abc.abstractmethod
    async def store(
        self, log: Logger, stores: StoreIterator[EndpointConfig, ResourceConfig, ConnectorState]
    ) -> tuple[ConnectorState | None, Coroutine[None, None, None] | None]: ...

    @abc.abstractmethod
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> response.Opened: ...

    @abc.abstractmethod
    async def acknowledge(self, log: Logger) -> ConnectorState | None: ...

    async def handle_requests(
        self,
        log: Logger,
        request: Request[EndpointConfig, ResourceConfig, ConnectorState],
        requests: AsyncGenerator[
            Request[EndpointConfig, ResourceConfig, ConnectorState], None
        ],
    ) -> Coroutine[None, None, None] | None:
        if spec := request.spec:
            res = await self.spec(log, spec)
            res.protocol = 3032023
            self._emit_model(Response(spec=res))

        elif validate := request.validate_:
            self._emit_model(Response(validated=await self.validate(log, validate)))

        elif apply := request.apply:
            self._emit_model(Response(applied=await self.apply(log, apply)))

        elif open := request.open:
            self._emit_model(Response(opened=await self.open(log, open)))
            await self._run_transactions(log, requests)

        return None

    def loaded(self, binding: NonNegativeInt, doc_json: str):
        self.output.write(
            orjson.dumps(
                {
                    "binding": binding,
                    "docJson": doc_json,
                },
                option=orjson.OPT_APPEND_NEWLINE
            )
        )

    async def _run_transactions(
        self, log: Logger, requests: AsyncGenerator[Request[EndpointConfig, ResourceConfig, ConnectorState], None]
    ):
        commit_task: asyncio.Task[None] | None = None
        acknowledge_task: asyncio.Task[None] | None = None

        async def run_ack():
            nonlocal commit_task

            if commit_task is not None:
                await commit_task

            acknowledge_res = Response(
                acknowledged=response.Acknowledged[ConnectorState]()
            )
            if acknowledge_state_update := await self.acknowledge(log):
                acknowledge_res.acknowledged = response.Acknowledged[ConnectorState](
                    state=ConnectorStateUpdate(
                        updated=acknowledge_state_update,
                        mergePatch=True,
                    )
                )

            self._emit_model(acknowledge_res)

        while True:
            if req := (await anext(requests)).acknowledge:
                acknowledge_task = asyncio.create_task(run_ack())
            else:
                raise Exception(f"expected acknowledge but got {req}")

            if self.loads_wait_for_acknowledge():
                await acknowledge_task

            load_task = asyncio.create_task(
                self.load(log, LoadIterator(requests))
            )

            if not acknowledge_task.done():
                await acknowledge_task

            flushed_res = Response(flushed=response.Flushed[ConnectorState]())
            try:
                load_state_update = await load_task
            except ExceptEOF:
                # Clean exist on stdin EOF.
                log.info("EOF signal received")
                return
            except Exception as e:
                raise e

            if load_state_update:
                flushed_res.flushed = response.Flushed[ConnectorState](
                    state=ConnectorStateUpdate(
                        updated=load_state_update,
                        mergePatch=True,
                    )
                )

            self._emit_model(flushed_res)

            store_state_update, start_commit = await self.store(
                log, StoreIterator(requests)
            )

            if start_commit is not None:
                commit_task = asyncio.create_task(start_commit)

            started_commit_res = Response(
                startedCommit=response.StartedCommit[ConnectorState]()
            )
            if store_state_update:
                started_commit_res.startedCommit = response.StartedCommit[
                    ConnectorState
                ](
                    state=ConnectorStateUpdate(
                        updated=store_state_update,
                        mergePatch=True,
                    )
                )

            self._emit_model(started_commit_res)

    def _emit_model(self, response: Response[ConnectorState]):
        self.output.write(
            response.model_dump_json(by_alias=True, exclude_unset=True).encode()
        )
        self.output.write(b"\n")
        self.output.flush()
