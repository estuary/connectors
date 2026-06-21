import io
import json
import logging
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, AsyncGenerator, BinaryIO
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceState,
    _binding_snapshot_task,  # pyright: ignore[reportPrivateUsage]
    open_binding,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.flow import CaptureBinding, CollectionSpec

COMPOUND_KEY = ["/_meta/store", "/_meta/row_id"]


class StoreDoc(BaseDocument):
    """A document whose _meta carries a connector-specific `store` discriminator,
    mirroring how a multi-store connector keys a snapshot on [store, row_id]."""

    class Meta(BaseDocument.Meta):
        store: str = ""

    meta_: Meta = Field(
        default_factory=lambda: StoreDoc.Meta(op="u"),
        alias="_meta",
    )
    id: str


def _doc(store: str, id: str) -> StoreDoc:
    return StoreDoc(id=id, _meta=StoreDoc.Meta(op="u", store=store))


def _binding(key: list[str], name: str = "staff_members") -> CaptureBinding[ResourceConfig]:
    return CaptureBinding(
        collection=CollectionSpec(name="c/" + name, key=key, writeSchema={}),
        resourceConfig=ResourceConfig(name=name, interval=timedelta()),
        resourcePath=[name],
        stateKey=name,
    )


def _task(output: BinaryIO, stopping: Task.Stopping) -> Task:
    return Task(
        log=logging.getLogger("test-snapshot"),
        connector_status=MagicMock(),
        name="test-snapshot",
        output=output,
        stopping=stopping,
        tg=MagicMock(),
        transactor=Transactor(output),
        catalog_task_name="acmeCo/test-snapshot",
        requires_ack=False,
    )


def _read(output: BinaryIO) -> list[dict[str, Any]]:
    _ = output.seek(0)
    return [json.loads(line) for line in output.read().decode().splitlines() if line]


class _Passes:
    """Drives fetch_snapshot deterministically: yields the documents for each
    pass in turn, and sets the stopping event after `stop_after` passes so the
    snapshot loop exits."""

    def __init__(
        self,
        passes: list[list[StoreDoc]],
        stopping: Task.Stopping,
        stop_after: int,
    ):
        self._passes = passes
        self._i = 0
        self._stopping = stopping
        self._stop_after = stop_after

    def __call__(self, log: logging.Logger) -> AsyncGenerator[StoreDoc, None]:
        async def gen() -> AsyncGenerator[StoreDoc, None]:
            for d in self._passes[self._i]:
                yield d
            self._i += 1
            if self._i >= self._stop_after:
                self._stopping.event.set()

        return gen()


def _captured_docs(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r["captured"]["doc"] for r in records if "captured" in r]


def _checkpoints(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        r["checkpoint"]["state"]["updated"] for r in records if "checkpoint" in r
    ]


class TestBindingSnapshotSubtask:
    @pytest.mark.asyncio
    async def test_preserves_store_and_assigns_row_id(self):
        output = io.BytesIO()
        stopping = Task.Stopping()
        task = _task(output, stopping)
        binding = _binding(COMPOUND_KEY)

        fetch = _Passes(
            [[_doc("A", "a1"), _doc("A", "a2"), _doc("A", "a3")]],
            stopping,
            stop_after=1,
        )
        tombstone = _doc("A", "")

        await _binding_snapshot_task(
            binding, 0, fetch, None, task, tombstone, subtask_id="A"
        )

        docs = _captured_docs(_read(output))
        assert len(docs) == 3
        # Every emitted doc keeps its store and gets a positional row_id; first
        # snapshot is all creates.
        for i, doc in enumerate(docs):
            assert doc["_meta"]["store"] == "A"
            assert doc["_meta"]["row_id"] == i
            assert doc["_meta"]["op"] == "c"

    @pytest.mark.asyncio
    async def test_checkpoints_dict_keyed_snapshot_state(self):
        output = io.BytesIO()
        stopping = Task.Stopping()
        task = _task(output, stopping)
        binding = _binding(COMPOUND_KEY)

        fetch = _Passes([[_doc("A", "a1")]], stopping, stop_after=1)

        await _binding_snapshot_task(
            binding, 0, fetch, None, task, _doc("A", ""), subtask_id="A"
        )

        checkpoints = _checkpoints(_read(output))
        assert checkpoints
        snapshot = checkpoints[-1]["bindingStateV1"]["staff_members"]["snapshot"]
        # Dict-keyed by subtask id so the runtime merge-patches per subtask
        # rather than letting siblings clobber each other.
        assert set(snapshot.keys()) == {"A"}
        assert snapshot["A"]["last_count"] == 1

    @pytest.mark.asyncio
    async def test_deletion_tombstone_carries_store(self):
        output = io.BytesIO()
        stopping = Task.Stopping()
        task = _task(output, stopping)
        binding = _binding(COMPOUND_KEY)

        # Pass 1: three docs. Pass 2: a2 removed -> snapshot shrinks to two, so
        # the trailing row_id (2) must be tombstoned, carrying store "A".
        fetch = _Passes(
            [
                [_doc("A", "a1"), _doc("A", "a2"), _doc("A", "a3")],
                [_doc("A", "a1"), _doc("A", "a3")],
            ],
            stopping,
            stop_after=2,
        )

        await _binding_snapshot_task(
            binding, 0, fetch, None, task, _doc("A", ""), subtask_id="A"
        )

        docs = _captured_docs(_read(output))
        tombstones = [d for d in docs if d["_meta"]["op"] == "d"]
        assert len(tombstones) == 1
        assert tombstones[0]["_meta"] == {"op": "d", "row_id": 2, "store": "A"}

    @pytest.mark.asyncio
    async def test_unchanged_snapshot_is_suppressed(self):
        output = io.BytesIO()
        stopping = Task.Stopping()
        task = _task(output, stopping)
        binding = _binding(COMPOUND_KEY)

        # Three identical passes. Pass 1 emits creates; pass 2 emits updates
        # (op flips c->u, so its digest differs and it is NOT suppressed and
        # establishes the steady-state digest); pass 3 is byte-identical to
        # pass 2, so its documents are reset (suppressed). Emitting 4 (2+2+0)
        # rather than 6 docs confirms suppression.
        same = [_doc("A", "a1"), _doc("A", "a2")]
        fetch = _Passes(
            [list(same), list(same), list(same)], stopping, stop_after=3
        )

        await _binding_snapshot_task(
            binding, 0, fetch, None, task, _doc("A", ""), subtask_id="A"
        )

        docs = _captured_docs(_read(output))
        assert len(docs) == 4  # pass 3 suppressed
        assert not [d for d in docs if d["_meta"]["op"] == "d"]


class TestOpenBindingSnapshotDict:
    def _state(self) -> ResourceState:
        return ResourceState(
            snapshot={
                "A": ResourceState.Snapshot(
                    updated_at=datetime.now(tz=UTC), last_count=0, last_digest=""
                ),
                "B": None,
            }
        )

    async def _noop(self, log: logging.Logger) -> AsyncGenerator[StoreDoc, None]:
        if False:
            yield  # pragma: no cover

    def test_spawns_one_subtask_per_store_with_wired_state(self):
        output = io.BytesIO()
        task = _task(output, Task.Stopping())
        task.spawn_child = MagicMock()  # type: ignore[method-assign]

        state = self._state()
        fetch = {"A": self._noop, "B": self._noop}
        tombstone = {"A": _doc("A", ""), "B": _doc("B", "")}

        open_binding(
            _binding(COMPOUND_KEY),
            0,
            state,
            task,
            fetch_snapshot=fetch,
            tombstone=tombstone,
        )

        assert task.spawn_child.call_count == 2
        by_name = {
            call.args[0]: call.args[1] for call in task.spawn_child.call_args_list
        }
        assert set(by_name) == {
            "staff_members.snapshot.A",
            "staff_members.snapshot.B",
        }
        a = by_name["staff_members.snapshot.A"].keywords
        assert a["subtask_id"] == "A"
        assert a["tombstone"] is tombstone["A"]
        assert a["state"] is state.snapshot["A"]
        # A store with no prior snapshot state is wired with None (the task
        # self-initializes it).
        assert by_name["staff_members.snapshot.B"].keywords["state"] is None

    def test_requires_compound_key(self):
        output = io.BytesIO()
        task = _task(output, Task.Stopping())
        with pytest.raises(AssertionError, match="compound collection key"):
            open_binding(
                _binding(["/_meta/row_id"]),
                0,
                self._state(),
                task,
                fetch_snapshot={"A": self._noop},
                tombstone={"A": _doc("A", "")},
            )

    def test_requires_row_id_in_key(self):
        output = io.BytesIO()
        task = _task(output, Task.Stopping())
        with pytest.raises(AssertionError, match="compound collection key"):
            open_binding(
                _binding(["/_meta/store", "/id"]),
                0,
                self._state(),
                task,
                fetch_snapshot={"A": self._noop},
                tombstone={"A": _doc("A", "")},
            )

    def test_requires_matching_tombstone_dict(self):
        output = io.BytesIO()
        task = _task(output, Task.Stopping())
        with pytest.raises(AssertionError, match="tombstone dict"):
            open_binding(
                _binding(COMPOUND_KEY),
                0,
                self._state(),
                task,
                fetch_snapshot={"A": self._noop, "B": self._noop},
                tombstone={"A": _doc("A", "")},  # missing "B"
            )

    def test_requires_distinct_discriminators(self):
        output = io.BytesIO()
        task = _task(output, Task.Stopping())
        with pytest.raises(AssertionError, match="distinct key discriminator"):
            open_binding(
                _binding(COMPOUND_KEY),
                0,
                self._state(),
                task,
                fetch_snapshot={"A": self._noop, "B": self._noop},
                # Both tombstones carry store "A": their (store, row_id)
                # keyspaces would overlap and clobber each other.
                tombstone={"A": _doc("A", ""), "B": _doc("A", "")},
            )
