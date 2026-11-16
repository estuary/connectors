import asyncio
import io
import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any, AsyncGenerator, Callable
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, Field

from estuary_cdk.capture import _emit, common, request
from estuary_cdk.capture.backfill_signals import (
    BackfillContext,
    BackfillCoverage,
    BackfillCoverageFn,
    backfill_signal_flag_defaults,
)
from estuary_cdk.capture.common import (
    ConnectorState,
    PageCursor,
    Resource,
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.feature_flags import FeatureFlagsConfig
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec, RangeSpec
from estuary_cdk.utils import json_merge_patch

CUTOFF = datetime(2026, 10, 1, tzinfo=UTC)

Binding = tuple[CaptureBinding[ResourceConfig], Resource[Any, ResourceConfig, ResourceState]]


class _Advanced(FeatureFlagsConfig):
    FEATURE_FLAG_DEFAULTS = backfill_signal_flag_defaults(
        enabled_for_tasks_created_after=date(2000, 1, 1)
    )


class _Config(BaseModel):
    advanced: _Advanced = Field(default_factory=_Advanced)


class _Doc(BaseDocument, extra="allow"):
    id: str


@pytest.fixture(autouse=True)
def _fresh_emit_lock(monkeypatch: pytest.MonkeyPatch):
    # The module-level lock binds to the event loop of its first contended use,
    # and each test runs its own loop.
    monkeypatch.setattr(_emit, "_emit_lock", asyncio.Lock())


def _binding(
    name: str = "things",
    resource_config: ResourceConfig | None = None,
) -> CaptureBinding[ResourceConfig]:
    return CaptureBinding(
        collection=CollectionSpec(name=f"c/{name}", key=["/id"], writeSchema={}),
        resourceConfig=resource_config or ResourceConfig(name=name),
        resourcePath=[name],
        stateKey=name,
    )


def _pages(
    *pages: list[str], stopping: Task.Stopping | None = None, stop_on_page: int | None = None
) -> Callable[..., AsyncGenerator[Any, None]]:
    """A FetchPageFn whose Nth page yields the given document IDs. With
    `stop_on_page`, it stops the capture once that page is fetched."""

    async def fetch_page(
        log: logging.Logger, page: PageCursor, cutoff: Any
    ) -> AsyncGenerator[Any, None]:
        i = page if isinstance(page, int) else 0
        for id in pages[i]:
            yield _Doc(id=id)
        if stopping and i == stop_on_page:
            stopping.event.set()
        if i + 1 < len(pages):
            yield i + 1

    return fetch_page


def _changes(stopping: Task.Stopping, *ids: str) -> Callable[..., AsyncGenerator[Any, None]]:
    """A FetchChangesFn that captures the given documents once, then stops the
    capture when the binding's backfill tasks are done. The BackfillComplete is
    sent by the last of them."""
    calls = 0

    async def fetch_changes(log: logging.Logger, cursor: Any) -> AsyncGenerator[Any, None]:
        nonlocal calls
        calls += 1
        if calls == 1:
            for id in ids:
                yield _Doc(id=id)
            yield cursor + timedelta(seconds=1)
            return

        backfills = [t for t in asyncio.all_tasks() if ".backfill" in t.get_name()]
        if backfills:
            _ = await asyncio.wait(backfills)
        stopping.event.set()

    return fetch_changes


def _resource(
    backfill_coverage: BackfillCoverage | BackfillCoverageFn[ResourceConfig] | None,
    initial_state: ResourceState,
    fetch_page: Any = None,
    fetch_changes: Any = None,
) -> Resource[Any, ResourceConfig, ResourceState]:
    async def open(binding, binding_index, state, task, all_bindings):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=fetch_page,
            fetch_changes=fetch_changes,
        )
        # Like a connector whose open awaits, this lets the binding's tasks run
        # before open() returns.
        await asyncio.sleep(0.01)

    return Resource(
        name="things",
        key=["/id"],
        model=_Doc,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name="things"),
        schema_inference=True,
        backfill_coverage=backfill_coverage,
    )


def _backfill_state(next_page: PageCursor = None) -> ResourceState:
    return ResourceState(backfill=ResourceState.Backfill(cutoff=CUTOFF, next_page=next_page))


def _awaiting_begin(state: ResourceState) -> ResourceState:
    state.is_awaiting_backfill_begin = True
    return state


def _awaiting(state: ResourceState) -> ResourceState:
    state.is_awaiting_backfill_complete = True
    return state


async def _session(
    bindings: list[Binding],
    states: dict[str, ResourceState] | None = None,
    feature_flags: str = "",
    stopping: Task.Stopping | None = None,
    backfill_requests: list[str] | None = None,
    config: BaseModel | None = None,
    output: io.BytesIO | None = None,
) -> list[dict[str, Any]]:
    """Runs a capture session and returns its output messages."""
    if config is None:
        config = _Config(advanced=_Advanced(feature_flags=feature_flags))
        config.advanced.resolve_feature_flags(None)

    open_request = request.Open[Any, ResourceConfig, ConnectorState[ResourceState]](
        capture=CaptureSpec[Any, ResourceConfig](
            name="acmeCo/capture",
            connectorType="IMAGE",
            config=config,
            intervalSeconds=0,
        ),
        version="v1",
        range=RangeSpec(),
        state=ConnectorState[ResourceState](
            bindingStateV1=dict(states or {}),
            backfillRequests={key: True for key in backfill_requests or []},
        ),
    )
    _, run = common.open(open_request, bindings)

    output = output or io.BytesIO()
    stopping = stopping or Task.Stopping()
    try:
        async with asyncio.TaskGroup() as tg:
            task = Task(
                log=logging.getLogger("test-backfill-signals"),
                connector_status=MagicMock(),
                name="capture",
                output=output,
                stopping=stopping,
                tg=tg,
                transactor=Transactor(output),
                catalog_task_name="acmeCo/capture",
            )
            await run(task)
    except ExceptionGroup as group:
        raise group.exceptions[0]

    # Scheduled bindings leave behind a timer to stop the capture later.
    for other in asyncio.all_tasks():
        if other is not asyncio.current_task():
            _ = other.cancel()

    if stopping.first_error:
        raise stopping.first_error

    return _messages(output)


def _messages(output: io.BytesIO) -> list[dict[str, Any]]:
    _ = output.seek(0)
    messages = [json.loads(line) for line in output.read().decode().splitlines()]

    # The runtime fails a session whose backfill signal shares its checkpoint
    # with documents or another signal.
    for i, message in enumerate(messages):
        if "backfillBegin" in message or "backfillComplete" in message:
            assert i == 0 or "checkpoint" in messages[i - 1], messages
            assert "checkpoint" in messages[i + 1], messages

    return messages


async def _capture(
    binding: CaptureBinding[ResourceConfig],
    resource: Resource[Any, ResourceConfig, ResourceState],
    state: ResourceState | None = None,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    states = {binding.stateKey: state} if state else None
    return await _session([(binding, resource)], states, **kwargs)


def _next_states(
    states: dict[str, ResourceState], messages: list[dict[str, Any]]
) -> dict[str, ResourceState]:
    """The binding states the runtime hands the next session, after reducing
    this session's checkpoints into `states`."""
    doc: dict[str, Any] = {
        "bindingStateV1": {
            key: state.model_dump(mode="json", by_alias=True, exclude_unset=True)
            for key, state in states.items()
        }
    }
    for message in messages:
        if "checkpoint" in message:
            json_merge_patch(doc, message["checkpoint"]["state"]["updated"])
    reduced = ConnectorState[ResourceState].model_validate(doc).bindingStateV1
    return {key: state for key, state in reduced.items() if state is not None}


def _timeline(messages: list[dict[str, Any]], binding: int | None = None) -> list[str]:
    """Signals and captured document IDs in order, optionally of one binding."""
    timeline: list[str] = []
    for message in messages:
        for kind in ("captured", "backfillBegin", "backfillComplete"):
            if kind not in message:
                continue
            if binding is not None and message[kind].get("binding", 0) != binding:
                continue
            if kind == "captured":
                timeline.append(message[kind]["doc"]["id"])
            else:
                timeline.append("begin" if kind == "backfillBegin" else "complete")
    return timeline


def _binding_checkpoints(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        m["checkpoint"]["state"]["updated"]["bindingStateV1"]["things"]
        for m in messages
        if "checkpoint" in m and "things" in m["checkpoint"]["state"]["updated"].get("bindingStateV1", {})
    ]


def _initialized(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """The binding's initialized state, which is the checkpoint carrying its backfill."""
    return next(c for c in _binding_checkpoints(messages) if c and "backfill" in c)


def _state_after(messages: list[dict[str, Any]], signal: str) -> dict[str, Any]:
    """The binding state in the checkpoint that follows a backfill signal."""
    i = next(i for i, m in enumerate(messages) if signal in m)
    return messages[i + 1]["checkpoint"]["state"]["updated"]["bindingStateV1"]["things"]


def _json_time(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


@pytest.mark.asyncio
async def test_signals_bracket_a_backfill_with_full_coverage():
    stopping = Task.Stopping()
    messages = await _capture(
        _binding(),
        _resource(
            BackfillCoverage.FULL,
            ResourceState(
                inc=ResourceState.Incremental(cursor=CUTOFF),
                backfill=ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
            ),
            fetch_page=_pages(["a", "b"], ["c"]),
            fetch_changes=_changes(stopping, "inc"),
        ),
        stopping=stopping,
    )

    timeline = _timeline(messages)
    # Begin precedes every document, including the incremental task's. Complete
    # follows the last page.
    assert timeline[0] == "begin"
    assert sorted(timeline[1:-1]) == ["a", "b", "c", "inc"]
    assert timeline[-1] == "complete"

    # The initialized state commits before the BackfillBegin, which then moves the
    # binding from owing its BackfillBegin to owing its BackfillComplete.
    assert _initialized(messages)["is_awaiting_backfill_begin"] is True
    assert _state_after(messages, "backfillBegin") == {
        "is_awaiting_backfill_begin": None,
        "is_awaiting_backfill_complete": True,
    }
    assert _state_after(messages, "backfillComplete") == {"is_awaiting_backfill_complete": None}


@pytest.mark.asyncio
async def test_binding_waits_for_a_slow_backfill_begin(monkeypatch: pytest.MonkeyPatch):
    backfill_begin = Task._backfill_begin  # pyright: ignore[reportPrivateUsage]

    async def slow_backfill_begin(self: Task, binding: int, state: ConnectorState) -> None:
        await asyncio.sleep(0.05)
        await backfill_begin(self, binding, state)

    monkeypatch.setattr(Task, "_backfill_begin", slow_backfill_begin)
    stopping = Task.Stopping()
    messages = await _capture(
        _binding(),
        _resource(
            BackfillCoverage.FULL,
            ResourceState(
                inc=ResourceState.Incremental(cursor=CUTOFF),
                backfill=ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
            ),
            fetch_page=_pages(["a"]),
            fetch_changes=_changes(stopping, "inc"),
        ),
        stopping=stopping,
    )
    assert _timeline(messages)[0] == "begin"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "coverage, feature_flags, signals",
    [
        (BackfillCoverage.FULL, "", True),
        (BackfillCoverage.FULL, "no_emit_backfill_signals", False),
        (BackfillCoverage.FULL, "no_emit_backfill_signals,ignore_config_for_backfill_signals", False),
        (BackfillCoverage.BOUNDED_BY_CONFIG, "", False),
        (BackfillCoverage.BOUNDED_BY_CONFIG, "ignore_config_for_backfill_signals", True),
        (BackfillCoverage.PARTIAL, "ignore_config_for_backfill_signals", False),
        (None, "", False),
    ],
)
async def test_signals_only_for_qualifying_backfills(
    coverage: BackfillCoverage | None, feature_flags: str, signals: bool
):
    messages = await _capture(
        _binding(),
        _resource(coverage, _backfill_state(), fetch_page=_pages(["a"])),
        feature_flags=feature_flags,
    )
    assert _timeline(messages) == (["begin", "a", "complete"] if signals else ["a"])


@pytest.mark.asyncio
async def test_resource_without_a_backfill_never_signals():
    stopping = Task.Stopping()
    messages = await _capture(
        _binding(),
        _resource(
            BackfillCoverage.FULL,
            ResourceState(inc=ResourceState.Incremental(cursor=CUTOFF)),
            fetch_changes=_changes(stopping, "inc"),
        ),
        stopping=stopping,
    )
    assert _timeline(messages) == ["inc"]


@pytest.mark.asyncio
async def test_backfill_interrupted_mid_pass_completes_in_the_next_session():
    stopping = Task.Stopping()
    resource = _resource(
        BackfillCoverage.FULL,
        _backfill_state(),
        fetch_page=_pages(["a"], ["b"], ["c"], stopping=stopping, stop_on_page=0),
    )
    first = await _capture(_binding(), resource, stopping=stopping)
    assert _timeline(first) == ["begin", "a"]

    second = await _session([(_binding(), resource)], _next_states({}, first))
    assert _timeline(second) == ["b", "c", "complete"]


@pytest.mark.asyncio
async def test_owed_begin_is_sent_in_the_next_session():
    # The initialized state committed, but the session stopped before its
    # BackfillBegin.
    messages = await _capture(
        _binding(),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages(["a"])),
        state=_awaiting_begin(_backfill_state()),
    )
    assert _timeline(messages) == ["begin", "a", "complete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("owed", [_awaiting_begin, _awaiting], ids=["begin", "complete"])
async def test_backfill_that_stops_qualifying_drops_its_owed_signals(owed: Callable[[ResourceState], ResourceState]):
    messages = await _capture(
        _binding(),
        _resource(BackfillCoverage.PARTIAL, _backfill_state(), fetch_page=_pages(["a"], ["b"])),
        state=owed(_backfill_state()),
    )
    assert _timeline(messages) == ["a", "b"]
    assert {"is_awaiting_backfill_begin": None, "is_awaiting_backfill_complete": None} in _binding_checkpoints(messages)


@pytest.mark.asyncio
async def test_resumed_scheduled_backfill_is_judged_as_connector_initiated():
    seen: list[bool] = []

    def coverage(ctx: BackfillContext[ResourceConfig]) -> BackfillCoverage:
        seen.append(ctx.is_connector_initiated)
        return BackfillCoverage.PARTIAL if ctx.is_connector_initiated else BackfillCoverage.FULL

    state = _awaiting(_backfill_state(next_page=1))
    state.is_connector_initiated = True

    messages = await _capture(
        _binding(),
        _resource(coverage, _backfill_state(), fetch_page=_pages(["a"], ["b"])),
        state=state,
    )
    assert seen == [True]
    assert _timeline(messages) == ["b"]


@pytest.mark.asyncio
async def test_triggered_backfill_begins_again():
    # A Triggers.BACKFILL restarts the binding from its initial state, even
    # while a BackfillComplete is owed.
    messages = await _capture(
        _binding(),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages(["a"])),
        state=_awaiting(_backfill_state(next_page=7)),
        backfill_requests=["things"],
    )
    assert _timeline(messages) == ["begin", "a", "complete"]


@pytest.mark.asyncio
async def test_subtasks_send_one_complete_after_the_last():
    initial = ResourceState(
        backfill={
            "x": ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
            "y": ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
        }
    )
    messages = await _capture(
        _binding(),
        _resource(
            BackfillCoverage.FULL,
            initial,
            fetch_page={"x": _pages(["x1"], ["x2"]), "y": _pages(["y1"])},
        ),
    )
    timeline = _timeline(messages)
    assert timeline[0] == "begin" and timeline[-1] == "complete"
    assert sorted(timeline[1:-1]) == ["x1", "x2", "y1"]


@pytest.mark.asyncio
async def test_subtask_that_finishes_in_a_later_session_sends_the_complete():
    stopping = Task.Stopping()
    initial = ResourceState(
        backfill={
            "x": ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
            "y": ResourceState.Backfill(cutoff=CUTOFF, next_page=None),
        }
    )
    resource = _resource(
        BackfillCoverage.FULL,
        initial,
        fetch_page={
            "x": _pages(["x1"]),
            "y": _pages(["y1"], ["y2"], stopping=stopping, stop_on_page=0),
        },
    )
    first = await _capture(_binding(), resource, stopping=stopping)
    assert _timeline(first) == ["begin", "x1", "y1"]

    second = await _session([(_binding(), resource)], _next_states({}, first))
    assert _timeline(second) == ["y2", "complete"]


@pytest.mark.asyncio
async def test_subtask_of_a_removed_parent_does_not_hold_up_complete():
    # "gone" has no fetcher any more, and "x" finished in an earlier session.
    state = _awaiting(
        ResourceState(backfill={"gone": ResourceState.Backfill(cutoff=CUTOFF, next_page=3)})
    )
    messages = await _capture(
        _binding(),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page={"x": _pages(["x1"])}),
        state=state,
    )
    assert _timeline(messages) == ["complete"]


def _scheduled_state(**inc_cursors: Any) -> ResourceState:
    return ResourceState(
        inc={key: ResourceState.Incremental(cursor=cursor) for key, cursor in inc_cursors.items()},
        last_initialized=datetime.now(tz=UTC) - timedelta(days=3),
    )


_SCHEDULED = ResourceConfigWithSchedule(name="things", schedule="0 0 * * *")


def _resetting_state() -> ResourceState:
    """An initial state whose dict backfill a scheduled backfill can't coordinate with
    the incremental cursors, so the binding goes back to it."""
    return ResourceState(
        inc={"only": ResourceState.Incremental(cursor=CUTOFF)},
        backfill={"x": ResourceState.Backfill(cutoff=CUTOFF, next_page=None)},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["single", "subtasks"])
async def test_signaling_scheduled_backfill_rewinds_every_incremental_cursor(shape: str):
    if shape == "single":
        state = ResourceState(
            inc=ResourceState.Incremental(cursor=CUTOFF),
            last_initialized=datetime.now(tz=UTC) - timedelta(days=3),
        )
    else:
        state = _scheduled_state(realtime=CUTOFF + timedelta(hours=12), lookback=CUTOFF)

    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages([])),
        state=state,
    )
    assert _timeline(messages) == ["begin", "complete"]
    # The cutoff is the slowest cursor, and every cursor moves back to just before
    # it, so the incremental tasks recapture what they captured at or after it.
    initialized = _initialized(messages)
    assert initialized["backfill"]["cutoff"] == _json_time(CUTOFF)
    inc = initialized["inc"]
    cursors = [inc["cursor"]] if "cursor" in inc else [i["cursor"] for i in inc.values()]
    assert cursors and set(cursors) == {_json_time(CUTOFF - common.BACKFILL_SIGNAL_CURSOR_REWIND)}


@pytest.mark.asyncio
async def test_scheduled_backfill_that_doesnt_signal_keeps_its_cursors():
    # Answers like a resource whose scheduled backfills refresh only some fields.
    def coverage(ctx: BackfillContext[ResourceConfig]) -> BackfillCoverage:
        return BackfillCoverage.PARTIAL if ctx.is_connector_initiated else BackfillCoverage.FULL

    realtime = CUTOFF + timedelta(hours=12)
    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(coverage, _backfill_state(), fetch_page=_pages([])),
        state=_scheduled_state(realtime=realtime, lookback=CUTOFF),
    )
    assert _timeline(messages) == []
    initialized = _initialized(messages)
    assert {k: v["cursor"] for k, v in initialized["inc"].items()} == {
        "realtime": _json_time(realtime),
        "lookback": _json_time(CUTOFF),
    }


@pytest.mark.asyncio
async def test_scheduled_backfill_that_resets_its_state_signals():
    # A dict backfill can't be coordinated with the incremental cursors, so the
    # binding goes back to its initial state and begins afresh.
    seen: list[bool] = []

    def coverage(ctx: BackfillContext[ResourceConfig]) -> BackfillCoverage:
        seen.append(ctx.is_connector_initiated)
        return BackfillCoverage.FULL

    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(coverage, _resetting_state(), fetch_page={"x": _pages(["x1"])}),
        state=_scheduled_state(only=CUTOFF + timedelta(days=2)),
    )
    assert seen == [True]
    assert _timeline(messages) == ["begin", "x1", "complete"]
    assert _initialized(messages)["inc"]["only"]["cursor"] == _json_time(CUTOFF)


@pytest.mark.asyncio
async def test_owed_complete_is_sent_before_a_missed_schedule_begins_again():
    state = _awaiting(_scheduled_state(only=CUTOFF))
    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(BackfillCoverage.FULL, _resetting_state(), fetch_page={"x": _pages(["a"])}),
        state=state,
    )
    assert _timeline(messages) == ["complete", "begin", "a", "complete"]
    assert not _next_states({"things": state}, messages)["things"].is_awaiting_backfill_complete


@pytest.mark.asyncio
async def test_no_scheduled_backfill_starts_while_one_runs():
    # Restarting the pass would move the BackfillBegin's boundary past the
    # documents the running backfill already captured again.
    state = _awaiting(_scheduled_state(only=CUTOFF))
    state.backfill = ResourceState.Backfill(cutoff=CUTOFF, next_page=1)
    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages(["a"], ["b"])),
        state=state,
    )
    assert _timeline(messages) == ["b", "complete"]


@pytest.mark.asyncio
async def test_backfill_that_doesnt_signal_clears_a_stale_owed_complete():
    # A requested backfill on a scheduled binding coordinates with its earlier
    # state. That state still owes a BackfillComplete that this unsignaled backfill
    # must not send.
    state = _awaiting(_scheduled_state(only=CUTOFF))
    messages = await _capture(
        _binding(resource_config=_SCHEDULED),
        _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages(["a"])),
        state=state,
        backfill_requests=["things"],
        feature_flags="no_emit_backfill_signals",
    )
    assert _timeline(messages) == ["a"]
    assert _initialized(messages)["is_awaiting_backfill_complete"] is None


@pytest.mark.asyncio
async def test_signals_carry_their_binding():
    # One resource serves both bindings and answers by binding.
    def coverage(ctx: BackfillContext[ResourceConfig]) -> BackfillCoverage:
        is_signaling = ctx.binding.stateKey == "signaling"
        return BackfillCoverage.FULL if is_signaling else BackfillCoverage.PARTIAL

    resource = _resource(coverage, _backfill_state(), fetch_page=_pages(["a"]))
    messages = await _session([(_binding("plain"), resource), (_binding("signaling"), resource)])
    assert _timeline(messages, binding=0) == ["a"]
    assert _timeline(messages, binding=1) == ["begin", "a", "complete"]


class _NoFlags(FeatureFlagsConfig):
    FEATURE_FLAG_DEFAULTS = {}


class _NoFlagsConfig(BaseModel):
    advanced: _NoFlags = Field(default_factory=_NoFlags)


class _PlainAdvanced(BaseModel):
    feature_flags: str = ""


class _PlainConfig(BaseModel):
    advanced: _PlainAdvanced = Field(default_factory=_PlainAdvanced)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config, match",
    [
        (_NoFlagsConfig(), "backfill_signal_flag_defaults"),
        (_PlainConfig(), "isn't a FeatureFlagsConfig"),
    ],
)
async def test_signaling_resource_needs_the_flags_before_anything_is_sent(config: BaseModel, match: str):
    # Even the reset of a requested backfill waits for the flags.
    output = io.BytesIO()
    with pytest.raises(RuntimeError, match=match):
        await _capture(
            _binding(),
            _resource(BackfillCoverage.FULL, _backfill_state(), fetch_page=_pages(["a"])),
            state=_backfill_state(next_page=1),
            backfill_requests=["things"],
            config=config,
            output=output,
        )
    assert _messages(output) == []


@pytest.mark.asyncio
async def test_resources_that_dont_opt_in_need_no_flags():
    messages = await _capture(
        _binding(),
        _resource(None, _backfill_state(), fetch_page=_pages(["a"])),
        config=_PlainConfig(),
    )
    assert _timeline(messages) == ["a"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_state, fetch_page",
    [
        # The resource doesn't pass a fetch_page.
        (_backfill_state(), None),
        # No fetcher matches the backfill's subtasks.
        (
            ResourceState(backfill={"x": ResourceState.Backfill(cutoff=CUTOFF, next_page=None)}),
            {"y": _pages(["y1"])},
        ),
    ],
)
async def test_backfill_that_cant_run_fails_before_its_begin(initial_state: ResourceState, fetch_page: Any):
    output = io.BytesIO()
    with pytest.raises(RuntimeError, match="no fetch_page to run it"):
        await _capture(
            _binding(),
            _resource(BackfillCoverage.FULL, initial_state, fetch_page=fetch_page),
            output=output,
        )
    assert "begin" not in _timeline(_messages(output))


def test_signals_default_on_only_for_tasks_created_on_or_after_the_date():
    class Advanced(FeatureFlagsConfig):
        FEATURE_FLAG_DEFAULTS = backfill_signal_flag_defaults(
            enabled_for_tasks_created_after=date(2026, 11, 4)
        )

    off = {"emit_backfill_signals": False, "ignore_config_for_backfill_signals": False}
    assert Advanced().resolve_feature_flags(date(2026, 11, 3)) == off
    assert Advanced().resolve_feature_flags(date(2026, 11, 4)) == {**off, "emit_backfill_signals": True}


def test_snapshot_resource_never_signals():
    with pytest.raises(AssertionError, match="never sends backfill signals"):
        _ = SnapshotResource(
            name="things",
            open=lambda *args: None,
            initial_config=ResourceConfig(name="things"),
            backfill_coverage=BackfillCoverage.FULL,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("send_signal", ["_backfill_begin", "_backfill_complete"])
async def test_signal_refuses_to_share_a_checkpoint(send_signal: str):
    output = io.BytesIO()
    task = Task(
        log=logging.getLogger("test-backfill-signals"),
        connector_status=MagicMock(),
        name="capture",
        output=output,
        stopping=Task.Stopping(),
        tg=MagicMock(),
        transactor=Transactor(output),
        catalog_task_name="acmeCo/capture",
    )
    task.captured(0, _Doc(id="a"))
    with pytest.raises(RuntimeError, match="stand alone"):
        await getattr(task, send_signal)(0, ConnectorState())
