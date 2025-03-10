import abc
import asyncio
import functools
from enum import Enum
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Literal,
    TypeVar,
    Tuple,
)
from pydantic import AwareDatetime, BaseModel, Field, NonNegativeInt

from ..cron import next_fire
from ..flow import (
    AccessToken,
    BaseOAuth2Credentials,
    CaptureBinding,
    ClientCredentialsOAuth2Credentials,
    ClientCredentialsOAuth2Spec,
    AuthorizationCodeFlowOAuth2Credentials,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    ValidationError,
    BasicAuth,
)
from ..pydantic_polyfill import GenericModel
from . import Task, request, response

LogCursor = Tuple[str | int] | AwareDatetime | NonNegativeInt
"""LogCursor is a cursor into a logical log of changes.
The two predominant strategies for accessing logs are:
 a) fetching entities which were created / updated / deleted since a given datetime.
 b) fetching changes by their offset in a sequential log (Kafka partition or Gazette journal).

Note that `str` cannot be added to this type union, as it makes parsing states ambiguous, however
the tuple type allows for str or integer values to be used
"""

PageCursor = str | int | None
"""PageCursor is a cursor into a paged result set.
These cursors are predominantly an opaque string or an internal offset integer.

None means "begin a new iteration" in a request context,
and "no pages remain" in a response context.
"""


class Triggers(Enum):
    BACKFILL = "BACKFILL"


class BaseDocument(BaseModel):
    class Meta(BaseModel):
        op: Literal["c", "u", "d"] = Field(
            default="u",
            description="Operation type (c: Create, u: Update, d: Delete)",
        )
        row_id: int = Field(
            default=-1,
            description="Row ID of the Document, counting up from zero, or -1 if not known",
        )

    meta_: Meta = Field(
        default=Meta(op="u"), alias="_meta", description="Document metadata"
    )


_BaseDocument = TypeVar("_BaseDocument", bound=BaseDocument)


class BaseResourceConfig(abc.ABC, BaseModel, extra="forbid"):
    """
    AbstractResourceConfig is a base class for ResourceConfig classes.
    """

    PATH_POINTERS: ClassVar[list[str]]

    @abc.abstractmethod
    def path(self) -> list[str]:
        raise NotImplementedError()


_BaseResourceConfig = TypeVar("_BaseResourceConfig", bound=BaseResourceConfig)


class ResourceConfig(BaseResourceConfig):
    """ResourceConfig is a common resource configuration shape."""

    PATH_POINTERS: ClassVar[list[str]] = ["/name"]

    name: str = Field(description="Name of this resource")
    interval: timedelta = Field(
        default=timedelta(), description="Interval between updates for this resource"
    )

    # NOTE(johnny): If we need a namespace, introduce an ExtResourceConfig (?)
    # which adds a `namespace` field like:
    # namespace: str | None = Field(
    #    default=None, description="Enclosing schema namespace of this resource"
    # )

    def path(self) -> list[str]:
        return [self.name]


_ResourceConfig = TypeVar("_ResourceConfig", bound=ResourceConfig)


CRON_REGEX = (
    r"^"
    r"((?:[0-5]?\d(?:-[0-5]?\d)?|\*(?:/[0-5]?\d)?)(?:,(?:[0-5]?\d(?:-[0-5]?\d)?|\*(?:/[0-5]?\d)?))*)\s+"  # minute
    r"((?:[01]?\d|2[0-3]|(?:[01]?\d|2[0-3])-(?:[01]?\d|2[0-3])|\*(?:/[01]?\d|/2[0-3])?)(?:,(?:[01]?\d|2[0-3]|(?:[01]?\d|2[0-3])-(?:[01]?\d|2[0-3])|\*(?:/[01]?\d|/2[0-3])?))*)\s+"  # hour
    r"((?:0?[1-9]|[12]\d|3[01]|(?:0?[1-9]|[12]\d|3[01])-(?:0?[1-9]|[12]\d|3[01])|\*(?:/[0-9]|/1[0-9]|/2[0-9]|/3[01])?)(?:,(?:0?[1-9]|[12]\d|3[01]|(?:0?[1-9]|[12]\d|3[01])-(?:0?[1-9]|[12]\d|3[01])|\*(?:/[0-9]|/1[0-9]|/2[0-9]|/3[01])?))*)\s+"  # day of month
    r"((?:[1-9]|1[0-2]|(?:[1-9]|1[0-2])-(?:[1-9]|1[0-2])|\*(?:/[1-9]|/1[0-2])?)(?:,(?:[1-9]|1[0-2]|(?:[1-9]|1[0-2])-(?:[1-9]|1[0-2])|\*(?:/[1-9]|/1[0-2])?))*)\s+"  # month
    r"((?:[0-6]|(?:[0-6])-(?:[0-6])|\*(?:/[0-6])?)(?:,(?:[0-6]|(?:[0-6])-(?:[0-6])|\*(?:/[0-6])?))*)"  # day of week
    r"$|^$"  # Empty string to signify no schedule
)


class ResourceConfigWithSchedule(ResourceConfig):
    schedule: str = Field(
        default="",
        title="Schedule",
        description="Schedule to automatically rebackfill this binding. Accepts a cron expression.",
        pattern=CRON_REGEX,
    )


class BaseResourceState(abc.ABC, BaseModel, extra="forbid"):
    """
    AbstractResourceState is a base class for ResourceState classes.
    """

    pass


_BaseResourceState = TypeVar("_BaseResourceState", bound=BaseResourceState)


class ResourceState(BaseResourceState, BaseModel, extra="forbid"):
    """ResourceState composes separate incremental, backfill, and snapshot states.
    Inner states can be updated independently, so long as sibling states are left unset.
    The Flow runtime will merge-patch partial checkpoint states into an aggregated state.
    """

    class Incremental(BaseModel, extra="forbid"):
        """Partial state of a resource which is being incrementally captured"""

        cursor: LogCursor = Field(
            description="Cursor of the last-synced document in the logical log"
        )

    class Backfill(BaseModel, extra="forbid"):
        """Partial state of a resource which is being backfilled"""

        cutoff: LogCursor = Field(
            description="LogCursor at which incremental replication began"
        )
        next_page: PageCursor = Field(
            description="PageCursor of the next page to fetch", default=None
        )

    class Snapshot(BaseModel, extra="forbid"):
        """Partial state of a resource for which periodic snapshots are taken"""

        updated_at: AwareDatetime = Field(description="Time of the last snapshot")
        last_count: int = Field(
            description="Number of documents captured from this resource by the last snapshot"
        )
        last_digest: str = Field(
            description="The xxh3_128 hex digest of documents of this resource in the last snapshot"
        )

    inc: Incremental | dict[str, Incremental | None] | None = Field(
        default=None, description="Incremental capture progress"
    )

    backfill: Backfill | dict[str, Backfill | None] | None = Field(
        default=None,
        description="Backfill progress, or None if no backfill is occurring",
    )

    snapshot: Snapshot | None = Field(default=None, description="Snapshot progress")

    last_initialized: datetime | None = Field(
        default=None, description="The last time this state was initialized."
    )


_ResourceState = TypeVar("_ResourceState", bound=ResourceState)


class ConnectorState(GenericModel, Generic[_BaseResourceState], extra="forbid"):
    """ConnectorState represents a number of ResourceStates, keyed by binding state key."""

    bindingStateV1: dict[str, _BaseResourceState | None] = {}
    backfillRequests: dict[str, bool | None] = {}


_ConnectorState = TypeVar("_ConnectorState", bound=ConnectorState)


@dataclass
class AssociatedDocument(Generic[_BaseDocument]):
    """
    Emitting AssociatedDocument allows you to represent capturing document for other bindings.
    You might use this if your data model requires you to load "child" documents when capturing a "parent" document,
    instead of independently loading the child data stream.
    """

    doc: _BaseDocument
    binding: int


FetchSnapshotFn = Callable[[Logger], AsyncGenerator[_BaseDocument | dict, None]]
"""
FetchSnapshotFn is a function which fetches a complete snapshot of a resource.

Snapshot resources are typically "small" -- they fit easily on disk -- and are
gathered in a single shot. Its content is digested to determine if its
changed since the last snapshot. If it hasn't, the snapshot is discarded and
not emitted by the connector.
"""

FetchPageFn = Callable[
    [Logger, PageCursor, LogCursor],
    AsyncGenerator[_BaseDocument | dict | AssociatedDocument | PageCursor, None],
]
"""
FetchPageFn fetches available checkpoints since the provided last PageCursor.
It will typically fetch just one page, though it may fetch multiple pages.

The argument PageCursor is None if a new iteration is being started.
Otherwise it is the last PageCursor yielded by FetchPageFn.

The argument LogCursor is the "cutoff" log position at which incremental
replication started, and should be used to suppress documents which were
modified at-or-after the cutoff, as such documents are
already observed through incremental replication.

Checkpoints consist of a yielded sequence of documents followed by a
non-None PageCursor, which checkpoints those preceding documents,
or by simply returning if the iteration is complete.

It's an error if FetchPageFn yields a PageCursor of None.
Instead, mark the end of the sequence by yielding documents and then
returning without yielding a final PageCursor.
"""

FetchChangesFn = Callable[
    [Logger, LogCursor],
    AsyncGenerator[_BaseDocument | dict | AssociatedDocument | LogCursor, None],
]
"""
FetchChangesFn fetches available checkpoints since the provided last LogCursor.

Checkpoints consist of a yielded sequence of documents followed by a LogCursor,
where the LogCursor checkpoints those preceding documents.

Yielded LogCursors MUST be strictly increasing relative to the argument
LogCursor and also to previously yielded LogCursors.

It's an error if FetchChangesFn yields documents, and then returns without
yielding a final LogCursor. NOTE(johnny): if needed, we could extend the
contract to allow an explicit "roll back" sentinel.

FetchChangesFn yields until no further checkpoints are readily available,
and then returns. If no checkpoints are available at all,
it yields nothing and returns.

Implementations may block for brief periods to await checkpoints, such as while
awaiting a server response, but MUST NOT block forever as it prevents the
connector from exiting.

Implementations MAY return early, such as if it's convenient to fetch only
a next page of recent changes. If an implementation yields any checkpoints,
then it is immediately re-invoked.

Otherwise if it returns without yielding a checkpoint, then
`ResourceConfig.interval` is respected between invocations.
Implementations SHOULD NOT sleep or implement their own coarse rate limit
(use `ResourceConfig.interval`).
"""


@dataclass
class Resource(Generic[_BaseDocument, _BaseResourceConfig, _BaseResourceState]):
    """Resource is a high-level description of an available capture resource,
    encapsulating metadata for catalog discovery as well as a capability
    to open() the resource for capture."""

    @dataclass
    class FixedSchema:
        """
        FixedSchema encapsulates a prior JSON schema which should be used
        as the model schema, rather than dynamically generating a schema.
        """

        value: dict

    name: str
    key: list[str]
    model: type[_BaseDocument] | FixedSchema
    open: Callable[
        [
            CaptureBinding[_BaseResourceConfig],
            int,
            ResourceState,
            Task,
            list[
                tuple[
                    CaptureBinding[_ResourceConfig],
                    "Resource[_BaseDocument, _ResourceConfig, _ResourceState]",
                ]
            ],
        ],
        None,
    ]
    initial_state: _BaseResourceState
    initial_config: _BaseResourceConfig
    schema_inference: bool


def discovered(
    resources: list["Resource[_BaseDocument, _BaseResourceConfig, _BaseResourceState]"],
) -> response.Discovered[_BaseResourceConfig]:
    bindings: list[response.DiscoveredBinding] = []

    for resource in resources:
        if isinstance(resource.model, Resource.FixedSchema):
            schema = resource.model.value
        else:
            schema = resource.model.model_json_schema(mode="serialization")

        if resource.schema_inference:
            schema["x-infer-schema"] = True

        bindings.append(
            response.DiscoveredBinding(
                documentSchema=schema,
                key=resource.key,
                recommendedName=resource.name,
                resourceConfig=resource.initial_config,
            )
        )

    return response.Discovered(bindings=bindings)


_ResolvableBinding = TypeVar(
    "_ResolvableBinding", bound=CaptureBinding | request.ValidateBinding
)
"""_ResolvableBinding is either a CaptureBinding or a request.ValidateBinding"""


def resolve_bindings(
    bindings: list[_ResolvableBinding],
    resources: list[Resource[Any, _BaseResourceConfig, Any]],
    resource_term="Resource",
) -> list[tuple[_ResolvableBinding, Resource[Any, _BaseResourceConfig, Any]]]:
    resolved: list[
        tuple[_ResolvableBinding, Resource[Any, _BaseResourceConfig, Any]]
    ] = []
    errors: list[str] = []

    for binding in bindings:
        path = binding.resourceConfig.path()

        # Find a resource which matches this binding.
        found = False
        for resource in resources:
            if path == resource.initial_config.path():
                resolved.append((binding, resource))
                found = True
                break

        if not found:
            errors.append(f"{resource_term} '{'.'.join(path)}' was not found.")

    if errors:
        raise ValidationError(errors)

    return resolved


def validated(
    resolved_bindings: list[
        tuple[
            request.ValidateBinding[_BaseResourceConfig],
            Resource[Any, _BaseResourceConfig, Any],
        ]
    ],
) -> response.Validated:
    return response.Validated(
        bindings=[
            response.ValidatedBinding(resourcePath=b[0].resourceConfig.path())
            for b in resolved_bindings
        ],
    )


def open(
    open: request.Open[Any, _ResourceConfig, _ConnectorState],
    resolved_bindings: list[
        tuple[
            CaptureBinding[_ResourceConfig],
            Resource[_BaseDocument, _ResourceConfig, _ResourceState],
        ]
    ],
) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
    async def _run(task: Task):
        backfill_requests = []
        if open.state.backfillRequests is not None:
            for stateKey in open.state.backfillRequests.keys():
                task.log.info(
                    "clearing checkpoint due to backfill trigger",
                    {"stateKey": stateKey},
                )
                backfill_requests.append(stateKey)
                task.checkpoint(
                    ConnectorState(
                        bindingStateV1={stateKey: None},
                        backfillRequests={stateKey: None},
                    )
                )

        soonest_future_scheduled_initialization: datetime | None = None

        for index, (binding, resource) in enumerate(resolved_bindings):
            state: _ResourceState | None = open.state.bindingStateV1.get(
                binding.stateKey
            )

            should_initialize = state is None or binding.stateKey in backfill_requests

            if state:
                if state.last_initialized is None:
                    state.last_initialized = datetime.now(tz=UTC)
                    task.checkpoint(
                        ConnectorState(bindingStateV1={binding.stateKey: state})
                    )

                if isinstance(binding.resourceConfig, ResourceConfigWithSchedule):
                    cron_schedule = binding.resourceConfig.schedule
                    next_scheduled_initialization = next_fire(
                        cron_schedule, state.last_initialized
                    )

                    if (
                        next_scheduled_initialization
                        and next_scheduled_initialization < datetime.now(tz=UTC)
                    ):
                        # Re-initialize the binding if we missed a scheduled re-initialization.
                        should_initialize = True
                        if state.backfill:
                            task.log.warning(
                                f"Scheduled backfill for binding {resource.name} is taking precedence over its ongoing backfill."
                                " Please extend the binding's configured cron schedule if you'd like the previous backfill to"
                                " complete before the next scheduled backfill starts."
                            )

                        next_scheduled_initialization = next_fire(
                            cron_schedule, datetime.now(tz=UTC)
                        )

                    if (
                        next_scheduled_initialization
                        and soonest_future_scheduled_initialization
                    ):
                        soonest_future_scheduled_initialization = min(
                            soonest_future_scheduled_initialization,
                            next_scheduled_initialization,
                        )
                    elif next_scheduled_initialization:
                        soonest_future_scheduled_initialization = (
                            next_scheduled_initialization
                        )

            if should_initialize:
                # Checkpoint the binding's initialized state prior to any processing.
                state = resource.initial_state
                state.last_initialized = datetime.now(tz=UTC)

                task.checkpoint(
                    ConnectorState(
                        bindingStateV1={binding.stateKey: state},
                    )
                )

            resource.open(
                binding,
                index,
                state,
                task,
                resolved_bindings,
            )

        async def scheduled_stop(future_dt: datetime | None) -> None:
            if not future_dt:
                return None

            sleep_duration = future_dt - datetime.now(tz=UTC)
            await asyncio.sleep(sleep_duration.total_seconds())
            task.stopping.event.set()

        # Gracefully exit to ensure relatively close adherence to any bindings'
        # re-initialization schedules.
        asyncio.create_task(scheduled_stop(soonest_future_scheduled_initialization))

    return (response.Opened(explicitAcknowledgements=False), _run)


def open_binding(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    state: _ResourceState,
    task: Task,
    fetch_changes: FetchChangesFn[_BaseDocument]
    | dict[str, FetchChangesFn[_BaseDocument]]
    | None = None,
    fetch_page: FetchPageFn[_BaseDocument]
    | dict[str, FetchPageFn[_BaseDocument]]
    | None = None,
    fetch_snapshot: FetchSnapshotFn[_BaseDocument] | None = None,
    tombstone: _BaseDocument | None = None,
):
    """
    open_binding() is intended to be called by closures set as Resource.open Callables.

    It does 'heavy lifting' to actually capture a binding.

    When fetch_changes, fetch_page, or fetch_snapshot are provided as dictionaries,
    each function will be run as a separate subtask with its own independent state.
    The dictionary keys are used as subtask IDs and are used to store and retrieve
    the state for each subtask in state.inc, state.backfill, or state.snapshot.
    """

    prefix = ".".join(binding.resourceConfig.path())

    if fetch_changes:

        async def incremental_closure(
            task: Task,
            fetch_changes: FetchChangesFn[_BaseDocument],
            state: ResourceState.Incremental,
        ):
            assert state and not isinstance(state, dict)
            await _binding_incremental_task(
                binding,
                binding_index,
                fetch_changes,
                state,
                task,
            )

        if isinstance(fetch_changes, dict):
            assert state.inc and isinstance(state.inc, dict)
            for subtask_id, subtask_fetch_changes in fetch_changes.items():
                inc_state = state.inc.get(subtask_id)
                assert inc_state

                task.spawn_child(
                    f"{prefix}.incremental.{subtask_id}",
                    functools.partial(
                        incremental_closure,
                        fetch_changes=subtask_fetch_changes,
                        state=inc_state,
                    ),
                )
        else:
            assert state.inc and not isinstance(state.inc, dict)
            task.spawn_child(
                f"{prefix}.incremental",
                functools.partial(
                    incremental_closure,
                    fetch_changes=fetch_changes,
                    state=state.inc,
                ),
            )

    if fetch_page and state.backfill:

        async def backfill_closure(
            task: Task,
            fetch_page: FetchPageFn[_BaseDocument],
            state: ResourceState.Backfill,
        ):
            assert state and not isinstance(state, dict)
            await _binding_backfill_task(
                binding,
                binding_index,
                fetch_page,
                state,
                task,
            )

        if isinstance(fetch_page, dict):
            assert state.backfill and isinstance(state.backfill, dict)
            for subtask_id, subtask_fetch_page in fetch_page.items():
                backfill_state = state.backfill.get(subtask_id)
                assert backfill_state

                task.spawn_child(
                    f"{prefix}.backfill.{subtask_id}",
                    functools.partial(
                        backfill_closure,
                        fetch_page=subtask_fetch_page,
                        state=backfill_state,
                    ),
                )

        else:
            assert state.backfill and not isinstance(state.backfill, dict)
            task.spawn_child(
                f"{prefix}.backfill",
                functools.partial(
                    backfill_closure,
                    fetch_page=fetch_page,
                    state=state.backfill,
                ),
            )

    if fetch_snapshot:

        async def closure(task: Task):
            assert tombstone
            await _binding_snapshot_task(
                binding,
                binding_index,
                fetch_snapshot,
                state.snapshot,
                task,
                tombstone,
            )

        task.spawn_child(f"{prefix}.snapshot", closure)


async def _binding_snapshot_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_snapshot: FetchSnapshotFn[_BaseDocument],
    state: ResourceState.Snapshot | None,
    task: Task,
    tombstone: _BaseDocument,
):
    """Snapshot the content of a resource at a regular interval."""

    if not state:
        state = ResourceState.Snapshot(
            updated_at=datetime.fromtimestamp(0, tz=UTC),
            last_count=0,
            last_digest="",
        )

    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(snapshot=state)}
    )

    while True:
        # Yield to the event loop to prevent starvation.
        # Note that wait_for does *not* yield if sleep_for has already elapsed.
        await asyncio.sleep(0)

        next_sync = state.updated_at + binding.resourceConfig.interval
        sleep_for = next_sync - datetime.now(tz=UTC)

        task.log.debug(
            "awaiting next snapshot",
            {"sleep_for": sleep_for, "next": next_sync},
        )

        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.log.debug(f"periodic snapshot is idle and is yielding to stop")
            return
        except asyncio.TimeoutError:
            # `sleep_for` elapsed.
            state.updated_at = datetime.now(tz=UTC)

        count = 0
        async for doc in fetch_snapshot(task.log):
            if isinstance(doc, dict):
                doc["meta_"] = {
                    "op": "u" if count < state.last_count else "c",
                    "row_id": count,
                }
            else:
                doc.meta_ = BaseDocument.Meta(
                    op="u" if count < state.last_count else "c", row_id=count
                )
            task.captured(binding_index, doc)
            count += 1

        digest = task.pending_digest()
        task.log.debug(
            "polled snapshot",
            {
                "count": count,
                "digest": digest,
                "last_count": state.last_count,
                "last_digest": state.last_digest,
            },
        )

        if digest != state.last_digest:
            for del_id in range(count, state.last_count):
                tombstone.meta_ = BaseDocument.Meta(op="d", row_id=del_id)
                task.captured(binding_index, tombstone)

            state.last_count = count
            state.last_digest = digest
        else:
            # Suppress all captured documents, as they're unchanged.
            task.reset()

        task.checkpoint(connector_state)


async def _binding_backfill_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_page: FetchPageFn[_BaseDocument],
    state: ResourceState.Backfill,
    task: Task,
):
    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(backfill=state)}
    )

    if state.next_page is not None:
        task.log.info(f"resuming backfill", state)
    else:
        task.log.info(f"beginning backfill", state)

    while True:
        # Yield to the event loop to prevent starvation.
        await asyncio.sleep(0)

        if task.stopping.event.is_set():
            task.log.debug(f"backfill is yielding to stop")
            return

        # Track if fetch_page returns without having yielded a PageCursor.
        done = True

        async for item in fetch_page(task.log, state.next_page, state.cutoff):
            if isinstance(item, BaseDocument) or isinstance(item, dict):
                task.captured(binding_index, item)
                done = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                done = True
            elif item is None:
                raise RuntimeError(
                    f"Implementation error: FetchPageFn yielded PageCursor None. To represent end-of-sequence, yield documents and return without a final PageCursor."
                )
            else:
                state.next_page = item
                task.checkpoint(connector_state)
                done = False

        if done:
            break

    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(backfill=None)}
    )
    task.checkpoint(connector_state)
    task.log.info(f"completed backfill")


async def _binding_incremental_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_changes: FetchChangesFn[_BaseDocument],
    state: ResourceState.Incremental,
    task: Task,
):
    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(inc=state)}
    )

    sleep_for = timedelta()

    task.log.info(f"resuming incremental replication", state)

    if isinstance(state.cursor, datetime):
        lag = datetime.now(tz=UTC) - state.cursor

        if lag < binding.resourceConfig.interval:
            sleep_for = binding.resourceConfig.interval - lag
            task.log.info(
                "incremental task ran recently, sleeping until `interval` has fully elapsed",
                {"sleep_for": sleep_for, "interval": binding.resourceConfig.interval},
            )

    while True:
        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.log.debug(f"incremental replication is idle and is yielding to stop")
            return
        except asyncio.TimeoutError:
            pass  # `sleep_for` elapsed.

        checkpoints = 0
        pending = False

        async for item in fetch_changes(task.log, state.cursor):
            if isinstance(item, BaseDocument) or isinstance(item, dict):
                task.captured(binding_index, item)
                pending = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                pending = True
            elif item == Triggers.BACKFILL:
                task.log.info("incremental task triggered backfill")
                task.stopping.event.set()
                task.checkpoint(
                    ConnectorState(backfillRequests={binding.stateKey: True})
                )
                return
            else:
                # Ensure LogCursor types match and that they're strictly increasing.
                is_larger = False
                if isinstance(item, int) and isinstance(state.cursor, int):
                    is_larger = item > state.cursor
                elif isinstance(item, datetime) and isinstance(state.cursor, datetime):
                    is_larger = item > state.cursor
                elif (
                    isinstance(item, tuple)
                    and isinstance(state.cursor, tuple)
                    and isinstance(item[0], str)
                    and isinstance(state.cursor[0], str)
                ):
                    is_larger = item[0] > state.cursor[0]
                else:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} of a different type than the last LogCursor {state.cursor}",
                    )

                if not is_larger:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} which is not greater than the last LogCursor {state.cursor}",
                    )

                state.cursor = item
                task.checkpoint(connector_state)
                checkpoints += 1
                pending = False

        if pending:
            raise RuntimeError(
                "Implementation error: FetchChangesFn yielded a documents without a final LogCursor",
            )

        if not checkpoints:
            # We're idle. Sleep for the full back-off interval.
            sleep_for = binding.resourceConfig.interval

        elif isinstance(state.cursor, datetime):
            lag = datetime.now(tz=UTC) - state.cursor

            if lag > binding.resourceConfig.interval:
                # We're not idle. Attempt to fetch the next changes.
                sleep_for = timedelta()
                continue
            else:
                # We're idle. Sleep until the cursor is `interval` old.
                sleep_for = binding.resourceConfig.interval - lag
        else:
            # We're not idle. Attempt to fetch the next changes.
            sleep_for = timedelta()
            continue

        task.log.debug(
            "incremental task is idle", {"sleep_for": sleep_for, "cursor": state.cursor}
        )
