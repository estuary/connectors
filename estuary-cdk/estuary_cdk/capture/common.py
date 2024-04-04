import abc
import asyncio
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
)
from pydantic import AwareDatetime, BaseModel, Field, NonNegativeInt

from ..flow import (
    AccessToken,
    BaseOAuth2Credentials,
    CaptureBinding,
    OAuth2Spec,
    ValidationError,
)
from ..pydantic_polyfill import GenericModel
from . import Task, request, response


LogCursor = AwareDatetime | NonNegativeInt
"""LogCursor is a cursor into a logical log of changes.
The two predominant strategies for accessing logs are:
 a) fetching entities which were created / updated / deleted since a given datetime.
 b) fetching changes by their offset in a sequential log (Kafka partition or Gazette journal). 

Note that `str` cannot be added to this type union, as it makes parsing states ambiguous.
"""

PageCursor = str | NonNegativeInt | None
"""PageCursor is a cursor into a paged result set.
These cursors are predominantly an opaque string or an internal offset integer.

None means "begin a new iteration" in a request context,
and "no pages remain" in a response context.
"""


class BaseDocument(BaseModel):

    class Meta(BaseModel):
        op: Literal["c", "u", "d"] = Field(
            description="Operation type (c: Create, u: Update, d: Delete)"
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
            description="PageCursor of the next page to fetch",
            default=None
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

    inc: Incremental | None = Field(
        default=None, description="Incremental capture progress"
    )

    backfill: Backfill | None = Field(
        default=None,
        description="Backfill progress, or None if no backfill is occurring",
    )

    snapshot: Snapshot | None = Field(default=None, description="Snapshot progress")


_ResourceState = TypeVar("_ResourceState", bound=ResourceState)


class ConnectorState(GenericModel, Generic[_BaseResourceState], extra="forbid"):
    """ConnectorState represents a number of ResourceStates, keyed by binding state key."""

    bindingStateV1: dict[str, _BaseResourceState] = {}


_ConnectorState = TypeVar("_ConnectorState", bound=ConnectorState)


FetchSnapshotFn = Callable[[Logger], AsyncGenerator[_BaseDocument, None]]
"""
FetchSnapshotFn is a function which fetches a complete snapshot of a resource.

Snapshot resources are typically "small" -- they fit easily on disk -- and are
gathered in a single shot. Its content is digested to determine if its
changed since the last snapshot. If it hasn't, the snapshot is discarded and
not emitted by the connector.
"""

FetchPageFn = Callable[
    [Logger, PageCursor, LogCursor],
    AsyncGenerator[_BaseDocument | PageCursor, None],
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
    AsyncGenerator[_BaseDocument | LogCursor, None],
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
        for index, (binding, resource) in enumerate(resolved_bindings):
            state: _ResourceState | None = open.state.bindingStateV1.get(
                binding.stateKey
            )

            if state is None:
                # Checkpoint the binding's initialized state prior to any processing.
                task.checkpoint(
                    ConnectorState(
                        bindingStateV1={binding.stateKey: resource.initial_state}
                    )
                )
                state = resource.initial_state

            resource.open(
                binding,
                index,
                state,
                task,
            )

    return (response.Opened(explicitAcknowledgements=False), _run)


def open_binding(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    state: _ResourceState,
    task: Task,
    fetch_changes: FetchChangesFn[_BaseDocument] | None = None,
    fetch_page: FetchPageFn[_BaseDocument] | None = None,
    fetch_snapshot: FetchSnapshotFn[_BaseDocument] | None = None,
    tombstone: _BaseDocument | None = None,
):
    """
    open_binding() is intended to be called by closures set as Resource.open Callables.

    It does 'heavy lifting' to actually capture a binding.

    TODO(johnny): Separate into snapshot vs incremental tasks?
    """

    prefix = ".".join(binding.resourceConfig.path())

    if fetch_changes:

        async def closure(task: Task):
            assert state.inc
            await _binding_incremental_task(
                binding, binding_index, fetch_changes, state.inc, task
            )

        task.spawn_child(f"{prefix}.incremental", closure)

    if fetch_page and state.backfill:

        async def closure(task: Task):
            assert state.backfill
            await _binding_backfill_task(
                binding, binding_index, fetch_page, state.backfill, task
            )

        task.spawn_child(f"{prefix}.backfill", closure)

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

        # Track if fetch_page returns without having yielded a PageCursor.
        done = True 

        async for item in fetch_page(task.log, state.next_page, state.cutoff):
            if isinstance(item, BaseDocument):
                task.captured(binding_index, item)
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
    task.log.info(f"resuming incremental replication", state)

    while True:
        # Yield to the event loop to prevent starvation.
        await asyncio.sleep(0)

        checkpoints = 0
        pending = False

        async for item in fetch_changes(task.log, state.cursor):
            if isinstance(item, BaseDocument):
                task.captured(binding_index, item)
                pending = True
            else:

                # Ensure LogCursor types match and that they're strictly increasing.
                is_larger = False
                if isinstance(item, int) and isinstance(state.cursor, int):
                    is_larger = item > state.cursor
                elif isinstance(item, datetime) and isinstance(state.cursor, datetime):
                    is_larger = item > state.cursor
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

        sleep_for : timedelta = binding.resourceConfig.interval

        if not checkpoints:
            # We're idle. Sleep for the full back-off interval.
            sleep_for = binding.resourceConfig.interval 

        elif isinstance(state.cursor, datetime):
            lag = (datetime.now(tz=UTC) - state.cursor)

            if lag > binding.resourceConfig.interval:
                # We're not idle. Attempt to fetch the next changes.
                continue
            else:
                # We're idle. Sleep until the cursor is `interval` old.
                sleep_for = binding.resourceConfig.interval - lag
        else:
            # We're not idle. Attempt to fetch the next changes.
            continue

        task.log.debug("incremental task is idle", {"sleep_for": sleep_for, "cursor": state.cursor})

        # At this point we've fully caught up with the log and are idle.
        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.log.debug(f"incremental replication is idle and is yielding to stop")
            return
        except asyncio.TimeoutError:
            pass  # `interval` elapsed.
