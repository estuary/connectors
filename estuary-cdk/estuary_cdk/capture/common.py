from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
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
from logging import Logger
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    NonNegativeInt,
)
from uuid import UUID
import asyncio
import abc


from . import Task, request, response

from ..flow import (
    AccessToken,
    BaseOAuth2Credentials,
    CaptureBinding,
    OAuth2Spec,
    ValidationError,
)

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
        uuid: UUID = Field(default=UUID(int=0), description="UUID of the Document")

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

    PATH_POINTERS: ClassVar[list[str]] = ["/schema", "/name"]

    name: str = Field(description="Name of this resource")
    namespace: str | None = Field(
        default=None, description="Enclosing schema namespace of this resource"
    )
    interval: timedelta = Field(
        default=timedelta(), description="Interval between updates for this resource"
    )

    def path(self) -> list[str]:
        if self.namespace:
            return [self.namespace, self.name]

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
            description="PageCursor of the next page to fetch"
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


class ConnectorState(BaseModel, Generic[_BaseResourceState], extra="forbid"):
    """ConnectorState represents a number of ResourceStates, keyed by binding state key."""

    bindingStateV1: dict[str, _BaseResourceState] = {}


_ConnectorState = TypeVar("_ConnectorState", bound=ConnectorState)


FetchSnapshotFn = Callable[[Logger], AsyncGenerator[_BaseDocument, None]]
"""
FetchSnapshotFn is a function which fetches a complete snapshot of a resource.
"""

FetchPageFn = Callable[
    [PageCursor, LogCursor, Logger],
    Awaitable[tuple[Iterable[_BaseDocument], PageCursor]],
]
"""
FetchPageFn is a function which fetches a page of documents.
It takes a PageCursor for the next page and an upper-bound "cutoff" LogCursor.
It returns documents and an updated PageCursor, where None indicates
no further pages remain.

The "cutoff" LogCursor represents the log position at which incremental
replication started, and should be used to filter returned documents
which were modified at-or-after the cutoff, as such documents are
already observed through incremental replication.
"""

FetchChangesFn = Callable[
    [LogCursor, Logger],
    Awaitable[tuple[AsyncGenerator[_BaseDocument, None], LogCursor]],
]
"""
FetchChangesFn is a function which fetches available documents since the LogCursor.
It returns a tuple of Documents and an update LogCursor which reflects progress
against processing the log. If no documents are available, then it returns an
empty AsyncGenerator rather than waiting indefinitely for more documents.

Implementations may block for brief periods to await documents, such as while
awaiting a server response, but should not block forever as it prevents the
connector from exiting.

Implementations should NOT sleep or implement their own coarse rate limit.
Instead, configure a resource `interval` to enable periodic polling.
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
            schema = resource.model.model_json_schema()

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
        next_sync = state.updated_at + binding.resourceConfig.interval
        sleep_for = next_sync - datetime.now(tz=UTC)

        task.logger.debug(
            "awaiting next snapshot",
            {"sleep_for": sleep_for, "next": next_sync},
        )

        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.logger.debug(f"periodic snapshot is idle and is yielding to stop")
            return
        except asyncio.TimeoutError:
            # `sleep_for` elapsed.
            state.updated_at = datetime.now(tz=UTC)

        count = 0
        async for doc in fetch_snapshot(task.logger):
            doc.meta_ = BaseDocument.Meta(
                op="u" if count < state.last_count else "c", row_id=count
            )
            task.captured(binding_index, doc)
            count += 1

        digest = task.pending_digest()
        task.logger.debug(
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

    if state.next_page:
        task.logger.info(f"resuming backfill", state)
    else:
        task.logger.info(f"beginning backfill", state)

    while True:
        page, next_page = await fetch_page(state.next_page, state.cutoff, task.logger)
        for doc in page:
            task.captured(binding_index, doc)

        if next_page is not None:
            state.next_page = next_page
            task.checkpoint(connector_state)
        else:
            break

    connector_state = ConnectorState(
        bindingStateV1={binding.stateKey: ResourceState(backfill=None)}
    )
    task.checkpoint(connector_state)
    task.logger.info(f"completed backfill")


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
    task.logger.info(f"resuming incremental replication", state)

    while True:
        changes, next_cursor = await fetch_changes(state.cursor, task.logger)

        count = 0
        async for doc in changes:
            task.captured(binding_index, doc)
            count += 1

        if count != 0 or next_cursor != state.cursor:
            state.cursor = next_cursor
            task.checkpoint(connector_state)

        if count != 0:
            continue  # Immediately fetch subsequent changes.

        # At this point we've fully caught up with the log and are idle.
        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(),
                    timeout=binding.resourceConfig.interval.total_seconds(),
                )

            task.logger.debug(
                f"incremental replication is idle and is yielding to stop"
            )
            return
        except asyncio.TimeoutError:
            pass  # `interval` elapsed.
