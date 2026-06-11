import abc
import asyncio
import functools
import inspect
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum
from itertools import combinations
from logging import Logger
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    TypeVar,
    cast,
)
from typing_extensions import override

from pydantic import AwareDatetime, BaseModel, Field, NonNegativeInt

from ..cron import next_fire
from ..flow import (
    AccessToken,
    AuthorizationCodeFlowOAuth2Credentials,
    BaseOAuth2Credentials,
    BasicAuth,
    CaptureBinding,
    ClientCredentialsOAuth2Credentials,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    OAuth2TokenFlowSpec,
    ResourceOwnerPasswordOAuth2Credentials,
    RotatingOAuth2Credentials,
    ValidationError,
)
from ..pydantic_polyfill import GenericModel
from ..utils import json_merge_patch
from . import Task, request, response
from .webhook.match import CollectionMatchingSpec, UrlMatch
from .document import _BaseDocument, AssociatedDocument, BaseDocument

LogCursor = tuple[str | int] | AwareDatetime | NonNegativeInt
"""LogCursor is a cursor into a logical log of changes.
The two predominant strategies for accessing logs are:
 a) fetching entities which were created / updated / deleted since a given datetime.
 b) fetching changes by their offset in a sequential log (Kafka partition or Gazette journal).

Note that `str` cannot be added to this type union, as it makes parsing states ambiguous, however
the tuple type allows for str or integer values to be used
"""

# Cursor marker for dict-based cursors.
# This marker is necessary to distinguish between regular dictionaries (documents) 
# and cursor dictionaries when yielded by fetch functions. Without this marker,
# the runtime cannot determine whether a dict should be treated as a document to
# capture or as a PageCursor for checkpointing progress.
CURSOR_MARKER = "__flow_cursor__"


def is_cursor_dict(item: dict) -> bool:
    """Check if a dict is a cursor by looking for the cursor marker."""
    return item.get(CURSOR_MARKER) is True


def make_cursor_dict(data: dict) -> dict:
    """Convert a regular dict to a cursor dict by adding the marker.
    
    The intended way of using this is to first call `make_cursor_dict` on a dictionary that
    represents the full state of a resource's `PageCursor` and then subsequent calls representing changes
    to that state. This allows JSON merge patches to be used for efficient incremental updates.
    
    IMPORTANT: When checkpointing, connectors should yield only the changes/patches that need to be
    merged into the backfill state's next_page, not the entire dictionary. This approach:
    - Keeps checkpoints relatively small 
    - Reduces the impact checkpoints have on the recovery log size
    - Enables efficient incremental state updates via JSON merge patches
    
    Example:
    ```python
    # Initial full state - use make_cursor_dict for the complete state
    initial_full_page_cursor = make_cursor_dict({"board1": {"items": 10}, "board2": {"items": 20}})
    yield initial_full_page_cursor

    # do some processing for a single invocation of fetch_page

    # yield a patch for processed boards - just the changes, not the full state
    completed_boards = {"board1": None}  # marks board1 as completed
    yield completed_boards
    ```
    
    Args:
        data: The dictionary data to mark as a cursor. For initial state, this should be
              the complete cursor state. For subsequent calls, this should contain only
              the changes to be merged.
    """
    return {CURSOR_MARKER: True, **data}


def pop_cursor_marker(cursor: dict) -> dict:
    """Remove the cursor marker from a cursor dict."""
    cursor.pop(CURSOR_MARKER, None)
    return cursor


PageCursor = str | int | dict[str, Any] | None
"""PageCursor is a cursor into a paged result set.
These cursors are predominantly an opaque string or an internal offset integer.
When a dict is used, it represents a structured cursor that supports JSON merge patches
for efficient incremental updates.

None means "begin a new iteration" in a request context,
and "no pages remain" in a response context.
"""

class Triggers(Enum):
    BACKFILL = "BACKFILL"


@dataclass
class SourcedSchema:
    """
    SourcedSchema encapsulates a source-defined schema for a specific binding.
    SourcedSchemas are used to inform the runtime about types & metadata that
    aren't easily inferred by inspecting specific documents. The runtime
    widens inferred schemas to accommodate the current inferred schema along
    with any SourcedSchemas.
    """

    value: dict[str, Any]


class BaseResourceConfig(abc.ABC, BaseModel, extra="forbid"):
    """
    AbstractResourceConfig is a base class for ResourceConfig classes.
    """

    PATH_POINTERS: ClassVar[list[str]]

    meta_: dict | None = Field(
        default=None, alias="_meta", title="Meta",
    )

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


class WebhookResourceConfig(BaseResourceConfig):
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]

    name: str = Field(description="Name of this resource")
    match_rule: CollectionMatchingSpec = Field(
        default_factory=lambda: UrlMatch(value="*"),
        description="Matching spec for routing incoming webhooks to this collection",
        discriminator="type",
    )

    @override
    def path(self) -> list[str]:
        return [self.name]


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


async def scheduled_stop(task: Task, future_dt: datetime) -> None:
    sleep_duration = future_dt - datetime.now(tz=UTC)
    await asyncio.sleep(sleep_duration.total_seconds())
    task.stopping.event.set()


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

    snapshot: Snapshot | dict[str, Snapshot | None] | None = Field(
        default=None, description="Snapshot progress"
    )

    last_initialized: datetime | None = Field(
        default=None, description="The last time this state was initialized."
    )

    is_connector_initiated: bool = Field(
        default=False,
        description="Indicates if this backfill was initiated by the connector.",
    )


_ResourceState = TypeVar("_ResourceState", bound=ResourceState)


class ConnectorState(GenericModel, Generic[_BaseResourceState], extra="forbid"):
    """ConnectorState represents a number of ResourceStates, keyed by binding state key."""

    bindingStateV1: dict[str, _BaseResourceState | None] = {}
    backfillRequests: dict[str, bool | None] = {}
    # A refresh token that's more recent than the one in the connector's spec. It's used when
    # a connector requires periodically rotating refresh tokens, otherwise it's None.
    refresh_token: str | None = None


_ConnectorState = TypeVar("_ConnectorState", bound=ConnectorState)

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

RecurringFetchPageFn = Callable[
    [Logger, PageCursor, LogCursor, bool],
    AsyncGenerator[_BaseDocument | dict | AssociatedDocument | PageCursor, None],
]
"""
RecurringFetchPagesFn fetches available checkpoints since the provided last PageCursor.
It will typically fetch just one page, though it may fetch multiple pages.

RecurringFetchPagesFn is intended to start new iterations on some cadence,
often based on a schedule set in ResourceConfigWithSchedule.

The argument PageCursor is None if a new iteration is being started.
Otherwise it is the last PageCursor yielded by RecurringFetchPagesFn.

The argument LogCursor is the "cutoff" log position at which incremental
replication started, and should be used to suppress documents which were
modified at-or-after the cutoff, as such documents are
already observed through incremental replication.

The boolean argument signals if this iteration/backfill was connector-initiated.
It is True if this iteration was connector-initiated, and False otherwise.

Checkpoints consist of a yielded sequence of documents followed by a
non-None PageCursor, which checkpoints those preceding documents,
or by simply returning if the iteration is complete.

It's an error if RecurringFetchPagesFn yields a PageCursor of None.
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


def is_recurring_fetch_page_fn(fn: FetchPageFn | RecurringFetchPageFn, log: Logger, page: PageCursor, cutoff: LogCursor, is_connector_initiated: bool) -> bool:
    """Check if the function signature accepts the arguments of a RecurringFetchPageFn."""
    try:
        inspect.signature(fn).bind(log, page, cutoff, is_connector_initiated)
        return True
    except TypeError:
        return False


class ReductionStrategy(StrEnum):
    APPEND = "append"
    FIRST_WRITE_WINS = "firstWriteWins"
    LAST_WRITE_WINS = "lastWriteWins"
    MERGE = "merge"
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"
    SET = "set"
    SUM = "sum"


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
    # The open callback can be async or sync.
    # Async is required when the callback needs to call task.checkpoint().
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
        None | Awaitable[None],
    ]
    initial_state: _BaseResourceState
    initial_config: _BaseResourceConfig
    schema_inference: bool
    reduction_strategy: ReductionStrategy | None = None
    disable: bool = False
    requires_explicit_acks: bool = False


@dataclass(kw_only=True)
class SnapshotResource(Resource[_BaseDocument, _BaseResourceConfig, ResourceState]):
    """A Resource for snapshot bindings with standard defaults that
    work well with the rest of the CDK and Estuary Flow platform.

    Sets key to /_meta/row_id (required for CDK deletion inference),
    initial_state to an empty ResourceState, model to BaseDocument
    with schema_inference enabled (snapshot bindings rely on schema
    inference rather than a typed model) — the invariant choices
    for snapshot bindings.
    """

    key: list[str] = field(default_factory=lambda: ["/_meta/row_id"])
    initial_state: ResourceState = field(default_factory=ResourceState)
    model: type[BaseDocument] | Resource.FixedSchema = BaseDocument
    schema_inference: bool = True


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

        # Materializations require this if/then/else conditional to support
        # hard deletes. It tells Flow to use a top-level merge reduction
        # strategy for deletions and mark them with `delete: True` so materializations
        # know to issue DELETEs to destinations.
        #
        # `_meta` and `_meta.op` are both `required` in the `if` clause because
        # JSON Schema's `properties` keyword passes when a key is absent.
        # Without these `required` assertions, a document missing `_meta`
        # or `_meta.op` would still match the `if` and get marked as a
        # delete. The `required` clauses force the `if` to match only when
        # `_meta.op` is present and equals `"d"`.
        schema["if"] = {
            "required": ["_meta"],
            "properties": {
                "_meta": {
                    "required": ["op"],
                    "properties": {"op": {"const": "d"}},
                }
            },
        }
        schema["then"] = {"reduce": {"strategy": "merge", "delete": True}}
        if resource.reduction_strategy:
            schema["else"] = {"reduce": {"strategy": resource.reduction_strategy}}

        bindings.append(
            response.DiscoveredBinding(
                documentSchema=schema,
                key=resource.key,
                recommendedName=resource.name,
                resourceConfig=resource.initial_config,
                disable=resource.disable,
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

    # Check for routing conflicts between webhook match rules.
    webhook_match_rules = [
        resource.initial_config.match_rule
        for resource in resources
        if isinstance(resource.initial_config, WebhookResourceConfig)
    ]
    errors.extend(
        filter(
            bool,
            (
                rule.list_compatibility_errors(other)
                for rule, other in combinations(webhook_match_rules, 2)
            ),
        )
    )

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


def _get_min_incremental_cursor(state: ResourceState) -> datetime | None:
    """Extract the minimum incremental cursor from a ResourceState.

    Returns the minimum datetime cursor if a valid one can be determined, None otherwise.
    Handles both single incremental task (state.inc is ResourceState.Incremental)
    and multiple incremental tasks (state.inc is a dict).
    """
    if isinstance(state.inc, ResourceState.Incremental):
        if isinstance(state.inc.cursor, datetime):
            return state.inc.cursor
    elif isinstance(state.inc, dict):
        min_cursor: datetime | None = None
        for inc_state in state.inc.values():
            if isinstance(inc_state, ResourceState.Incremental) and isinstance(inc_state.cursor, datetime):
                if min_cursor is None or inc_state.cursor < min_cursor:
                    min_cursor = inc_state.cursor
        return min_cursor
    return None


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
                await task.checkpoint(
                    ConnectorState(
                        bindingStateV1={stateKey: None},
                        backfillRequests={stateKey: None},
                    )
                )

        soonest_future_scheduled_initialization: datetime | None = None
        NOW = datetime.now(tz=UTC)

        for index, (binding, resource) in enumerate(resolved_bindings):
            state: _ResourceState | None = open.state.bindingStateV1.get(
                binding.stateKey
            )

            should_initialize = state is None or binding.stateKey in backfill_requests
            is_connector_initiated = False

            if state:
                if state.last_initialized is None:
                    state.last_initialized = NOW
                    await task.checkpoint(
                        ConnectorState(bindingStateV1={binding.stateKey: state})
                    )

                if not state.backfill and isinstance(binding.resourceConfig, ResourceConfigWithSchedule):
                    cron_schedule = binding.resourceConfig.schedule
                    missed_scheduled_initialization = next_fire(
                        cron_schedule, state.last_initialized, NOW
                    )
                    future_scheduled_initialization = next_fire(
                        cron_schedule, NOW,
                    )

                    if missed_scheduled_initialization:
                        should_initialize = True
                        is_connector_initiated = True

                    if future_scheduled_initialization:
                        if not soonest_future_scheduled_initialization:
                            soonest_future_scheduled_initialization = future_scheduled_initialization
                        else:
                            soonest_future_scheduled_initialization = min(soonest_future_scheduled_initialization, future_scheduled_initialization)

            if should_initialize:
                if is_connector_initiated:
                    # Attempt to coordinate the initialized backfill's cutoff with the current incremental
                    # state's cursor(s), preserving the incremental cursor position(s).
                    min_cursor = _get_min_incremental_cursor(state) if state else None
                    initial_backfill_state = resource.initial_state.backfill

                    # Check if we can coordinate the backfill cutoff with the incremental cursor.
                    # We currently only perform this coordination when there's a single backfill task.
                    if (
                        state and
                        min_cursor is not None and
                        isinstance(initial_backfill_state, ResourceState.Backfill) and
                        isinstance(initial_backfill_state.cutoff, datetime)
                    ):
                        state.backfill = initial_backfill_state.model_copy(deep=True)
                        state.backfill.cutoff = min_cursor
                    # In all other cases, wipe the state back to the initial state.
                    else:
                        state = resource.initial_state.model_copy(deep=True)

                    state.is_connector_initiated = True
                else:
                    state = resource.initial_state.model_copy(deep=True)

                state.last_initialized = NOW

                # Checkpoint the binding's initialized state prior to any processing.
                await task.checkpoint(
                    ConnectorState(
                        bindingStateV1={binding.stateKey: state},
                    )
                )

            result = resource.open(
                binding,
                index,
                state,
                task,
                resolved_bindings,
            )
            # Support both sync and async open callbacks
            if inspect.iscoroutine(result):
                await result

        if soonest_future_scheduled_initialization:
            # Gracefully exit to ensure relatively close adherence to any bindings'
            # re-initialization schedules.
            asyncio.create_task(
                scheduled_stop(task, soonest_future_scheduled_initialization)
            )

        if task.stopping.webhook_task is not None:
            # The webhook server runs outside the TaskGroup so it can outlive
            # non-webhook tasks during two-phase shutdown. Block here so the
            # TaskGroup stays alive until graceful shutdown begins.
            await task.stopping.event.wait()

    requires_explicit_acks = any(
        rsc.requires_explicit_acks for _, rsc in resolved_bindings
    )

    return (response.Opened(explicitAcknowledgements=requires_explicit_acks), _run)


def _tombstone_discriminator_values(
    tombstone: BaseDocument, key: list[str]
) -> tuple[object, ...]:
    """Return a tombstone's collection-key values other than the CDK-managed
    /_meta/row_id, in key order. These are what must differ between snapshot
    subtasks: two subtasks sharing them would emit tombstones into the same
    (discriminator, row_id) keyspace."""
    dump = tombstone.model_dump(by_alias=True)
    values: list[object] = []
    for key_pointer in key:
        # row_id is assigned per-pass by the CDK and is identical across
        # subtasks, so it can't distinguish them.
        if key_pointer == "/_meta/row_id":
            continue
        node: Any = dump
        for field in key_pointer.strip("/").split("/"):
            # A KeyError here means the tombstone is missing a key field.
            node = node[field]
        values.append(node)
    return tuple(values)


def open_binding(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    resource_state: _ResourceState,
    task: Task,
    fetch_changes: FetchChangesFn[_BaseDocument]
    | dict[str, FetchChangesFn[_BaseDocument]]
    | None = None,
    fetch_page: FetchPageFn[_BaseDocument]
    | RecurringFetchPageFn[_BaseDocument]
    | dict[str, FetchPageFn[_BaseDocument]]
    | None = None,
    fetch_snapshot: FetchSnapshotFn[_BaseDocument]
    | dict[str, FetchSnapshotFn[_BaseDocument]]
    | None = None,
    tombstone: _BaseDocument | dict[str, _BaseDocument] | None = None,
):
    """
    open_binding() is intended to be called by closures set as Resource.open Callables.

    It does 'heavy lifting' to actually capture a binding.

    When fetch_changes, fetch_page, or fetch_snapshot are provided as dictionaries,
    each function will be run as a separate subtask with its own independent state.
    The dictionary keys are used as subtask IDs and are used to store and retrieve
    the state for each subtask in state.inc, state.backfill, or state.snapshot.
    """

    task.connector_status.inc_binding_count()
    prefix = ".".join(binding.resourceConfig.path())

    if fetch_changes:

        async def incremental_closure(
            task: Task,
            fetch_changes: FetchChangesFn[_BaseDocument],
            state: ResourceState.Incremental,
            subtask_id: str | None = None,
        ):
            assert state and not isinstance(state, dict)
            await _binding_incremental_task(
                binding,
                binding_index,
                fetch_changes,
                state,
                task,
                subtask_id,
            )

        if isinstance(fetch_changes, dict):
            assert resource_state.inc and isinstance(resource_state.inc, dict)
            for subtask_id, subtask_fetch_changes in fetch_changes.items():
                inc_state = resource_state.inc.get(subtask_id)
                assert inc_state

                task.spawn_child(
                    f"{prefix}.incremental.{subtask_id}",
                    functools.partial(
                        incremental_closure,
                        fetch_changes=subtask_fetch_changes,
                        state=inc_state,
                        subtask_id=subtask_id,
                    ),
                )
        else:
            assert resource_state.inc and not isinstance(resource_state.inc, dict)
            task.spawn_child(
                f"{prefix}.incremental",
                functools.partial(
                    incremental_closure,
                    fetch_changes=fetch_changes,
                    state=resource_state.inc,
                ),
            )

    if fetch_page and resource_state.backfill:

        async def backfill_closure(
            task: Task,
            fetch_page: FetchPageFn[_BaseDocument] | RecurringFetchPageFn,
            state: ResourceState.Backfill,
            subtask_id: str | None = None,
        ):
            assert state and not isinstance(state, dict)
            task.connector_status.inc_backfilling(binding_index)
            await _binding_backfill_task(
                binding,
                binding_index,
                fetch_page,
                state,
                resource_state.last_initialized,
                resource_state.is_connector_initiated,
                task,
                subtask_id,
            )
            task.connector_status.dec_backfilling(binding_index)

        if isinstance(fetch_page, dict):
            assert resource_state.backfill and isinstance(resource_state.backfill, dict)
            for subtask_id, subtask_fetch_page in fetch_page.items():
                backfill_state = resource_state.backfill.get(subtask_id)

                if not backfill_state:
                    continue

                task.spawn_child(
                    f"{prefix}.backfill.{subtask_id}",
                    functools.partial(
                        backfill_closure,
                        fetch_page=subtask_fetch_page,
                        state=backfill_state,
                        subtask_id=subtask_id,
                    ),
                )

        else:
            assert resource_state.backfill and not isinstance(resource_state.backfill, dict)
            task.spawn_child(
                f"{prefix}.backfill",
                functools.partial(
                    backfill_closure,
                    fetch_page=fetch_page,
                    state=resource_state.backfill,
                ),
            )

    if fetch_snapshot:

        async def snapshot_closure(
            task: Task,
            fetch_snapshot: FetchSnapshotFn[_BaseDocument],
            state: ResourceState.Snapshot | None,
            tombstone: _BaseDocument,
            subtask_id: str | None = None,
        ):
            await _binding_snapshot_task(
                binding,
                binding_index,
                fetch_snapshot,
                state,
                task,
                tombstone,
                subtask_id,
            )

        if isinstance(fetch_snapshot, dict):
            key = binding.collection.key
            assert "/_meta/row_id" in key and len(key) > 1, (
                f"A fetch_snapshot subtask requires a compound collection "
                f"key containing /_meta/row_id plus a subtask discriminator. "
                f"Got: {key}"
            )
            # The CDK can set _meta/row_id but not the connector-specific discriminator, so
            # each subtask must supply its own tombstone with that field pre-set.
            assert (
                isinstance(tombstone, dict)
                and tombstone.keys() == fetch_snapshot.keys()
            ), (
                "fetch_snapshot subtasks requires a tombstone dict with "
                "the same keys, each carrying the subtask discriminator. "
            )

            # Each subtask numbers row_id independently from zero, so the
            # non-row_id key field(s) are all that distinguish one subtask's
            # tombstones from another's. If two subtasks shared a discriminator,
            # a deletion from one would land on the other's keyspace.
            discriminators = [
                _tombstone_discriminator_values(subtask_tombstone, key)
                for subtask_tombstone in tombstone.values()
            ]
            assert len(set(discriminators)) == len(discriminators), (
                f"fetch_snapshot subtask tombstones must carry distinct key "
                f"discriminator values, one per subtask. Got: {discriminators}"
            )

            snapshot_state = resource_state.snapshot
            assert snapshot_state is None or isinstance(snapshot_state, dict)

            for subtask_id, subtask_fetch_snapshot in fetch_snapshot.items():
                subtask_state = (
                    snapshot_state.get(subtask_id) if snapshot_state else None
                )
                task.spawn_child(
                    f"{prefix}.snapshot.{subtask_id}",
                    functools.partial(
                        snapshot_closure,
                        fetch_snapshot=subtask_fetch_snapshot,
                        state=subtask_state,
                        tombstone=tombstone[subtask_id],
                        subtask_id=subtask_id,
                    ),
                )
        else:
            if tombstone is None:
                # Default tombstone for snapshot bindings so callers don't need to
                # provide one. The cast satisfies the _BaseDocument TypeVar since
                # BaseDocument is its bound.
                tombstone = cast(
                    _BaseDocument, BaseDocument(_meta=BaseDocument.Meta(op="d"))
                )
            assert not isinstance(tombstone, dict)
            assert not isinstance(resource_state.snapshot, dict)

            task.spawn_child(
                f"{prefix}.snapshot",
                functools.partial(
                    snapshot_closure,
                    fetch_snapshot=fetch_snapshot,
                    state=resource_state.snapshot,
                    tombstone=tombstone,
                ),
            )


async def _binding_snapshot_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_snapshot: FetchSnapshotFn[_BaseDocument],
    state: ResourceState.Snapshot | None,
    task: Task,
    tombstone: _BaseDocument,
    subtask_id: str | None = None,
):
    """Snapshot the content of a resource at a regular interval."""

    if not state:
        state = ResourceState.Snapshot(
            updated_at=datetime.fromtimestamp(0, tz=UTC),
            last_count=0,
            last_digest="",
        )

    if subtask_id is not None:
        connector_state = ConnectorState(
            bindingStateV1={binding.stateKey: ResourceState(snapshot={subtask_id: state})}
        )
    else:
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
            # Set only op/row_id, preserving any other _meta fields (e.g. a
            # connector-injected subtask discriminator that is part of a
            # compound key) rather than replacing _meta wholesale.
            op = "u" if count < state.last_count else "c"
            if isinstance(doc, dict):
                meta = doc.get("meta_")
                if not isinstance(meta, dict):
                    meta = {}
                    doc["meta_"] = meta
                meta["op"] = op
                meta["row_id"] = count
            else:
                # Reassign so meta_ is marked as explicitly set on
                # the parent document and included when serializing
                # with model_dump(exclude_unset=True).
                doc.meta_ = doc.meta_.model_copy(update={"op": op, "row_id": count})
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
                # Mutate only op/row_id so the tombstone
                # keeps any fields the caller pre-set.
                tombstone.meta_.op = "d"
                tombstone.meta_.row_id = del_id
                task.captured(binding_index, tombstone)

            state.last_count = count
            state.last_digest = digest
        else:
            # Suppress all captured documents, as they're unchanged.
            task.reset()

        await task.checkpoint(connector_state)


async def _binding_backfill_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_page: FetchPageFn[_BaseDocument] | RecurringFetchPageFn[_BaseDocument],
    state: ResourceState.Backfill,
    last_initialized: datetime | None,
    is_connector_initiated: bool,
    task: Task,
    subtask_id: str | None = None,
):
    def _initialize_connector_state(state: ResourceState.Backfill) -> ConnectorState:
        if subtask_id is not None:
            connector_state = ConnectorState(
                bindingStateV1={
                    binding.stateKey: ResourceState(backfill={subtask_id: state})
                }
            )
        else:
            connector_state = ConnectorState(
                bindingStateV1={binding.stateKey: ResourceState(backfill=state)}
            )

        return connector_state

    connector_state = _initialize_connector_state(state)

    if state.next_page is not None:
        task.log.info("resuming backfill", {"state": state, "subtask_id": subtask_id})
    else:
        task.log.info("beginning backfill", {"state": state, "subtask_id": subtask_id})

    while True:
        # Yield to the event loop to prevent starvation.
        await asyncio.sleep(0)

        if task.stopping.event.is_set():
            task.log.debug("backfill is yielding to stop", {"subtask_id": subtask_id})
            return

        # Track if fetch_page returns without having yielded a PageCursor.
        done = True

        # Distinguish between FetchPageFn and RecurringFetchPageFn to provide the correct arguments.
        if is_recurring_fetch_page_fn(fetch_page, task.log, state.next_page, state.cutoff, is_connector_initiated):
            fn = cast(RecurringFetchPageFn, fetch_page)
            pages = fn(task.log, state.next_page, state.cutoff, is_connector_initiated)
        else:
            fn = cast(FetchPageFn, fetch_page)
            pages = fn(task.log, state.next_page, state.cutoff)

        async for item in pages:
            if isinstance(item, BaseDocument) or (
                isinstance(item, dict) and not is_cursor_dict(item)
            ):
                task.captured(binding_index, item)
                done = True
            elif isinstance(item, AssociatedDocument):
                task.captured(item.binding, item.doc)
                done = True
            elif item is None:
                raise RuntimeError(
                    "Implementation error: FetchPageFn yielded PageCursor None. To represent end-of-sequence, yield documents and return without a final PageCursor."
                )
            else:
                if isinstance(item, dict) and is_cursor_dict(item):
                    # For dict-based cursors, create a separate state to checkpoint with just the item
                    # This ensures the Flow runtime gets the merge patch, not the merged result.

                    pop_cursor_marker(item)

                    if state.next_page is None:
                        state.next_page = item
                        state_to_checkpoint = connector_state
                    elif isinstance(state.next_page, dict):
                        # Perform JSON merge patch on the in-memory state.
                        json_merge_patch(state.next_page, item)
                        # Only emit a checkpoint containing the patch.
                        checkpoint_state = ResourceState.Backfill(
                            cutoff=state.cutoff, next_page=item
                        )
                        state_to_checkpoint = _initialize_connector_state(
                            checkpoint_state
                        )
                    else:
                        raise RuntimeError(
                            f"Implementation error: dictionary PageCursor was yielded but the previous PageCursor was a {type(state.next_page)}"
                        )
                else:
                    state.next_page = item
                    state_to_checkpoint = connector_state

                await task.checkpoint(state_to_checkpoint)
                done = False

        if done:
            break

    if subtask_id is not None:
        await task.checkpoint(
            ConnectorState(
                bindingStateV1={
                    binding.stateKey: ResourceState(backfill={subtask_id: None})
                }
            )
        )
    else:
        await task.checkpoint(
            ConnectorState(
                bindingStateV1={binding.stateKey: ResourceState(backfill=None)}
            )
        )
    task.log.info("completed backfill", {"subtask_id": subtask_id})

    # If this binding has an initialization schedule to adhere to, either stop
    # if we missed a scheduled initialization or schedule a future stop.
    if isinstance(binding.resourceConfig, ResourceConfigWithSchedule):
        assert isinstance(last_initialized, datetime)
        NOW = datetime.now(tz=UTC)
        cron_schedule = binding.resourceConfig.schedule
        missed_scheduled_initialization = next_fire(cron_schedule, last_initialized, NOW)
        future_scheduled_initialization = next_fire(cron_schedule, NOW)
        if missed_scheduled_initialization:
            task.log.info("Backfill completed after the next backfill was scheduled. Resetting the connector to keep adherence to the schedule.")
            task.stopping.event.set()
        elif future_scheduled_initialization:
            asyncio.create_task(scheduled_stop(task, future_scheduled_initialization))


async def _binding_incremental_task(
    binding: CaptureBinding[_ResourceConfig],
    binding_index: int,
    fetch_changes: FetchChangesFn[_BaseDocument],
    state: ResourceState.Incremental,
    task: Task,
    subtask_id: str | None = None,
):
    if subtask_id is not None:
        connector_state = ConnectorState(
            bindingStateV1={binding.stateKey: ResourceState(inc={subtask_id: state})}
        )
    else:
        connector_state = ConnectorState(
            bindingStateV1={binding.stateKey: ResourceState(inc=state)}
        )

    sleep_for = timedelta()

    task.log.info(
        "resuming incremental replication", {"state": state, "subtask_id": subtask_id}
    )

    if isinstance(state.cursor, datetime):
        lag = datetime.now(tz=UTC) - state.cursor

        if lag < binding.resourceConfig.interval:
            sleep_for = binding.resourceConfig.interval - lag
            task.log.info(
                "incremental task ran recently, sleeping until `interval` has fully elapsed",
                {
                    "sleep_for": sleep_for,
                    "interval": binding.resourceConfig.interval,
                    "subtask_id": subtask_id,
                },
            )

    while True:
        try:
            if not task.stopping.event.is_set():
                await asyncio.wait_for(
                    task.stopping.event.wait(), timeout=sleep_for.total_seconds()
                )

            task.log.debug(
                "incremental replication is idle and is yielding to stop",
                {"subtask_id": subtask_id},
            )
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
                task.log.info(
                    "incremental task triggered backfill", {"subtask_id": subtask_id}
                )
                task.stopping.event.set()
                await task.checkpoint(
                    ConnectorState(backfillRequests={binding.stateKey: True})
                )
                return
            else:
                # Ensure LogCursor types match
                # and that they're strictly increasing when appropriate.
                is_larger = False
                should_be_larger = True

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
                    should_be_larger = False
                else:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} of a different type than the last LogCursor {state.cursor}",
                    )

                if should_be_larger and not is_larger:
                    raise RuntimeError(
                        f"Implementation error: FetchChangesFn yielded LogCursor {item} which is not greater than the last LogCursor {state.cursor}",
                    )

                state.cursor = item
                await task.checkpoint(connector_state)
                checkpoints += 1
                pending = False

        if pending:
            raise RuntimeError(
                "Implementation error: FetchChangesFn yielded documents without a final LogCursor",
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
            "incremental task is idle",
            {"sleep_for": sleep_for, "cursor": state.cursor, "subtask_id": subtask_id},
        )
