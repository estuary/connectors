# Prices Stream Implementation

This document describes the implementation of the Prices stream for `source-stripe-native`, including the infrastructure changes required to support it.

## Problem Statement

### Stripe API Limitation

The Stripe `/v1/prices` API has a limitation: it requires an `active` parameter to filter prices by status:
- `active=true` (default): Returns only active prices
- `active=false`: Returns only inactive prices

**There is no way to fetch both active and inactive prices in a single API request.**

This creates a challenge for backfilling prices data, because we need complete coverage of all prices regardless of their active status.

### Incremental Replication Works Differently

For incremental replication via the `/v1/events` endpoint, this limitation doesn't apply. Events for `price.created`, `price.updated`, and `price.deleted` contain the full price object regardless of its active status. A single incremental task can capture all price changes.

### The Mismatch

This creates an asymmetry between incremental and backfill tasks:
- **Incremental**: 1 task per account (events contain both active and inactive prices)
- **Backfill**: 2 tasks per account (separate API calls for active vs inactive)

The existing `open_binding_with_priority_queue` infrastructure assumed a 1:1 mapping between accounts and backfill tasks, which doesn't work for Prices.

## Solution Overview

### Two-Part Implementation

1. **Extend `priority_capture.py`**: Add support for multiple backfill subtasks per incremental subtask via a new `backfill_subtask_suffixes` parameter.

2. **Implement Prices stream**: Use the extended infrastructure for connected accounts mode, and the standard CDK subtask dict pattern for single account mode.

### State Structure

For connected accounts mode, the state uses composite keys for backfill:

```python
# Incremental state: keyed by account_id (unchanged)
state.inc = {
    "acct_123": Incremental(cursor=...),
    "acct_456": Incremental(cursor=...),
}

# Backfill state: keyed by composite keys (account_id + suffix)
state.backfill = {
    "acct_123_active": Backfill(next_page=..., cutoff=...),
    "acct_123_inactive": Backfill(next_page=..., cutoff=...),
    "acct_456_active": Backfill(next_page=..., cutoff=...),
    "acct_456_inactive": Backfill(next_page=..., cutoff=...),
}
```

For single account mode, the standard CDK subtask pattern is used:

```python
state.inc = Incremental(cursor=...)
state.backfill = {
    "_active": Backfill(next_page=..., cutoff=...),
    "_inactive": Backfill(next_page=..., cutoff=...),
}
```

---

## Commit 1: Extend Priority Capture Infrastructure

### Motivation

The `open_binding_with_priority_queue` function manages concurrent capture from multiple Stripe connected accounts. It uses a priority queue to fairly distribute work across accounts, prioritizing those that are furthest behind (i.e., still backfilling).

The existing implementation assumed each account has exactly one backfill task. To support Prices (and potentially other streams with similar requirements in the future), we need to support multiple backfill tasks per account.

### Changes to `priority_capture.py`

#### 1. New Protocol: `FetchPageFnFactoryWithSuffix`

```python
class FetchPageFnFactoryWithSuffix(Protocol):
    """Factory function for creating backfill fetch functions with suffix support.

    Unlike the standard FetchPageFnFactory which only takes account_id,
    this factory also takes a suffix to create different fetch functions
    for different backfill variants (e.g., "_active" vs "_inactive" for Prices).
    """
    def __call__(self, account_id: str, suffix: str) -> Optional[Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]]]:
        ...
```

This new protocol allows the factory to create different fetch functions based on both the account ID and a suffix. For Prices, the suffix determines whether to fetch active or inactive prices.

#### 2. New Dataclass: `BackfillVariant`

```python
@dataclass
class BackfillVariant:
    """Represents a single backfill variant (e.g., active prices, inactive prices).

    Each variant has its own:
    - suffix: Used to construct the composite state key
    - fetch_fn: The actual fetch function for this variant
    - state: The backfill state tracking pagination progress
    """
    suffix: str  # e.g., "_active", "_inactive", or "" for single-backfill mode
    fetch_fn: Union[FetchPageFn[BaseDocument], RecurringFetchPageFn[BaseDocument]]
    state: ResourceState.Backfill

    def state_key(self, account_id: str) -> str:
        """Returns the composite state key for this backfill variant."""
        return f"{account_id}{self.suffix}"
```

This dataclass encapsulates everything needed to execute a single backfill variant. The `state_key` method generates the composite key used to track this variant's progress in the connector state.

#### 3. Modified `WorkItem`

Before:
```python
@dataclass
class WorkItem:
    account_id: str
    priority: WorkItemPriority
    incremental_fetch_fn: FetchChangesFn[BaseDocument]
    incremental_state: ResourceState.Incremental
    backfill_fetch_fn: Optional[...]  # Single backfill
    backfill_state: Optional[ResourceState.Backfill]  # Single state
```

After:
```python
@dataclass
class WorkItem:
    account_id: str
    priority: WorkItemPriority
    incremental_fetch_fn: FetchChangesFn[BaseDocument]
    incremental_state: ResourceState.Incremental
    backfill_variants: list[BackfillVariant] = field(default_factory=list)  # Multiple variants
    cancellation_event: asyncio.Event = field(default_factory=asyncio.Event)

    def has_backfill_work(self) -> bool:
        """Returns True if there are any incomplete backfill variants."""
        return len(self.backfill_variants) > 0
```

The `WorkItem` now holds a list of `BackfillVariant` objects instead of a single backfill function and state.

#### 4. Modified `PriorityCalculator`

The priority calculator determines which accounts should be processed and in what order. It was updated to:

- Accept an optional `backfill_subtask_suffixes` parameter
- Check composite state keys when determining if an account needs backfilling
- Count actual incomplete backfill variants when calculating subtask counts

Key changes:

```python
def get_account_priority(self, account_id: str) -> WorkItemPriority:
    # When suffixes provided: check if ANY composite key has incomplete backfill
    if self.backfill_subtask_suffixes:
        needs_backfill = any(
            isinstance(self.resource_state.backfill.get(f"{account_id}{suffix}"), ResourceState.Backfill)
            for suffix in self.backfill_subtask_suffixes
        )
    else:
        needs_backfill = isinstance(self.resource_state.backfill.get(account_id), ResourceState.Backfill)

    if needs_backfill:
        return WorkItemPriority.BACKFILL
    return WorkItemPriority.INCREMENTAL
```

#### 5. Modified `WorkItemManager`

The work item manager creates `WorkItem` objects for accounts. The `_get_backfill_work` method was completely rewritten:

Before: Returns a single `(fetch_fn, state)` tuple or `None`

After: Returns a `list[BackfillVariant]`

```python
def _get_backfill_work(self, account_id: str) -> list[BackfillVariant]:
    """Get all incomplete backfill variants for an account."""
    variants = []

    if self.backfill_subtask_suffixes:
        # Multi-backfill mode: check each suffix
        for suffix in self.backfill_subtask_suffixes:
            state_key = f"{account_id}{suffix}"
            backfill_state = self.resource_state.backfill.get(state_key)

            if isinstance(backfill_state, ResourceState.Backfill):
                # Create fetch function using the suffix-aware factory
                fetch_fn = self.fetch_page_factory(account_id, suffix)
                if fetch_fn:
                    variants.append(BackfillVariant(
                        suffix=suffix,
                        fetch_fn=fetch_fn,
                        state=backfill_state,
                    ))
    else:
        # Single-backfill mode (existing behavior)
        # ... returns single variant with empty suffix

    return variants
```

#### 6. Modified `SubtaskExecutor`

The executor spawns async tasks for each work item. It was updated to spawn multiple backfill tasks:

```python
async def execute_work_item(self, work_item: WorkItem, task: Task):
    subtasks: list[asyncio.Task] = []

    # Spawn a backfill task for EACH variant
    for variant in work_item.backfill_variants:
        state_key = variant.state_key(work_item.account_id)
        backfill_task = task.spawn_child(
            f"backfill.{state_key}",  # e.g., "backfill.acct_123_active"
            lambda t, v=variant: self._execute_backfill_variant(work_item, v, t)
        )
        subtasks.append(backfill_task)

    # Spawn single incremental task (unchanged)
    incremental_task = task.spawn_child(
        f"incremental.{work_item.account_id}",
        lambda t: self._execute_incremental_subtask(work_item, t)
    )
    subtasks.append(incremental_task)

    await asyncio.gather(*subtasks)
```

#### 7. New Function: `_binding_backfill_task_with_variant`

A new helper function that executes a single backfill variant and checkpoints using the composite state key:

```python
async def _binding_backfill_task_with_variant(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    variant: BackfillVariant,
    account_id: str,
    last_initialized: datetime | None,
    is_connector_initiated: bool,
    task: Task,
    work_item: WorkItem,
):
    state_key = variant.state_key(account_id)  # e.g., "acct_123_active"

    # Checkpoint uses the composite key
    connector_state = ConnectorState(
        bindingStateV1={
            binding.stateKey: ResourceState(backfill={state_key: state})
        }
    )

    # ... execute backfill pages, checkpoint progress ...

    # Mark complete with None
    await task.checkpoint(
        ConnectorState(
            bindingStateV1={
                binding.stateKey: ResourceState(backfill={state_key: None})
            }
        )
    )
```

#### 8. Modified `open_binding_with_priority_queue` Signature

```python
def open_binding_with_priority_queue(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_account_ids: list[str],
    fetch_changes_factory: FetchChangesFnFactory,
    fetch_page_factory: Union[FetchPageFnFactory, FetchPageFnFactoryWithSuffix],  # Now accepts both
    fetch_snapshot: Optional[FetchSnapshotFn[BaseDocument]] = None,
    tombstone: Optional[BaseDocument] = None,
    config: Optional[PriorityQueueConfig] = None,
    backfill_subtask_suffixes: list[str] | None = None,  # NEW PARAMETER
):
```

When `backfill_subtask_suffixes` is `None`, the existing single-backfill behavior is preserved (backward compatible).

### Backward Compatibility

All changes are backward compatible:
- `backfill_subtask_suffixes=None` preserves existing behavior
- Existing streams continue to work without modification
- The single-backfill mode internally uses an empty string suffix `""`

---

## Commit 2: Implement Prices Stream

### Changes to `models.py`

Added the `Prices` model class:

```python
class Prices(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Prices"
    SEARCH_NAME: ClassVar[str] = "prices"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "price.created": "c",
        "price.updated": "u",
        "price.deleted": "d",
    }
```

Added `Prices` to the `STREAMS` list (after `Products`, since they're related).

### Changes to `api.py`

Added `fetch_backfill_prices` function:

```python
async def fetch_backfill_prices(
    cls: type[Prices],
    start_date: datetime,
    platform_account_id: str | None,
    connected_account_id: str | None,
    active: bool,  # KEY DIFFERENCE: filters by active status
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Prices | str, None]:
    """
    Variant of fetch_backfill specifically for Prices.

    The Stripe Prices API requires an `active` parameter to filter by
    active/inactive status. There is no way to fetch both active and
    inactive prices in a single request, so we need separate backfill
    tasks for each.
    """
    # ... same structure as fetch_backfill ...

    parameters["active"] = "true" if active else "false"  # The key addition

    # ... pagination and yielding logic ...
```

This function is identical to `fetch_backfill` except it adds the `active` query parameter.

### Changes to `resources.py`

#### 1. New Constant

```python
# Backfill suffixes for Prices stream
PRICES_BACKFILL_SUFFIXES = ["_active", "_inactive"]
```

#### 2. New Function: `_create_prices_initial_state`

Creates the initial state structure with the appropriate keys:

```python
def _create_prices_initial_state(account_ids: str | list[str]) -> ResourceState:
    cutoff = datetime.now(tz=UTC)

    if isinstance(account_ids, str):
        # Single account mode - use subtask dict (keyed by suffix only)
        initial_state = ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill={
                suffix: ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for suffix in PRICES_BACKFILL_SUFFIXES
            },
        )
    else:
        # Connected accounts mode - use composite keys
        initial_state = ResourceState(
            inc={
                account_id: ResourceState.Incremental(cursor=cutoff)
                for account_id in account_ids
            },
            backfill={
                f"{account_id}{suffix}": ResourceState.Backfill(next_page=None, cutoff=cutoff)
                for account_id in account_ids
                for suffix in PRICES_BACKFILL_SUFFIXES
            },
        )

    return initial_state
```

#### 3. New Function: `_reconcile_prices_state`

Handles state reconciliation when new connected accounts are discovered:

```python
async def _reconcile_prices_state(
    account_ids: list[str],
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
):
    """
    Reconciles state for newly discovered connected accounts for Prices stream.
    Similar to _reconcile_connector_state but handles composite backfill keys.
    """
    # For each account, check if state exists
    # If not, initialize both _active and _inactive backfill states
```

#### 4. New Function: `prices_object`

The main factory function that creates the Prices resource:

```python
def prices_object(
    http: HTTPSession,
    start_date: datetime,
    platform_account_id: str,
    connected_account_ids: list[str],
    all_account_ids: list[str],
) -> Resource:
    initial_state = _create_prices_initial_state(
        all_account_ids if connected_account_ids else platform_account_id
    )

    async def open(binding, binding_index, state, task, all_bindings):
        if not connected_account_ids:
            # SINGLE ACCOUNT MODE
            # Use standard open_binding with subtask dict for fetch_page
            fetch_changes_fn = functools.partial(fetch_incremental, Prices, ...)
            fetch_page_fns = {
                "_active": functools.partial(fetch_backfill_prices, ..., active=True, ...),
                "_inactive": functools.partial(fetch_backfill_prices, ..., active=False, ...),
            }
            open_binding(binding, binding_index, state, task,
                fetch_changes=fetch_changes_fn,
                fetch_page=fetch_page_fns,
            )
        else:
            # CONNECTED ACCOUNTS MODE
            # Use priority queue with multi-backfill support
            await _reconcile_prices_state(all_account_ids, binding, state, initial_state, task)

            def fetch_changes_factory(account_id):
                return functools.partial(fetch_incremental, Prices, platform_account_id, account_id, http)

            def fetch_page_factory_with_suffix(account_id, suffix):
                active = suffix == "_active"
                return functools.partial(fetch_backfill_prices, Prices, start_date,
                    platform_account_id, account_id, active, http)

            open_binding_with_priority_queue(
                binding, binding_index, state, task, all_account_ids,
                fetch_changes_factory=fetch_changes_factory,
                fetch_page_factory=fetch_page_factory_with_suffix,
                backfill_subtask_suffixes=PRICES_BACKFILL_SUFFIXES,
            )

    return Resource(name=Prices.NAME, key=["/id"], model=Prices, open=open, ...)
```

#### 5. Modified `all_resources`

Added special handling for Prices in the stream processing loop:

```python
if is_accessible_stream:
    # Prices requires special handling due to multi-backfill requirements
    if base_stream == Prices:
        resource = prices_object(http, config.start_date, platform_account_id,
            connected_account_ids, all_account_ids)
    elif hasattr(base_stream, "EVENT_TYPES"):
        resource = base_object(...)
    else:
        resource = no_events_object(...)
```

---

## Design Decisions

### Why Two Different Patterns?

**Single account mode** uses the standard CDK `open_binding` with a subtask dict because:
- It's the established pattern for subtasks in the CDK
- No need for priority queue complexity with just one account
- Consistent with how other connectors handle similar situations

**Connected accounts mode** uses `open_binding_with_priority_queue` with multi-backfill support because:
- Need to fairly distribute work across potentially thousands of accounts
- Priority queue ensures accounts that are behind get processed first
- The infrastructure already handles cancellation, checkpointing, and worker management

### Why Composite Keys?

Using composite keys like `"acct_123_active"` instead of nested structures:
- Flat key space is simpler to reason about
- Consistent with existing state serialization patterns
- Easy to identify which variant a state entry belongs to
- Avoids complexity of nested dict merging during checkpoints

### Backward Compatibility

The implementation is fully backward compatible:
- Existing streams work without modification
- `backfill_subtask_suffixes=None` preserves single-backfill behavior
- State format changes only affect Prices stream

---

## Testing

### Verified Behavior

1. **Discovery**: Prices stream appears in discovery output with correct schema
2. **Single account capture**: Successfully captures all 9 prices from test account
3. **Active/inactive separation**: Both `_active` and `_inactive` subtasks execute
4. **Existing tests pass**: No regressions in other streams

### Test Commands

```bash
# Run discovery
flowctl raw discover --source test.flow.yaml

# Run capture tests with snapshot updates
poetry run pytest --insta=update

# Manual verification with only Prices binding
flowctl preview --source test-prices.flow.yaml
```

---

## Future Considerations

The multi-backfill infrastructure added in this PR could be useful for other streams that have similar API limitations:
- Any Stripe resource that requires filtering by status/type in separate requests
- Resources with other mutually exclusive query parameters

The pattern is generic: any stream can use `backfill_subtask_suffixes` to split its backfill into multiple parallel subtasks, each with its own pagination state.
