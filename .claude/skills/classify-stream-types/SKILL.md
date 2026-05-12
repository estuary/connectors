---
name: classify-stream-types
description: Classify API endpoints into stream types (webhook, incremental, backfill, snapshot) and generate resource definitions for estuary-cdk connectors. Use after scaffolding a connector or when adding new streams.
argument-hint: "[connector-name]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch
---

Classify each API endpoint of `source-$ARGUMENTS` into the appropriate stream type and generate resource definitions. Read the connector's `api.py` and `resources.py` first, then research the provider's API docs.

## Decision flowchart

Evaluate each endpoint in order:

1. **Does the provider push events via HTTP?** → **Webhook** stream (via `WebhookCaptureSpec`). See `/create-webhook-connector` for setup.
2. **Does the endpoint support date-range or cursor-based filtering with a cursor that persists over time?** (e.g., `updated_at`, monotonic ID, event timestamp, sequence number — not just `created_at`) → **Incremental + Backfill**. Prefer endpoints that also support sorting, but filtering without document sorting is fine.
3. **No filtering, but supports sorting in reverse chronological order?** → **Incremental only**. Use `fetch_changes` to walk from the latest document backward to the cursor (initially `start_date`). Only yield a cursor checkpoint once the walk reaches the cursor — if interrupted mid-walk, the next invocation restarts from the top. After the initial catch-up, subsequent invocations only walk back to the last checkpoint.
4. **Is the dataset small with no change tracking?** → **Snapshot**.
5. **Large dataset, no filtering or sorting?** → Look for a different endpoint or ask the user.

**Always ask the user for confirmation before committing to an incremental-only approach (no backfill).** If there's any usable cursor, prefer incremental + backfill. Only fall back to incremental-only if the user explicitly confirms backfill isn't needed.

## Code patterns

### Incremental + Backfill

Reference: `source-sentry/source_sentry/resources.py` — `open_issue_binding` function.

```python
def open_binding_fn(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings,
):
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_changes=functools.partial(fetch_incremental, http, ...),
        fetch_page=functools.partial(backfill_historical, http, ...),
    )

# Initial state must set both inc and backfill:
# Convention: next_page=None signals "beginning of backfill".
# The fetch_page function handles None by falling back to start_date.
cutoff = datetime.now(tz=UTC)
initial_state = ResourceState(
    inc=ResourceState.Incremental(cursor=cutoff),
    backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
)
```

### Incremental only

Reference: `source-front/source_front/resources.py` — `incremental_resources_with_cursor_fields` function.

```python
common.open_binding(
    binding, binding_index, state, task,
    fetch_changes=functools.partial(fetch_changes_fn, http, ...),
)

initial_state = ResourceState(
    inc=ResourceState.Incremental(cursor=config.start_date),
)
```

### Snapshot

**Use `SnapshotResource`, not the generic `Resource`.**
Reference: `source-ashby/source_ashby/resources.py`

```python
from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    Task,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding

def open(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings,
) -> None:
    open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_snapshot=functools.partial(list_all, http, ...),
    )

resource = SnapshotResource(
    name=MyDocument.name,
    open=open,
    initial_config=ResourceConfig(name=MyDocument.name, interval=timedelta(minutes=5)),
)
```

### Webhook

Defer to `/create-webhook-connector` skill for webhook stream setup.

## Backfill design guidance

- `fetch_page(log, page_cursor, cutoff)` walks historical data from oldest to cutoff
- Page cursor tracks progress across the CDK's 24-hour periodic restart
- Each invocation fetches one page or time window, then yields a `PageCursor` for the next
- **Checkpoint every N wall-clock minutes** (e.g., 5 min): track elapsed time within `fetch_page` and yield a `PageCursor` checkpoint when the time budget is reached, even mid-sequence. This ensures progress is saved if the connector restarts.
- Return without yielding a `PageCursor` to signal completion
- The `cutoff` LogCursor marks where incremental replication takes over — suppress documents at or after the cutoff

## Workflow

1. Read the connector's `api.py` and `resources.py`
2. Research the provider's API docs (via WebFetch/WebSearch) to understand each endpoint's filtering, pagination, and cursor capabilities
3. For each endpoint, apply the decision flowchart above
4. Present the classification to the user for confirmation before generating code
5. Generate or modify resource definitions in `resources.py` and fetch functions in `api.py`
