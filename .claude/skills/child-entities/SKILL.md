# Child Entity Pattern for estuary-cdk Connectors

Use this when an API endpoint requires a parent ID to list child records, AND those children need independent incremental sync per parent (separate cursor, composite key including the parent ID). The canonical reference is `source-posthog`.

If the children can instead be re-snapshotted in full on every interval, use the much simpler snapshot-child approach in `source-ashby` (`snapshot_child_entity` in `source-ashby/source_ashby/api.py`) and ignore the rest of this file.

## Before You Start

Check whether the parent ID is already in the child's response body. If it is, you don't need context-injection machinery. This pattern is specifically for when the parent ID is absent from the response and must be stamped onto each child document during validation.

## Drain parents first, with the minimum data possible

Before iterating to fetch children, drain the parent stream into a list of **only** the fields you actually need downstream — typically just `id`, plus any filter fields like `updated_at`. Do **not** interleave streaming a parent response with per-parent child requests. Holding a parent HTTP response open while fetching children for each parent leaves the parent connection idle long enough to trigger timeout errors. Don't buffer the full parent records — define a small dataclass or extract just `parent.id` (and the filter fields you need) to save on memory.

**Reject empty parent IDs at the drain step.** An empty parent ID templates a malformed child path (e.g. `parents//children`) that typically returns nothing, silently orphaning every child instead of failing. Reject only the empty string, not every falsy value: integer IDs can legitimately be `0`.

## Pattern

### 1. Validation-context dataclass (`models.py`)

A frozen dataclass carries the parent ID through Pydantic's validation context. See `source-posthog/source_posthog/models.py:39-43`:

```python
@dataclass(frozen=True, slots=True)
class ProjectIdValidationContext:
    """Validation context carrying the project_id for document construction."""
    project_id: int
```

### 2. Mixin that extends `_meta` and injects from context (`models.py`)

Extend `BaseDocument.Meta` with the parent-id field, then use `@model_validator(mode="before")` to copy it from the context into `_meta` before field validation runs. See `source-posthog/source_posthog/models.py`:

```python
class ProjectScopedMixin(BaseDocument):
    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)
        project_id: int = Field(default=-1, description="...")

    meta_: Meta = Field(default_factory=Meta, alias="_meta", description="Document metadata")

    @model_validator(mode="before")
    @classmethod
    def _inject_project_id_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, ProjectIdValidationContext):
            meta = data.get("_meta") or {}
            meta["project_id"] = info.context.project_id
            data["_meta"] = meta
        return data
```

Child entities inherit from this mixin (`HogQLEntity(ProjectScopedMixin, BasePostHogEntity[T])`, `models.py`).

### 3. Fetch functions pass the context through (`api.py`)

Two call shapes appear in source-posthog:

**Direct `model_validate` with context** (when you already have a dict, e.g. HogQL columns → values), see `api.py:219-223`:

```python
async for row in processor:
    yield model.model_validate(
        dict(zip(column_names, row.root, strict=True)),
        context=ProjectIdValidationContext(project_id),
    )
```

**Streaming through `IncrementalJsonProcessor(..., validation_context=ctx)`**, see `api.py:79-91` and `api.py:244-250`:

```python
ctx = ProjectIdValidationContext(project_id=project_id)
async for item in _fetch_from_url(url, FeatureFlag, http, log, validation_context=ctx):
    ...
```

`IncrementalJsonProcessor` accepts a `validation_context` kwarg that it forwards into each item's Pydantic validation, so the injector fires as documents stream in.

### 4. Resource with per-parent cursors and composite key (`resources.py`)

One cursor per parent: build a dict of `f"{parent_id}"` → `functools.partial(fetcher, ..., parent_id)`, and pass the whole dict as `fetch_changes` to `open_binding`. See `resources.py:372-424`:

```python
incremental_fetchers = {
    f"{project_id}": functools.partial(fetch_feature_flags, http, config, project_id)
    for project_id in project_ids
}
backfill_fetchers = {
    f"{project_id}": functools.partial(backfill_feature_flags, http, config, project_id)
    for project_id in project_ids
}

async def open(binding, binding_index, state, task, all_bindings):
    await _patch_missing_project_states(binding, state, task, project_ids, cutoff)
    open_binding(binding, binding_index, state, task,
                 fetch_changes=incremental_fetchers, fetch_page=backfill_fetchers)

return PostHogResource(
    name=FeatureFlag.resource_name,
    key=["/_meta/project_id", "/id"],
    model=FeatureFlag,
    open=open,
    initial_state=_generate_resource_state(project_ids, cutoff),
    initial_config=ResourceConfig(name=FeatureFlag.resource_name, interval=timedelta(minutes=5)),
    schema_inference=True,
)
```

Two things to notice:

- **Key is composite**: `["/_meta/project_id", "/id"]`.
- **Newly-added parents between runs**: `_patch_missing_project_states` adds cursor state entries for projects that appear after the capture first opened. Without this, a new project would have no cursor slot and be silently skipped. Mirror this if parents can be added after initial sync.

## When to pick this vs. the snapshot-child shortcut

|                       | Incremental (source-posthog)                                                  | Snapshot (source-ashby)                                      |
| --------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Cursor                | One per parent, persisted in `ResourceState`                                  | None — re-fetch everything on each interval                  |
| Parent ID on doc      | Yes, injected into `_meta` via context + `@model_validator`                   | No — only sent in request body                               |
| Key                   | `["/_meta/<parent_id_field>", "/id"]`                                         | `["/_meta/row_id"]`                                          |
| Good for              | Large/mutable child sets, deletions not important, per-parent backfill wanted | Small stable child sets where periodic full-refresh is cheap |
| New parents appearing | Needs `_patch_missing_project_states`-style handling                          | Handled automatically — parent iteration picks them up       |
