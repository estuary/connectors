---
paths: ["source-*/**/*.py"]
---

# Connector Python — always applies

These are the rules where silence produces a bug, so they load whenever you open connector Python rather than waiting for a skill invocation. Longer-form rules live behind the triggers at the bottom.

- **Never `json.loads` an API response.** Define a Pydantic model and parse with `Model.model_validate_json(...)` — even for one-off auth or metadata endpoints. Untyped dict access hides schema drift until runtime.
- **Model a provider field the logic depends on as required**, never `getattr(obj, "field", None)`. A default silently swallows a response-shape change and the dependent logic quietly does nothing; a required field fails loudly at validation. The resulting breakage is the point.
- **Prefer `IncrementalJsonProcessor` / `IncrementalCsvProcessor`** over manually parsing responses. Feed it `body()` from `headers, body = await http.request_stream(log, url, params=params)`. Before your first use in a connector, read the class docstring at `estuary-cdk/estuary_cdk/incremental_json_processor.py` — it covers prefix semantics (`.item` for arrays, `""` for NDJSON), the keep-a-reference rule for `get_remainder()`, and points at its tests for complex prefixes. Not re-exported from `estuary_cdk/__init__.py`; import the full module path.
- **Document models subclass `BaseDocument` with `extra="allow"`.** Define only primary keys, cursors, and fields the code references — schema inference handles the rest.
- **Structured-log keys are snake_case** (`gated_campaigns`, `cache_evictions`). Match the connector's existing keys before inventing new ones.
- **Generics use PEP 695 syntax** (`class ListPage[ItemT](BaseModel)`, `def fetch[T](...)`, bounds via `[T: Bound]`) — never module-level `TypeVar` + `Generic[...]`. estuary-cdk's own `TypeVar` style is legacy, not the target. Exempt when `pyproject.toml` still declares `python = "^3.11"`.
- **Fetch function names are fixed:** `fetch_<stream>` (incremental), `backfill_<stream>` (backfill), `snapshot_<stream>` (snapshot). Helpers are unrestricted — a shared polymorphic engine belongs behind per-stream public functions named to this scheme.
- **`api.py` functions take narrow params, never the full config.** They receive only the fields they read (`region: str`, `start_date: datetime`); `resources.py` unpacks `EndpointConfig` into the `functools.partial` bindings. Resource-layer functions (`validate_credentials`, `all_resources`) may keep the full config.
- **Parameter order is `log`, `http`, `config`** where present. Exception: CDK fetch contracts append `log` and the cursor after the partial-bound params, so fetch functions are necessarily `fetch_x(http, ..., log, cursor)`. External contracts win.
- **Raise `ValidationError` for config problems; let `HTTPError` propagate** for API failures (the CDK handles retries). Never swallow an error.

## Read before you edit

- Writing or changing a `fetch_*` / `backfill_*` function → invoke the **`fetch-function-rules`** skill **first**. Four of its rules are data-loss-class (`FETCH-COMPLETE-TICKS`, `FETCH-SEAM-PRECISION`, `FETCH-VALUE-WATERMARK-RESUME`, `FETCH-CHECKPOINT-STABLE-STATE`) and none are inferable from the surrounding code.
- Child stream that needs a parent ID to list → **`child-entities`** skill.
- Choosing a replication strategy for a new stream → **`classify-stream-types`** skill.
- Calling the provider's API at all → [`.claude/shared/provider-api-consent.md`](../shared/provider-api-consent.md). Read-only calls are gated on `config.yaml` being clean and tracked; you never run a mutating call yourself.
