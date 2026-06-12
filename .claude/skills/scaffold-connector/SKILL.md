---
name: scaffold-connector
description: Scaffold the boilerplate of a new estuary-cdk pull capture connector — directory tree, packaging, the Connector class, an empty stream registry, the test harness, and a clearly-marked auth seam. Produces a connector that passes `flowctl raw spec`/`discover` with zero streams. Use as the first step of `create-capture-connector`, or any time you need a bare `source-*` skeleton before adding auth and streams.
argument-hint: "[provider-name]"
allowed-tools: Bash Read Write Edit Glob Grep
---

Scaffold the boilerplate for a new pull capture connector named `source-$1` using the estuary-cdk framework. The deliverable is a **runnable skeleton with zero streams and a stubbed auth seam** — not a finished connector. Auth is filled in by the `configure-auth` skill; streams are added by `classify-stream-types` + `add-stream`. Read `estuary-cdk/CLAUDE.md` for the canonical layout and `source-front/` as the reference connector before scaffolding.

This skill is designed to run **as its own subagent** so the orchestrator's context isn't polluted with file-creation noise. Its final message is a structured summary (see Output) that the caller consumes — keep that summary tight and machine-readable.

## Phase 0 — Reconnoiter

- Confirm `source-$1` does **not** already exist (`ls source-$1`). If it does, stop and surface it — scaffolding over an existing connector is destructive and out of scope.
- Derive the Python package name: `source-$1` → `source_<name>` with dashes replaced by underscores (e.g. `source-google-sheets-native` → `source_google_sheets_native`). Use this everywhere below; call it `$PKG`.
- Pick the reference connector. Default to `source-front/` (simple REST, single-token auth). If the orchestrator told you the provider uses OAuth, also skim `source-hubspot-native/` for the config-model shape — but still leave auth as a seam.
- Read `source-front/`'s skeleton files now: `pyproject.toml`, `VERSION`, `source_front/{__init__,__main__,models,resources,api}.py`, `test.flow.yaml`, `tests/test_snapshots.py`.

## Phase 1 — Directory & Packaging

Create the standard layout documented in `estuary-cdk/CLAUDE.md` § Connector Layout — `pyproject.toml`, `VERSION`, `test.flow.yaml`, `acmeCo/`, and the `$PKG/` package (`__init__`, `__main__`, `models`, `api`, `resources`). That doc is the canonical source for the tree; don't duplicate it. On top of the base layout, this skill also adds:

- `config.yaml` — plaintext placeholder (Phase 3).
- `tests/__init__.py` + `tests/test_snapshots.py` — the test harness (Phase 3).

**`pyproject.toml`** — copy `source-front/pyproject.toml`, changing only `name` (to `source-$1`) and `authors` (read the git user via `git config user.name`/`user.email`). Keep the `estuary-cdk = {path="../estuary-cdk", develop = true}` path dependency and the dev-group deps verbatim.

**`VERSION`** — `v1` (new connectors start at v1; existing ones increment).

## Phase 2 — Connector Skeleton

**`$PKG/__main__.py`**:

```python
import asyncio
import $PKG

asyncio.run($PKG.Connector().serve())
```

**`$PKG/__init__.py`** — the `Connector` class is boilerplate. Copy `source-front/source_front/__init__.py` verbatim, changing only the package-relative imports and `documentationUrl` (convention: `https://go.estuary.dev/source-$1`). It must define `spec`, `discover`, `validate`, and `open` exactly as the reference does.

**`$PKG/models.py`** — the minimal config + state, with the auth seam:

```python
from datetime import datetime, timedelta, UTC
from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
# AUTH SEAM: `configure-auth` replaces this import + the `credentials` field below
# with the scheme it picks (AccessToken / BasicAuth / OAuth2Credentials / ...).
from estuary_cdk.flow import AccessToken


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    # AUTH SEAM: scheme + fields owned by `configure-auth`.
    credentials: AccessToken = Field(
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]

# Stream document models and resource registries are added by `add-stream`.
```

**`$PKG/resources.py`** — empty registry plus a stubbed validator that still proves credentials wiring compiles:

```python
from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .models import EndpointConfig


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    # AUTH SEAM: `configure-auth` sets the token source and probes a cheap,
    # always-available endpoint, raising ValidationError on a 401.
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    raise NotImplementedError("configure-auth must implement validate_credentials")


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    # `add-stream` appends resources here. Empty until the first stream is added.
    return []
```

> Note the seam tension: `validate_credentials` raises `NotImplementedError`, but `discover`/`spec` (the smoke test in Phase 4) never call it — only `validate` does. That's intentional: the skeleton discovers cleanly with zero streams, and the loud `NotImplementedError` forces `configure-auth` to do its job rather than silently shipping a no-op validator.

**`$PKG/api.py`** — just the base URL and a place to grow:

```python
# Pure functions for provider API interaction live here; `add-stream` fills them in.
API = "https://api.example.com"  # AUTH SEAM-adjacent: set to the provider's base URL.
```

Set `API` to the provider's real base URL if you already know it from Phase 0; otherwise leave the placeholder and note it in the Output.

## Phase 3 — Test Harness

**`test.flow.yaml`** — the minimal, binding-free shape (matches what `regenerate-flow-discovery` resets to). No `import: acmeCo/flow.yaml` line yet — that file doesn't exist until discovery runs.

```yaml
captures:
  acmeCo/source-$1:
    endpoint:
      local:
        command:
          - python
          - "-m"
          - $PKG
        config: config.yaml
    bindings: []
```

**`tests/__init__.py`** — empty file.

**`tests/test_snapshots.py`** — copy `source-front/tests/test_snapshots.py` verbatim (the three `test_spec` / `test_discover` / `test_capture` functions are connector-agnostic).

**`config.yaml`** — plaintext placeholder, enough for `EndpointConfig` to parse (Law #6):

```yaml
credentials:
  access_token: PLACEHOLDER_REPLACE_WITH_REAL_TOKEN
start_date: "2024-01-01T00:00:00Z"
```

Adjust the `credentials` shape only if Phase 0 already established a non-bearer scheme; otherwise leave it and let `configure-auth` reshape it. Flag in the Output that this file holds no real credentials.

## Phase 4 — Environment & Smoke Test

From the connector directory:

```bash
python -m venv .venv && source .venv/bin/activate
poetry install
```

Then run the two **free** checks:

```bash
poetry run flowctl raw spec --source test.flow.yaml -o json --emit-raw
poetry run flowctl raw discover --source test.flow.yaml -o json --emit-raw
```

- `spec` must emit the config schema (with the `start_date` + `credentials` fields).
- `discover` must succeed and emit **zero bindings** (the empty registry). It will not write any schema files yet.

If either fails with `ModuleNotFoundError`, the venv isn't active — `source "$(poetry env info --path)/bin/activate"` and retry (per `regenerate-flow-discovery` Law #1). If `spec` fails on a Pydantic/import error, fix the skeleton before handing off.

Do **not** run `pytest` to create snapshots here — there are no streams to snapshot.

## Output

End with a structured summary the orchestrator can act on:

```
scaffold-connector: source-$1
- package: $PKG
- files created: <list>
- reference copied from: source-front (cite any deviations + why)
- API base URL: <set | PLACEHOLDER — needs setting>
- spec: PASS/FAIL   discover (0 bindings): PASS/FAIL
- AUTH SEAM locations:
    models.py: credentials field + import
    resources.py: validate_credentials body + token_source
    config.yaml: placeholder credentials
- NEXT: run `configure-auth source-$1`, then `classify-stream-types` / `add-stream` per stream/cluster.
```

## Out of scope

- Auth scheme selection and `validate_credentials` implementation → `configure-auth`.
- Any stream: document models, fetch functions, resource registration → `classify-stream-types` + `add-stream`.
- Webhook-receiver connectors → `create-webhook-connector` (which calls this skill for the shared skeleton, then does webhook-specific setup).
- Real credentials, sops encryption, and any live API call.
