---
name: create-webhook-connector
description: Create a new webhook-based capture connector using the estuary-cdk webhook framework. Reuses scaffold-connector for the shared skeleton and configure-auth for any outbound auth, then adds the webhook-specific wiring (discriminator, event-type discovery, WebhookResource, Dockerfile). Use when the provider pushes events to a webhook receiver.
argument-hint: "[provider-name]"
allowed-tools: Bash Read Write Edit Glob Grep Skill
---

Create a webhook-based capture connector `source-$1` using the estuary-cdk webhook framework. A webhook connector runs on the **same `BaseCaptureConnector`** as a pull connector — so the packaging, `Connector` class, models, and test harness are the shared skeleton — plus webhook-specific wiring in `resources.py`, a resource-config discriminated union, event-type discovery, and a Dockerfile symlink. Read `source-appsflyer/` (a webhook **+** pull hybrid) as the canonical reference.

## Phase 1 — Shared skeleton (delegate to scaffold-connector)

Invoke the `scaffold-connector` skill for `source-$1` (it can run as a subagent to keep boilerplate noise out of your context). That produces the `pyproject.toml`, package layout, `__main__.py`, `EndpointConfig`/`ConnectorState`, test harness, and `config.yaml` — all reusable as-is. You'll **replace** the pull-oriented `resources.py`/`api.py` stubs and adjust `__init__.py` in Phase 3, and the scaffold leaves an AUTH SEAM that Phase 2 resolves.

## Phase 2 — Auth (conditional)

Webhook receivers authenticate **inbound** (the provider POSTs to us), so a pure receiver makes **no outbound calls and needs no credentials**. Decide:

- **No outbound calls** (static event types, no pull endpoints) → there's no auth to configure. Remove the AUTH SEAM scaffold left: drop the `credentials` field from `EndpointConfig`, and delete `validate_credentials` / the `token_source` line from `resources.py`. `EndpointConfig` may end up carrying only the event-type `Advanced` config (Phase 3).
- **Outbound calls needed** — the connector fetches event types from a discovery API (Phase 3, Pattern 1) and/or has pull endpoints — → run the `configure-auth` skill normally to wire the scheme. `all_resources` will set `http.token_source` for those calls.

## Phase 3 — Webhook wiring

This is the part unique to webhook connectors. Replace the scaffold's `resources.py` stub with the webhook resource set.

### Choose a discriminator

Pick ONE based on how the provider identifies event types:

**HeaderDiscriminator** — event type is in an HTTP header (highest routing priority):

```python
from estuary_cdk.capture.webhook.match import HeaderDiscriminator
HeaderDiscriminator(key="X-Event-Type", known_values=event_types)
```

**BodyDiscriminator** — event type is in the JSON body (supports dot-paths like `event.type`):

```python
from estuary_cdk.capture.webhook.match import BodyDiscriminator
BodyDiscriminator(key="event.type", known_values=event_types)
```

**UrlDiscriminator** — different events hit different URL paths:

```python
from estuary_cdk.capture.webhook.match import UrlDiscriminator
UrlDiscriminator(known_values={"/installs", "/events/{type}"})
```

No discriminator (or empty `UrlDiscriminator`) creates a single catch-all collection.

**Routing priority:** Header > Body > URL (more literal segments first) > URL wildcard `*`

### Populate `known_values` (event-type discovery)

**Pattern 1: API-discovered (preferred when an endpoint exists).** Fetch the event types in `all_resources()` and pass them as `known_values`. Requires outbound auth (Phase 2). See `source-appsflyer`, which fetches from the AppsFlyer Push API.

```python
async def all_resources(log, http, config):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    event_types = await fetch_event_types(log, http)
    return WebhookCaptureSpec(
        discriminator=HeaderDiscriminator(key="X-Event-Type", known_values=event_types),
    ).create_resources()
```

**Pattern 2: Static with user override (no discovery endpoint).** Define the known set and expose it in `EndpointConfig.Advanced` so users can extend it:

```python
KNOWN_EVENT_TYPES = {"install", "uninstall", "in-app-event", "re-engagement"}

class EndpointConfig(BaseModel):
    class Advanced(BaseModel):
        event_types: set[str] = Field(
            title="Event types",
            description="Event types to accept as valid resources",
            default=KNOWN_EVENT_TYPES,
        )

    advanced: Annotated[
        Advanced,
        Field(
            title="Advanced Config",
            description="Advanced settings for the connector.",
            default_factory=Advanced,
            json_schema_extra={"advanced": True, "order": 1},
        ),
    ]
```

```python
async def all_resources(log, http, config):
    return WebhookCaptureSpec(
        discriminator=HeaderDiscriminator(
            key="X-Event-Type",
            known_values=config.advanced.event_types,
        ),
    ).create_resources()
```

### Resource config & spec

- **Pure webhook:** `spec()`'s `resourceConfigSchema` uses `WebhookResourceConfig`.
- **Hybrid (webhook + pull):** define a discriminated resource-config union on a `type` field and use a `TypeAdapter` for the schema — see `source-appsflyer`'s `AnyResourceConfig = Annotated[PullApiResourceConfig | …WebhookResourceConfig, Field(discriminator="type")]` and `AnyResourceConfigAdapter`. The lower-level `WebhookResource` + `open_webhook_binding` + `discriminator.create_match_rules()` path (rather than `WebhookCaptureSpec`) is what mixes webhook and pull resources in one `all_resources`.

### Constraints

- `known_values` must be non-empty for HeaderDiscriminator and BodyDiscriminator.
- Accepts single JSON objects and arrays; if ANY document in an array fails to match, the entire request returns 404.
- Schema inference is enabled (`schema_inference=True`) — do **not** define rigid output schemas.

### Key CDK files

- `estuary-cdk/estuary_cdk/capture/webhook/server.py` — `WebhookCaptureSpec`, `WebhookResourceConfig`
- `estuary-cdk/estuary_cdk/capture/webhook/resources.py` — `WebhookResource`, `open_webhook_binding`
- `estuary-cdk/estuary_cdk/capture/webhook/match.py` — discriminators and match rules
- `estuary-cdk/estuary_cdk/capture/common.py` — `WebhookDocument`, `Resource`

## Phase 4 — Dockerfile symlink

Webhook connectors need a `Dockerfile` symlink pointing at the shared webhook Dockerfile (CI and `build-local.sh` auto-detect connector-specific Dockerfiles):

```bash
cd source-$1
ln -s ../estuary-cdk/webhook-capture.Dockerfile Dockerfile
```

## Phase 5 — Pull endpoints (hybrid only)

If the provider also has pull API endpoints, classify and add them as ordinary streams with `classify-stream-types` + `add-stream` (they coexist with the webhook resources via the `AnyResourceConfig` union). For a whole connector built mostly of pull streams with a webhook side, consider driving the build from `create-capture-connector` and treating the webhook setup here as one cluster's integration.

## Smoke test

`flowctl raw spec` / `flowctl raw discover` (free, no API) should emit the config schema and the webhook resource(s). Don't run a live capture — webhook delivery is exercised by the provider POSTing to a deployed receiver, out of scope here.
