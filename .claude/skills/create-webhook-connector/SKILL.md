---
name: create-webhook-connector
description: Scaffold a new webhook-based capture connector using the estuary-cdk webhook framework. Use when the user wants to create a new source connector that receives webhook events.
argument-hint: "[provider-name]"
allowed-tools: Bash Read Write Edit Glob Grep
---

Create a new webhook-based capture connector named `source-$ARGUMENTS` using the estuary-cdk webhook framework. Read `source-appsflyer/` as the canonical reference before scaffolding. See `estuary-cdk/CLAUDE.md` for the standard connector layout.

## Supported discriminators

Choose ONE based on how the provider identifies event types:

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

## Event type discovery

There are two patterns for populating `known_values`:

### Pattern 1: API-discovered (preferred when an endpoint exists)

If the provider has an API to list available event types, fetch them in `all_resources()` and pass directly as `known_values`. See `source-appsflyer` for this pattern — it fetches from the AppsFlyer Push API and uses the response as-is.

```python
async def all_resources(log, http, config):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    event_types = await fetch_event_types(log, http)
    return WebhookCaptureSpec(
        discriminator=HeaderDiscriminator(key="X-Event-Type", known_values=event_types),
    ).create_resources()
```

### Pattern 2: Static with user override (when no discovery endpoint exists)

Define a predefined set of known event types and expose it in `EndpointConfig.Advanced` so users can modify it:

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

Pass whatever the user configured directly as `known_values`:

```python
async def all_resources(log, http, config):
    return WebhookCaptureSpec(
        discriminator=HeaderDiscriminator(
            key="X-Event-Type",
            known_values=config.advanced.event_types,
        ),
    ).create_resources()
```

## Constraints

- `known_values` must be non-empty for HeaderDiscriminator and BodyDiscriminator
- Accepts single JSON objects and arrays; if ANY document in an array fails to match, the entire request returns 404
- Schema inference is enabled by default — do not define rigid output schemas

## Key CDK files

- `estuary-cdk/estuary_cdk/capture/webhook/server.py` — WebhookCaptureSpec, WebhookResourceConfig
- `estuary-cdk/estuary_cdk/capture/webhook/match.py` — discriminators and match rules
- `estuary-cdk/estuary_cdk/capture/common.py` — WebhookDocument, Resource

## After scaffolding

Ask the user which discriminator strategy fits their webhook provider and whether event types can be discovered via API or need to be statically defined.

If the provider also has pull API endpoints (not just webhooks), use `/classify-stream-types` to determine the appropriate stream type for each endpoint.
