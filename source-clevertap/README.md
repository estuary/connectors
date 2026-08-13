# source-clevertap — native Estuary capture connector

A **pull** connector for CleverTap, written against Estuary's Python SDK
(`estuary-cdk`). Estuary runs it in their hosted cloud on a schedule, so there is
**no VM / cron / push script to operate** — this replaces the
`push_clevertap.py` + `source-http-ingest` design in `../clevertap_estuary`.

It produces the same two collections as the Fivetran connector, with the same
keys and upsert (dedupe) behavior:

- `events_data` — key `/event_id` (SHA-256 of `clevertapId|ts|event_name|properties`)
- `profile_data` — key `/clevertapId`

## How it works

```
events_data binding (DRIVER)
  └─ fetch_changes: one CleverTap scan (POST cursor → GET-paginate next_cursor)
       ├─ yield EventsData            ──▶ events_data collection
       └─ yield AssociatedDocument(ProfileData, profile_binding_index)
                                       ──▶ profile_data collection
profile_data binding (SINK)
  └─ noop_fetch: yields nothing; only receives the AssociatedDocuments above
```

`AssociatedDocument` (from `estuary_cdk.capture.document`) lets one binding emit
documents into another, so both tables come from a **single** CleverTap scan — no
duplicate API traffic for profiles.

**Scheduling:** each binding's `ResourceConfig.interval` is `timedelta(days=1)`,
and the incremental `LogCursor` is a datetime. After each run the cursor advances
to "now", and the runtime sleeps ~24h before re-invoking — i.e. a daily pull with
no external scheduler. `lookback_days` (default 1) re-pulls recent days to catch
late-arriving data; the deterministic `event_id` makes those re-pulls upsert.

> `events_data` is the driver and must stay enabled — disabling it stops profiles too.

## Config (`advanced`)

| Field | Default | Purpose |
|---|---|---|
| `start_date` | 2 days ago | YYYY-MM-DD initial cursor for the first sync |
| `end_date` | _(none)_ | YYYY-MM-DD cap for a bounded historical backfill; omit for continuous daily pulls |
| `lookback_days` | 1 | Re-pull this many days before the cursor each run (late data; deduped by `event_id`) |
| `skip_invalid_events` | false | If true, an event name CleverTap rejects as invalid is logged and skipped instead of failing the whole capture. If false, an invalid event fails the sync loudly. |

## Files

| File | Purpose |
|---|---|
| `source_clevertap/models.py` | `EndpointConfig`, `EventsData`, `ProfileData` |
| `source_clevertap/api.py` | CleverTap fetch + transforms (ported from `push_clevertap.py`) |
| `source_clevertap/resources.py` | `all_resources()` — the two bindings + cross-binding wiring |
| `source_clevertap/__init__.py` | `Connector` lifecycle (spec/discover/validate/open) |
| `source_clevertap/__main__.py` | entrypoint (`Connector().serve()`) |
| `test.flow.yaml` | local dev catalog (runs via your Python env, no image) |

## Local development

The connector expects `estuary-cdk` as a sibling directory (matching every
connector in `github.com/estuary/connectors`):

```
git clone https://github.com/estuary/connectors.git
cp -r source-clevertap connectors/
cd connectors
poetry -C source-clevertap install        # generates poetry.lock
```

Fill credentials in `test.flow.yaml`, then dry-run without writing anywhere:

```
flowctl preview --source source-clevertap/test.flow.yaml
```

## Build the image

All Python connectors share `estuary-cdk/common.Dockerfile`:

```
cd connectors
poetry -C source-clevertap lock          # produce poetry.lock (required by the build)
docker build -t ghcr.io/<you>/source-clevertap:v1 \
  --platform=linux/amd64 \
  --build-arg CONNECTOR_NAME=source-clevertap \
  --build-arg CONNECTOR_TYPE=capture \
  --build-arg USAGE_RATE=0 \
  -f estuary-cdk/common.Dockerfile .
docker push ghcr.io/<you>/source-clevertap:v1
```

## Run on Estuary cloud (no VM)

Reference the image in a capture spec and publish with `flowctl`:

```yaml
captures:
  wiom/clevertap/source-clevertap:
    endpoint:
      connector:
        image: ghcr.io/<you>/source-clevertap:v1
        config:
          account_id: ...
          secret: ...            # use sops-encrypted config or the dashboard UI
          event_names: [ ... ]
    bindings:
      - resource: { name: events_data, interval: P1D }
        target: wiom/clevertap/events_data
      - resource: { name: profile_data, interval: P1D }
        target: wiom/clevertap/profile_data
```

> **Managed-cloud note:** Estuary's docs state connectors "must be reviewed and
> added by the Estuary team" to run on the managed platform. Two routes to a
> truly VM-free deployment: (1) open a PR to `github.com/estuary/connectors` to
> get `source-clevertap` reviewed and hosted, or (2) push the image to a public
> registry and ask Estuary support to whitelist it for your tenant.

Then keep the existing Snowflake materialization from `../clevertap_estuary/flow.yaml`
(pointing at these same two collections) — nothing changes downstream.
