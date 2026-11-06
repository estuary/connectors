---
description: Use the Estuary connector to capture your own Estuary account's usage stats, catalog specs, task status, publication history, alerts, invoices, and configuration through the Estuary GraphQL API, using refresh token or service account API key authentication.
---

# Estuary

This connector captures data about your own Estuary account into Estuary collections, using Estuary's control-plane GraphQL API. Use it to track usage and costs, monitor task health, and audit changes to your catalog.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [catalog_stats_hourly](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/catalog_stats/mod.rs) | Incremental |
| [catalog_stats_daily](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/catalog_stats/mod.rs) | Incremental |
| [catalog_stats_monthly](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/catalog_stats/mod.rs) | Incremental |
| [live_specs](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/live_spec_refs.rs) | Full Refresh |
| [task_status](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/status.rs) | Full Refresh |
| [publication_history](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/publication_history.rs) | Incremental |
| [alerts](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/alerts.rs) | Full Refresh |
| [alert_configs](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/alert_configs.rs) | Full Refresh |
| [alert_subscriptions](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/alert_subscriptions.rs) | Full Refresh |
| [storage_mappings](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/storage_mappings.rs) | Full Refresh |
| [data_planes](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/data_planes.rs) | Full Refresh |
| [service_accounts](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/service_accounts.rs) | Full Refresh |
| [connectors](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/connectors.rs) | Full Refresh |
| [invoices](https://github.com/estuary/flow/blob/master/crates/control-plane-api/src/server/public/graphql/billing/invoices.rs) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
Stats buckets keep changing until they close, and can still change shortly after. Each `catalog_stats_*` sweep re-reads the buckets within the configured lookback (24 hours by default) before the previous sweep, and the re-read buckets replace earlier versions in the collection.

Each `catalog_stats_*` collection holds two kinds of rows: one rollup row per captured prefix (a `catalogName` ending in `/`), and one row per task or collection. Rollup rows already include the per-task rows beneath them, so filter on one kind before summing.
:::

Some data can only be captured while it exists:

- `catalog_stats_*` captures per-task rows only for specs that currently exist. A deleted task's earlier history isn't backfilled, but its usage is still included in its prefix's rollup row.
- `publication_history` can't read the history of deleted specs.
- Adding a prefix to an existing capture doesn't backfill that prefix's history. Re-backfill the affected bindings to capture it.

`invoices` captures invoice amounts and line items, but not payment status.

## Prerequisites

To set up the Estuary source connector, you'll need an Estuary refresh token or service account API key. The connector captures prefixes the credential can read. Some resources need more access:

- `alert_subscriptions` requires admin access to a prefix.
- `invoices` requires billing access to a tenant, which admins have. Invoices are captured per tenant, so the tenant prefix itself (for example, `acmeCo/`) must be configured, or `prefixes` left empty.
- `service_accounts` requires access to query service accounts, which admins have.

Prefixes the credential lacks access to are skipped for these resources. If the credential loses read access to a configured prefix entirely, the capture fails rather than skipping it, so no data is missed while access is lost.

Short-lived access tokens, which expire after an hour, are not accepted.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Estuary source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | An Estuary service account API key or refresh token. | string | Required |
| **`/credentials/credentials_title`** | Authentication | Name of the credentials set. Set to `API Key`. | string | Required |
| `/prefixes` | Prefixes | Catalog prefixes to capture, each ending in `/` (for example, `acmeCo/`). If left empty, every prefix the API key can read is captured. | array | `[]` |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string | |
| `/advanced/catalog_stats_lookback_hours` | Catalog Stats Lookback (Hours) | Hours before the cursor that every catalog stats sweep re-reads, so buckets that keep accruing after they close are captured in full. Raise this only to recover from a stats pipeline delay. | integer | `24` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-estuary:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
          prefixes:
            - acmeCo/
          start_date: 2026-01-01T00:00:00Z
    bindings:
      - resource:
          name: catalog_stats_daily
          interval: PT1H
        target: ${PREFIX}/catalog_stats_daily
      - resource:
          name: task_status
          interval: PT5M
        target: ${PREFIX}/task_status
      {...}
```
