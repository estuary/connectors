---
description: Use the Anthropic connector to sync Claude usage and cost analytics at minute, hour, and day granularity, using Admin API key or Analytics API key authentication.
---

# Anthropic

This connector captures Claude usage and cost data from Anthropic's [Usage and Cost Analytics API](https://platform.claude.com/docs/en/manage-claude/usage-cost-analytics-api) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Granularity | Replication Mode |
|----------|-------------|------------------|
| [usage_minutely](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/usage) | 1 minute | Incremental (no backfill of history) |
| [usage_hourly](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/usage) | 1 hour | Incremental |
| [usage_daily](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/usage) | 1 day | Incremental |
| [cost_minutely](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/cost) | 1 minute | Incremental (no backfill of history) |
| [cost_hourly](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/cost) | 1 hour | Incremental |
| [cost_daily](https://platform.claude.com/docs/en/api/http/beta/organization/analytics/cost) | 1 day | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

Rows are grouped by five dimensions chosen by the **Organization Type** setting. Claude Console organizations group by `actor`, `model`, `product`, `api_key_id`, and `workspace_id`. Claude Enterprise organizations group by `actor`, `model`, `product`, `speed`, and `context_window`, since Anthropic always returns `api_key_id` and `workspace_id` as null for them and `speed` and `context_window` explain price differences such as fast mode and long context. Cost resources additionally group by `cost_type` and `token_type`. Each document is keyed by its bucket's `starting_at` and a `group_key` string that joins the grouped dimensions, with empty values for dimensions that are null. The original `group` object is captured as-is. Cost `amount` and `list_amount` are decimal strings in US cents.

:::tip
Anthropic exports usage and cost data in batches, typically within 4 hours and occasionally up to 24 hours late, and may revise a month's figures until about seven days after it ends. Each sync re-fetches the configured lookback window (48 hours by default), and once a month, on the first sync after 00:00 UTC on the 9th, the previous month is re-fetched to capture its final values. Minutely resources re-fetch the same lookback window (at least 24 hours) but don't backfill history, so minute data older than the lookback isn't updated by the month-end re-fetch; hourly and daily resources backfill from the start date. Anthropic serves no data before 2026-01-01 or older than 365 days.
:::

:::note
Anthropic returns rows only for combinations that have usage or cost, and the connector doesn't delete documents. If a revision ever moves usage away from a combination, or reduces it to zero, the earlier document isn't removed and sums over that period can over-count.
:::

When querying the collections, keep in mind:

- Sums over minutely rows can differ slightly from hourly and daily totals once later corrections land, since the month-end re-fetch doesn't cover minutely resources. Use hourly or daily resources for final figures.
- Corrections older than the lookback window are applied on the 9th of the following month. If the capture is paused across more than one 9th, only the most recent previous month is re-fetched; backfill the binding to refresh earlier months.
- The current, incomplete bucket is emitted and updated on later syncs. Materialize with standard (non-delta) updates; a delta-updates materialization would count the partial and complete bucket both.
- To join usage and cost, match on `starting_at` and the fields in `group` (for example `actor`, `model`, and `product`), not on `group_key`. Cost rows are split further by `cost_type` and `token_type`, so their keys never match usage keys.

## Prerequisites

You'll need one of the following API keys:

- **Claude Console organizations:** an [Admin API key](https://platform.claude.com/settings/admin-keys) (`sk-ant-admin01-...`), created by an organization admin under **Keys & access > Admin keys**.
- **Claude Enterprise organizations:** an Analytics API key with the `read:analytics` scope, created by the organization's primary owner in claude.ai under **Organization settings > API**.

Anthropic limits the Usage and Cost Analytics API to 60 requests per minute per organization, shared by every key and integration in the organization.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Anthropic source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | An Admin API key (Claude Console organizations) or an Analytics API key with the `read:analytics` scope (Claude Enterprise organizations). | string | Required |
| **`/organization_type`** | Organization Type | `Claude Console` or `Claude Enterprise`. Sets the grouping dimensions. Changing it changes what each row means, so reset and backfill the collections afterwards. | string | Required |
| **`/credentials/credentials_title`** | Authentication | Name of the credentials set. Set to `API Key`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present. | string | |
| `/advanced/lookback_hours` | Lookback Window (Hours) | Hours of usage and cost data to re-fetch on each sync, to pick up late-arriving and revised data. Between 24 and 168. | integer | 48 |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | PT1H |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-anthropic:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
          organization_type: Claude Enterprise
          start_date: 2026-01-01T00:00:00Z
          advanced:
            lookback_hours: 48
    bindings:
      - resource:
          name: usage_minutely
        target: ${PREFIX}/usage_minutely
      - resource:
          name: usage_hourly
        target: ${PREFIX}/usage_hourly
      - resource:
          name: cost_daily
        target: ${PREFIX}/cost_daily
      {...}
```
