---
description: Capture data from Impact APIs using a Brand account in real time. Stream data on campaigns, ads, deals, invoices, and more.
---

# Impact

This connector captures data from Impact into Estuary collections.

## Supported data resources

The following data resources are supported through Impact's Brand API:

| Resource | Replication Mode |
| --- | --- |
| [Ads](https://integrations.impact.com/brand-api-reference/reference/ads) | Snapshot |
| [Campaign Actions](https://integrations.impact.com/brand-api-reference/reference/actions) | Incremental |
| [Campaign Action Inquiries](https://integrations.impact.com/brand-api-reference/reference/action-inquiries) | Incremental |
| [Campaign Block Redirect Rules](https://integrations.impact.com/brand-api-reference/reference/routing-rules) | Incremental |
| [Campaign Contracts](https://integrations.impact.com/brand-api-reference/reference/contracts) | Incremental |
| [Campaign Media Partner Groups](https://integrations.impact.com/brand-api-reference/reference/partner-groups) | Snapshot |
| [Campaign Notes](https://integrations.impact.com/brand-api-reference/reference/notes) | Incremental |
| [Campaign Tasks](https://integrations.impact.com/brand-api-reference/reference/tasks) | Incremental |
| [Campaigns](https://integrations.impact.com/brand-api-reference/reference/programs) | Snapshot |
| [Catalogs](https://integrations.impact.com/brand-api-reference/reference/catalogs) | Snapshot |
| [Deals](https://integrations.impact.com/brand-api-reference/reference/deals) | Incremental |
| [Exception Lists](https://integrations.impact.com/brand-api-reference/reference/exception-lists) | Incremental |
| [Invoices](https://integrations.impact.com/brand-api-reference/reference/invoices) | Incremental |
| [Jobs](https://integrations.impact.com/brand-api-reference/reference/jobs) | Incremental |
| [Phone Numbers](https://integrations.impact.com/brand-api-reference/reference/phone-numbers) | Snapshot |
| [Promo Codes](https://integrations.impact.com/brand-api-reference/reference/promo-codes) | Incremental |
| [Reports](https://integrations.impact.com/brand-api-reference/reference/report-export/reports-legacy) | Snapshot |
| [Tracking Value Requests](https://integrations.impact.com/brand-api-reference/reference/tracking-value-requests) | Incremental |
| [Unique URLs](https://integrations.impact.com/brand-api-reference/readme/introduction#unique-urls) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

To use this connector, you must have:
* A **Brand** Impact account
* User/password credentials for that account

## Configuration

You configure connectors either in the Estuary dashboard or by editing catalog specification files using the flowctl CLI.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Impact source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Set to `Username & Password`. | string | Required |
| **`/credentials/username`** | Username | Your Impact username. | string | Required |
| **`/credentials/password`** | Password | Your Impact password. | string | Required |
| **`/api_catalog`** | API Catalog | The Impact API catalog to use. Currently must be set to `Brand`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Data generated before this date will not be replicated. | string | 30 days before the present date |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Impact data stream to capture. | string | Required |
| `/interval` | Interval | Interval between data syncs for this resource. | string | `PT0S` |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-inmpact-native:v2
        config:
          credentials:
            credentials_title: Username & Password
            username: your_account
            password: <secret>
          api_catalog: Brand
          start_date: 2026-01-01T00:00:00Z
    bindings:
      - resource:
          name: ads
          interval: PT5M
        target: ${PREFIX}/ads
```
